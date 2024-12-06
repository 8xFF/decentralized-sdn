use std::{collections::VecDeque, fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc};

use atm0s_sdn_identity::NodeId;
use atm0s_sdn_router::shadow::ShadowRouterHistory;
use rand::RngCore;
use sans_io_runtime::{return_if_none, return_if_some, TaskSwitcher, TaskSwitcherBranch, TaskSwitcherChild};

use crate::{
    base::{
        Authorization, ConnectionEvent, FeatureContext, FeatureControlActor, FeatureInput, FeatureOutput, FeatureSharedInput, HandshakeBuilder, ServiceBuilder, ServiceControlActor, ServiceCtx,
        ServiceInput, ServiceOutput, ServiceSharedInput,
    },
    features::{FeaturesControl, FeaturesEvent},
    ExtIn, ExtOut, LogicControl, LogicEvent,
};

use self::{features::FeatureManager, neighbours::NeighboursManager, services::ServiceManager};

mod features;
mod neighbours;
mod services;

#[derive(Debug, Clone, convert_enum::From)]
pub enum Input<UserData, SC, SE, TC> {
    Ext(ExtIn<UserData, SC>),
    Control(LogicControl<UserData, SC, SE, TC>),
}

#[derive(Debug, Clone, convert_enum::From)]
pub enum Output<UserData, SE, TW> {
    Ext(ExtOut<UserData, SE>),
    Event(LogicEvent<UserData, SE, TW>),
    #[convert_enum(optout)]
    OnResourceEmpty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive)]
#[repr(usize)]
enum TaskType {
    Neighbours = 0,
    Feature = 1,
    Service = 2,
}

pub struct ControllerPlaneCfg<UserData, SC, SE, TC, TW> {
    pub session: u64,
    pub bind_addrs: Vec<SocketAddr>,
    #[allow(clippy::type_complexity)]
    pub services: Vec<Arc<dyn ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>>>,
    pub authorization: Arc<dyn Authorization>,
    pub handshake_builder: Arc<dyn HandshakeBuilder>,
    pub random: Box<dyn RngCore + Send + Sync>,
    pub history: Arc<dyn ShadowRouterHistory>,
}

pub struct ControllerPlane<UserData, SC, SE, TC, TW> {
    tick_count: u64,
    feature_ctx: FeatureContext,
    service_ctx: ServiceCtx,
    neighbours: TaskSwitcherBranch<NeighboursManager, neighbours::Output>,
    features: TaskSwitcherBranch<FeatureManager<UserData>, features::Output<UserData>>,
    #[allow(clippy::type_complexity)]
    services: TaskSwitcherBranch<ServiceManager<UserData, SC, SE, TC, TW>, services::Output<UserData, SE, TW>>,
    switcher: TaskSwitcher,
    queue: VecDeque<Output<UserData, SE, TW>>,
    shutdown: bool,
    history: Arc<dyn ShadowRouterHistory>,
}

impl<UserData, SC, SE, TC, TW> ControllerPlane<UserData, SC, SE, TC, TW>
where
    UserData: 'static + Hash + Copy + Eq + Debug,
{
    /// Create a new ControllerPlane
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node id of the current node
    /// * `session` - The session id of the current node, it can be a random number
    ///
    /// # Returns
    ///
    /// A new ControllerPlane
    pub fn new(node_id: NodeId, cfg: ControllerPlaneCfg<UserData, SC, SE, TC, TW>) -> Self {
        log::info!("Create ControllerPlane for node: {}, running session {}", node_id, cfg.session);
        let service_ids = cfg.services.iter().filter(|s| s.discoverable()).map(|s| s.service_id()).collect();

        Self {
            tick_count: 0,
            feature_ctx: FeatureContext { node_id, session: cfg.session },
            service_ctx: ServiceCtx { node_id, session: cfg.session },
            neighbours: TaskSwitcherBranch::new(
                NeighboursManager::new(node_id, cfg.bind_addrs, cfg.authorization, cfg.handshake_builder, cfg.random),
                TaskType::Neighbours,
            ),
            features: TaskSwitcherBranch::new(FeatureManager::new(node_id, cfg.session, service_ids), TaskType::Feature),
            services: TaskSwitcherBranch::new(ServiceManager::new(cfg.services), TaskType::Service),
            switcher: TaskSwitcher::new(3), //3 types: Neighbours, Feature, Service
            queue: VecDeque::new(),
            shutdown: false,
            history: cfg.history,
        }
    }

    pub fn on_tick(&mut self, now_ms: u64) {
        log::trace!("[ControllerPlane] on_tick: {}", now_ms);
        self.neighbours.input(&mut self.switcher).on_tick(now_ms, self.tick_count);
        self.features
            .input(&mut self.switcher)
            .on_shared_input(&self.feature_ctx, now_ms, FeatureSharedInput::Tick(self.tick_count));
        self.services
            .input(&mut self.switcher)
            .on_shared_input(&self.service_ctx, now_ms, ServiceSharedInput::Tick(self.tick_count));
        self.tick_count += 1;
        self.history.set_ts(now_ms);
    }

    pub fn on_event(&mut self, now_ms: u64, event: Input<UserData, SC, SE, TC>) {
        match event {
            Input::Ext(ExtIn::FeaturesControl(userdata, control)) => {
                self.features.input(&mut self.switcher).on_input(
                    &self.feature_ctx,
                    now_ms,
                    control.to_feature(),
                    FeatureInput::Control(FeatureControlActor::Controller(userdata), control),
                );
            }
            Input::Ext(ExtIn::ServicesControl(service, userdata, control)) => {
                self.services
                    .input(&mut self.switcher)
                    .on_input(&self.service_ctx, now_ms, service, ServiceInput::Control(ServiceControlActor::Controller(userdata), control));
            }
            Input::Control(LogicControl::NetNeighbour(pair, control)) => {
                self.neighbours.input(&mut self.switcher).on_input(now_ms, neighbours::Input::Control(pair, control));
            }
            Input::Control(LogicControl::Feature(to)) => {
                self.features
                    .input(&mut self.switcher)
                    .on_input(&self.feature_ctx, now_ms, to.to_feature(), FeatureInput::FromWorker(to));
            }
            Input::Control(LogicControl::Service(service, to)) => {
                self.services.input(&mut self.switcher).on_input(&self.service_ctx, now_ms, service, ServiceInput::FromWorker(to));
            }
            Input::Control(LogicControl::NetRemote(feature, conn, meta, msg)) => {
                if let Some(ctx) = self.neighbours.conn(conn) {
                    self.features.input(&mut self.switcher).on_input(&self.feature_ctx, now_ms, feature, FeatureInput::Net(ctx, meta, msg));
                }
            }
            Input::Control(LogicControl::NetLocal(feature, meta, msg)) => {
                self.features.input(&mut self.switcher).on_input(&self.feature_ctx, now_ms, feature, FeatureInput::Local(meta, msg));
            }
            Input::Control(LogicControl::ServiceEvent(service, event)) => {
                self.services.input(&mut self.switcher).on_input(&self.service_ctx, now_ms, service, ServiceInput::FeatureEvent(event));
            }
            Input::Control(LogicControl::ServicesControl(actor, service, control)) => {
                self.services
                    .input(&mut self.switcher)
                    .on_input(&self.service_ctx, now_ms, service, ServiceInput::Control(actor, control));
            }
            Input::Control(LogicControl::FeaturesControl(actor, control)) => {
                self.features
                    .input(&mut self.switcher)
                    .on_input(&self.feature_ctx, now_ms, control.to_feature(), FeatureInput::Control(actor, control));
            }
            Input::Control(LogicControl::ExtFeaturesEvent(userdata, event)) => {
                self.queue.push_back(Output::Ext(ExtOut::FeaturesEvent(userdata, event)));
            }
            Input::Control(LogicControl::ExtServicesEvent(service, userdata, event)) => {
                self.queue.push_back(Output::Ext(ExtOut::ServicesEvent(service, userdata, event)));
            }
        }
    }

    pub fn on_shutdown(&mut self, now_ms: u64) {
        if self.shutdown {
            return;
        }
        log::info!("[ControllerPlane] Shutdown");
        self.features.input(&mut self.switcher).on_shutdown(&self.feature_ctx, now_ms);
        self.services.input(&mut self.switcher).on_shutdown(&self.service_ctx, now_ms);
        self.neighbours.input(&mut self.switcher).on_shutdown(now_ms);
        self.shutdown = true;
    }

    fn pop_neighbours(&mut self, now_ms: u64) {
        let out = return_if_none!(self.neighbours.pop_output(now_ms, &mut self.switcher));
        match out {
            neighbours::Output::Control(remote, control) => self.queue.push_back(Output::Event(LogicEvent::NetNeighbour(remote, control))),
            neighbours::Output::Event(event) => {
                self.features
                    .input(&mut self.switcher)
                    .on_shared_input(&self.feature_ctx, now_ms, FeatureSharedInput::Connection(event.clone()));
                self.services
                    .input(&mut self.switcher)
                    .on_shared_input(&self.service_ctx, now_ms, ServiceSharedInput::Connection(event.clone()));
                match event {
                    ConnectionEvent::Connecting(_ctx) => {}
                    ConnectionEvent::ConnectError(_ctx, _err) => {}
                    ConnectionEvent::Connected(ctx, secure) => self.queue.push_back(Output::Event(LogicEvent::Pin(ctx.conn, ctx.node, ctx.pair, secure))),
                    ConnectionEvent::Stats(_ctx, _stats) => {}
                    ConnectionEvent::Disconnected(ctx) => self.queue.push_back(Output::Event(LogicEvent::UnPin(ctx.conn))),
                }
            }
            neighbours::Output::OnResourceEmpty => {
                log::info!("[ControllerPlane] Neighbours OnResourceEmpty");
            }
        }
    }

    fn pop_features(&mut self, now_ms: u64) {
        let out = return_if_none!(self.features.pop_output(now_ms, &mut self.switcher));

        let (feature, out) = match out {
            features::Output::Output(feature, out) => (feature, out),
            features::Output::Shutdown => {
                log::info!("[ControllerPlane] Features Shutdown");
                return;
            }
        };

        match out {
            FeatureOutput::ToWorker(is_broadcast, to) => self.queue.push_back(Output::Event(LogicEvent::Feature(is_broadcast, to))),
            FeatureOutput::Event(actor, event) => {
                log::debug!("[Controller] send FeatureEvent to actor {:?}, event {:?}", actor, event);
                match actor {
                    FeatureControlActor::Controller(userdata) => self.queue.push_back(Output::Ext(ExtOut::FeaturesEvent(userdata, event))),
                    FeatureControlActor::Worker(worker, userdata) => self.queue.push_back(Output::Event(LogicEvent::ExtFeaturesEvent(worker, userdata, event))),
                    FeatureControlActor::Service(service) => {
                        self.services.input(&mut self.switcher).on_input(&self.service_ctx, now_ms, service, ServiceInput::FeatureEvent(event));
                    }
                }
            }
            FeatureOutput::SendDirect(conn, meta, buf) => {
                log::debug!("[ControllerPlane] SendDirect to conn: {:?}, len: {}", conn, buf.len());
                let conn_ctx = return_if_none!(self.neighbours.conn(conn));
                self.queue.push_back(Output::Event(LogicEvent::NetDirect(feature, conn_ctx.pair, conn, meta, buf)))
            }
            FeatureOutput::SendRoute(rule, ttl, buf) => {
                log::debug!("[ControllerPlane] SendRoute to rule: {:?}, len: {}", rule, buf.len());
                self.queue.push_back(Output::Event(LogicEvent::NetRoute(feature, rule, ttl, buf)))
            }
            FeatureOutput::NeighboursConnectTo(addr) => {
                self.neighbours.input(&mut self.switcher).on_input(now_ms, neighbours::Input::ConnectTo(addr));
            }
            FeatureOutput::NeighboursDisconnectFrom(node) => {
                self.neighbours.input(&mut self.switcher).on_input(now_ms, neighbours::Input::DisconnectFrom(node));
            }
            FeatureOutput::OnResourceEmpty => {
                log::info!("[ControllerPlane] Feature {feature:?} OnResourceEmpty");
            }
        }
    }

    fn pop_services(&mut self, now_ms: u64) {
        let out = return_if_none!(self.services.pop_output(now_ms, &mut self.switcher));

        let (service, out) = match out {
            services::Output::Output(service, out) => (service, out),
            services::Output::OnResourceEmpty => {
                log::info!("[ControllerPlane] Services OnResourceEmpty");
                return;
            }
        };

        match out {
            ServiceOutput::FeatureControl(control) => {
                self.features
                    .input(&mut self.switcher)
                    .on_input(&self.feature_ctx, now_ms, control.to_feature(), FeatureInput::Control(FeatureControlActor::Service(service), control));
            }
            ServiceOutput::Event(actor, event) => match actor {
                ServiceControlActor::Controller(userdata) => self.queue.push_back(Output::Ext(ExtOut::ServicesEvent(service, userdata, event))),
                ServiceControlActor::Worker(worker, userdata) => self.queue.push_back(Output::Event(LogicEvent::ExtServicesEvent(worker, service, userdata, event))),
            },
            ServiceOutput::BroadcastWorkers(to) => self.queue.push_back(Output::Event(LogicEvent::Service(service, to))),
            ServiceOutput::OnResourceEmpty => {
                log::info!("[ControllerPlane] Service {service} OnResourceEmpty");
            }
        }
    }
}

impl<UserData, SC, SE, TC, TW> TaskSwitcherChild<Output<UserData, SE, TW>> for ControllerPlane<UserData, SC, SE, TC, TW>
where
    UserData: 'static + Hash + Copy + Eq + Debug,
{
    type Time = u64;

    fn empty_event(&self) -> Output<UserData, SE, TW> {
        Output::OnResourceEmpty
    }

    fn is_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty() && self.neighbours.is_empty() && self.features.is_empty() && self.services.is_empty()
    }

    fn pop_output(&mut self, now_ms: u64) -> Option<Output<UserData, SE, TW>> {
        return_if_some!(self.queue.pop_front());

        while let Some(current) = self.switcher.current() {
            match current.try_into().expect("Should convert to TaskType") {
                TaskType::Neighbours => self.pop_neighbours(now_ms),
                TaskType::Feature => self.pop_features(now_ms),
                TaskType::Service => self.pop_services(now_ms),
            }

            return_if_some!(self.queue.pop_front());
        }

        None
    }
}
