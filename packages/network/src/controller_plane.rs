use std::sync::Arc;

use atm0s_sdn_identity::NodeId;

use crate::{
    base::{ConnectionEvent, FeatureControlActor, FeatureInput, FeatureOutput, FeatureSharedInput, ServiceBuilder, ServiceControlActor, ServiceInput, ServiceOutput, ServiceSharedInput},
    features::{FeaturesControl, FeaturesEvent},
    san_io_utils::TasksSwitcher,
    ExtIn, ExtOut, LogicControl, LogicEvent,
};

use self::{features::FeatureManager, neighbours::NeighboursManager, services::ServiceManager};

mod features;
mod neighbours;
mod services;

#[derive(Debug, Clone, convert_enum::From)]
pub enum Input<SC, TC> {
    Ext(ExtIn<SC>),
    Control(LogicControl<TC>),
    #[convert_enum(optout)]
    ShutdownRequest,
}

#[derive(Debug, Clone, convert_enum::From)]
pub enum Output<SE, TW> {
    Ext(ExtOut<SE>),
    Event(LogicEvent<TW>),
    #[convert_enum(optout)]
    ShutdownSuccess,
}

const NEIGHBOURS_ID: u8 = 0;
const FEATURES_ID: u8 = 1;
const SERVICES_ID: u8 = 2;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskType {
    Neighbours = NEIGHBOURS_ID,
    Feature = FEATURES_ID,
    Service = SERVICES_ID,
}

impl TryFrom<usize> for TaskType {
    type Error = ();
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value as u8 {
            NEIGHBOURS_ID => Ok(Self::Neighbours),
            FEATURES_ID => Ok(Self::Feature),
            SERVICES_ID => Ok(Self::Service),
            _ => Err(()),
        }
    }
}

pub struct ControllerPlane<SC, SE, TC, TW> {
    neighbours: NeighboursManager,
    features: FeatureManager,
    services: ServiceManager<SC, SE, TC, TW>,
    // TODO may be we need stack style for optimize performance
    // and support some case task output call other task
    last_task: Option<TaskType>,
    switcher: TasksSwitcher<3>,
}

impl<SC, SE, TC, TW> ControllerPlane<SC, SE, TC, TW> {
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
    pub fn new(node_id: NodeId, session: u64, services: Vec<Arc<dyn ServiceBuilder<FeaturesControl, FeaturesEvent, SC, SE, TC, TW>>>) -> Self {
        log::info!("Create ControllerPlane for node: {}, running session {}", node_id, session);
        let service_ids = services.iter().filter(|s| s.discoverable()).map(|s| s.service_id()).collect();

        Self {
            neighbours: NeighboursManager::new(node_id),
            features: FeatureManager::new(node_id, session, service_ids),
            services: ServiceManager::new(services),
            last_task: None,
            switcher: TasksSwitcher::default(),
        }
    }

    pub fn on_tick(&mut self, now_ms: u64) {
        self.last_task = None;
        self.neighbours.on_tick(now_ms);
        self.features.on_shared_input(now_ms, FeatureSharedInput::Tick(now_ms));
        self.services.on_shared_input(now_ms, ServiceSharedInput::Tick(now_ms));
    }

    pub fn on_event(&mut self, now_ms: u64, event: Input<SC, TC>) {
        match event {
            Input::Ext(ExtIn::ConnectTo(addr)) => {
                self.last_task = Some(TaskType::Neighbours);
                self.neighbours.on_input(now_ms, neighbours::Input::ConnectTo(addr));
            }
            Input::Ext(ExtIn::DisconnectFrom(node)) => {
                self.last_task = Some(TaskType::Neighbours);
                self.neighbours.on_input(now_ms, neighbours::Input::DisconnectFrom(node));
            }
            Input::Ext(ExtIn::FeaturesControl(control)) => {
                self.last_task = Some(TaskType::Feature);
                self.features.on_input(now_ms, control.to_feature(), FeatureInput::Control(FeatureControlActor::Controller, control));
            }
            Input::Ext(ExtIn::ServicesControl(service, control)) => {
                self.last_task = Some(TaskType::Service);
                self.services.on_input(now_ms, service, ServiceInput::Control(ServiceControlActor::Controller, control));
            }
            Input::Control(LogicControl::NetNeighbour(remote, control)) => {
                self.last_task = Some(TaskType::Neighbours);
                self.neighbours.on_input(now_ms, neighbours::Input::Control(remote, control));
            }
            Input::Control(LogicControl::Feature(to)) => {
                self.last_task = Some(TaskType::Feature);
                self.features.on_input(now_ms, to.to_feature(), FeatureInput::FromWorker(to));
            }
            Input::Control(LogicControl::Service(service, to)) => {
                self.last_task = Some(TaskType::Service);
                self.services.on_input(now_ms, service, ServiceInput::FromWorker(to));
            }
            Input::Control(LogicControl::NetRemote(feature, conn, msg)) => {
                if let Some(ctx) = self.neighbours.conn(conn) {
                    self.last_task = Some(TaskType::Feature);
                    self.features.on_input(now_ms, feature, FeatureInput::Net(ctx, msg));
                }
            }
            Input::Control(LogicControl::NetLocal(feature, msg)) => {
                self.last_task = Some(TaskType::Feature);
                self.features.on_input(now_ms, feature, FeatureInput::Local(msg));
            }
            Input::Control(LogicControl::ServiceEvent(service, event)) => {
                self.last_task = Some(TaskType::Service);
                self.services.on_input(now_ms, service, ServiceInput::FeatureEvent(event));
            }
            Input::Control(LogicControl::FeaturesControl(actor, control)) => {
                self.last_task = Some(TaskType::Feature);
                self.features.on_input(now_ms, control.to_feature(), FeatureInput::Control(actor, control));
            }
            Input::ShutdownRequest => {
                self.last_task = Some(TaskType::Neighbours);
                self.neighbours.on_input(now_ms, neighbours::Input::ShutdownRequest);
            }
        }
    }

    pub fn pop_output(&mut self, now_ms: u64) -> Option<Output<SE, TW>> {
        if let Some(last_task) = &self.last_task {
            let res = match last_task {
                TaskType::Neighbours => self.pop_neighbours(now_ms),
                TaskType::Feature => self.pop_features(now_ms),
                TaskType::Service => self.pop_services(now_ms),
            };
            if res.is_none() {
                self.last_task = None;
            }
            res
        } else {
            while let Some(current) = self.switcher.current() {
                match current.try_into().expect("Should convert to TaskType") {
                    TaskType::Neighbours => {
                        let out = self.pop_neighbours(now_ms);
                        if let Some(out) = self.switcher.process(out) {
                            return Some(out);
                        }
                    }
                    TaskType::Feature => {
                        let out = self.pop_features(now_ms);
                        if let Some(out) = self.switcher.process(out) {
                            return Some(out);
                        }
                    }
                    TaskType::Service => {
                        let out = self.pop_services(now_ms);
                        if let Some(out) = self.switcher.process(out) {
                            return Some(out);
                        }
                    }
                }
            }

            None
        }
    }

    fn pop_neighbours(&mut self, now_ms: u64) -> Option<Output<SE, TW>> {
        let out = self.neighbours.pop_output()?;
        match out {
            neighbours::Output::Control(remote, control) => Some(Output::Event(LogicEvent::NetNeigbour(remote, control))),
            neighbours::Output::Event(event) => {
                self.features.on_shared_input(now_ms, FeatureSharedInput::Connection(event.clone()));
                self.services.on_shared_input(now_ms, ServiceSharedInput::Connection(event.clone()));
                match event {
                    ConnectionEvent::Connected(ctx, secure) => Some(Output::Event(LogicEvent::Pin(ctx.conn, ctx.node, ctx.remote, secure))),
                    ConnectionEvent::Stats(_ctx, _stats) => None,
                    ConnectionEvent::Disconnected(ctx) => Some(Output::Event(LogicEvent::UnPin(ctx.conn))),
                }
            }
            neighbours::Output::ShutdownResponse => Some(Output::ShutdownSuccess),
        }
    }

    fn pop_features(&mut self, now_ms: u64) -> Option<Output<SE, TW>> {
        let (feature, out) = self.features.pop_output()?;
        match out {
            FeatureOutput::ToWorkers(to) => Some(Output::Event(LogicEvent::Feature(to))),
            FeatureOutput::Event(actor, event) => {
                //TODO may be we need stack style for optimize performance
                match actor {
                    FeatureControlActor::Controller => Some(Output::Ext(ExtOut::FeaturesEvent(event))),
                    FeatureControlActor::Service(service) => {
                        self.services.on_input(now_ms, service, ServiceInput::FeatureEvent(event));
                        None
                    }
                }
            }
            FeatureOutput::SendDirect(conn, buf) => {
                log::debug!("[ControllerPlane] SendDirect to conn: {:?}, len: {}", conn, buf.len());
                Some(Output::Event(LogicEvent::NetDirect(feature, conn, buf)))
            }
            FeatureOutput::SendRoute(rule, ttl, buf) => {
                log::debug!("[ControllerPlane] SendRoute to rule: {:?}, len: {}", rule, buf.len());
                Some(Output::Event(LogicEvent::NetRoute(feature, rule, ttl, buf)))
            }
            FeatureOutput::NeighboursConnectTo(addr) => {
                //TODO may be we need stack style for optimize performance
                self.neighbours.on_input(now_ms, neighbours::Input::ConnectTo(addr));
                None
            }
            FeatureOutput::NeighboursDisconnectFrom(node) => {
                //TODO may be we need stack style for optimize performance
                self.neighbours.on_input(now_ms, neighbours::Input::DisconnectFrom(node));
                None
            }
        }
    }

    fn pop_services(&mut self, now_ms: u64) -> Option<Output<SE, TW>> {
        let (service, out) = self.services.pop_output()?;
        match out {
            ServiceOutput::FeatureControl(control) => {
                self.features
                    .on_input(now_ms, control.to_feature(), FeatureInput::Control(FeatureControlActor::Service(service), control));
                None
            }
            ServiceOutput::Event(actor, event) => match actor {
                ServiceControlActor::Controller => Some(Output::Ext(ExtOut::ServicesEvent(event))),
            },
            ServiceOutput::BroadcastWorkers(to) => Some(Output::Event(LogicEvent::Service(service, to))),
        }
    }
}
