use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    net::{AddrParseError, SocketAddr},
    sync::Arc,
};

use atm0s_sdn_identity::{ConnId, NodeId};
use atm0s_sdn_router::{
    shadow::{ShadowRouter, ShadowRouterHistory},
    RouteAction, RouteRule, RouterTable,
};
use sans_io_runtime::{collections::DynamicDeque, return_if_err, return_if_none, return_if_some, TaskSwitcher, TaskSwitcherBranch, TaskSwitcherChild};

use crate::{
    base::{
        Buffer, FeatureControlActor, FeatureWorkerContext, FeatureWorkerInput, FeatureWorkerOutput, NeighboursControl, NetOutgoingMeta, ServiceBuilder, ServiceControlActor, ServiceId,
        ServiceWorkerCtx, ServiceWorkerInput, ServiceWorkerOutput, TransportMsg, TransportMsgHeader,
    },
    features::{Features, FeaturesControl, FeaturesEvent},
    ExtIn, ExtOut, LogicControl, LogicEvent,
};

use self::{connection::DataPlaneConnection, features::FeatureWorkerManager, services::ServiceWorkerManager};

mod connection;
mod features;
mod services;

/// NetPair is a pair between remote addr and local addr.
/// This is for solving problems with multi-ip-addresses system.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct NetPair {
    pub local: SocketAddr,
    pub remote: SocketAddr,
}

impl NetPair {
    pub fn new(local: SocketAddr, remote: SocketAddr) -> Self {
        Self { local, remote }
    }

    pub fn new_str(local: &str, remote: &str) -> Result<Self, AddrParseError> {
        Ok(Self {
            local: local.parse::<SocketAddr>()?,
            remote: remote.parse::<SocketAddr>()?,
        })
    }
}

impl Display for NetPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}-{}]", self.local, self.remote))
    }
}

#[derive(Debug)]
pub enum NetInput {
    UdpPacket(NetPair, Buffer),
    #[cfg(feature = "vpn")]
    TunPacket(Buffer),
}

#[derive(Debug, Clone)]
pub enum CrossWorker<UserData, SE> {
    Feature(UserData, FeaturesEvent),
    Service(ServiceId, UserData, SE),
}

#[derive(Debug)]
pub enum Input<UserData, SC, SE, TW> {
    Ext(ExtIn<UserData, SC>),
    Net(NetInput),
    Event(LogicEvent<UserData, SE, TW>),
    Worker(CrossWorker<UserData, SE>),
}

#[derive(Debug)]
pub enum NetOutput {
    UdpPacket(NetPair, Buffer),
    UdpPackets(Vec<NetPair>, Buffer),
    #[cfg(feature = "vpn")]
    TunPacket(Buffer),
}

#[derive(convert_enum::From)]
pub enum Output<UserData, SC, SE, TC> {
    Ext(ExtOut<UserData, SE>),
    Net(NetOutput),
    Control(LogicControl<UserData, SC, SE, TC>),
    #[convert_enum(optout)]
    Worker(u16, CrossWorker<UserData, SE>),
    #[convert_enum(optout)]
    OnResourceEmpty,
    #[convert_enum(optout)]
    Continue,
}

#[derive(num_enum::TryFromPrimitive, num_enum::IntoPrimitive)]
#[repr(usize)]
enum TaskType {
    Feature = 0,
    Service = 1,
}

pub struct DataPlaneCfg<UserData, SC, SE, TC, TW> {
    pub worker_id: u16,
    #[allow(clippy::type_complexity)]
    pub services: Vec<Arc<dyn ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>>>,
    pub history: Arc<dyn ShadowRouterHistory>,
}

pub struct DataPlane<UserData, SC, SE, TC, TW> {
    tick_count: u64,
    worker_id: u16,
    feature_ctx: FeatureWorkerContext,
    service_ctx: ServiceWorkerCtx,
    features: TaskSwitcherBranch<FeatureWorkerManager<UserData>, features::Output<UserData>>,
    #[allow(clippy::type_complexity)]
    services: TaskSwitcherBranch<ServiceWorkerManager<UserData, SC, SE, TC, TW>, services::Output<UserData, SC, SE, TC>>,
    conns: HashMap<NetPair, DataPlaneConnection>,
    conns_reverse: HashMap<ConnId, NetPair>,
    queue: DynamicDeque<Output<UserData, SC, SE, TC>, 16>,
    shutdown: bool,
    switcher: TaskSwitcher,
}

impl<UserData, SC, SE, TC, TW> DataPlane<UserData, SC, SE, TC, TW>
where
    UserData: 'static + Copy + Eq + Hash + Debug,
{
    pub fn new(node_id: NodeId, cfg: DataPlaneCfg<UserData, SC, SE, TC, TW>) -> Self {
        log::info!("Create DataPlane for node: {}", node_id);

        Self {
            worker_id: cfg.worker_id,
            tick_count: 0,
            feature_ctx: FeatureWorkerContext {
                node_id,
                router: ShadowRouter::new(node_id, cfg.history),
            },
            service_ctx: ServiceWorkerCtx { node_id },
            features: TaskSwitcherBranch::new(FeatureWorkerManager::new(), TaskType::Feature),
            services: TaskSwitcherBranch::new(ServiceWorkerManager::new(cfg.services), TaskType::Service),
            conns: HashMap::new(),
            conns_reverse: HashMap::new(),
            queue: DynamicDeque::default(),
            shutdown: false,
            switcher: TaskSwitcher::new(2),
        }
    }

    pub fn route(&self, rule: RouteRule, source: Option<NodeId>, relay_from: Option<NodeId>) -> RouteAction<NetPair> {
        self.feature_ctx.router.derive_action(&rule, source, relay_from)
    }

    pub fn on_tick(&mut self, now_ms: u64) {
        log::trace!("[DataPlane] on_tick: {}", now_ms);
        self.features.input(&mut self.switcher).on_tick(&mut self.feature_ctx, now_ms, self.tick_count);
        self.services.input(&mut self.switcher).on_tick(&self.service_ctx, now_ms, self.tick_count);
        self.tick_count += 1;
    }

    pub fn on_event(&mut self, now_ms: u64, event: Input<UserData, SC, SE, TW>) {
        match event {
            Input::Ext(ext) => match ext {
                ExtIn::FeaturesControl(userdata, control) => {
                    let feature: Features = control.to_feature();
                    let actor = FeatureControlActor::Worker(self.worker_id, userdata);
                    self.features
                        .input(&mut self.switcher)
                        .on_input(&mut self.feature_ctx, feature, now_ms, FeatureWorkerInput::Control(actor, control));
                }
                ExtIn::ServicesControl(service, userdata, control) => {
                    let actor = ServiceControlActor::Worker(self.worker_id, userdata);
                    self.services
                        .input(&mut self.switcher)
                        .on_input(&self.service_ctx, now_ms, service, ServiceWorkerInput::Control(actor, control));
                }
            },
            Input::Worker(CrossWorker::Feature(userdata, event)) => self.queue.push_back(Output::Ext(ExtOut::FeaturesEvent(userdata, event))),
            Input::Worker(CrossWorker::Service(service, userdata, event)) => self.queue.push_back(Output::Ext(ExtOut::ServicesEvent(service, userdata, event))),
            Input::Net(NetInput::UdpPacket(pair, buf)) => {
                if buf.is_empty() {
                    return;
                }
                if let Ok(control) = NeighboursControl::try_from(&*buf) {
                    self.queue.push_back(LogicControl::NetNeighbour(pair, control).into());
                } else {
                    self.incoming_route(now_ms, pair, buf);
                }
            }
            #[cfg(feature = "vpn")]
            Input::Net(NetInput::TunPacket(pkt)) => {
                self.features
                    .input(&mut self.switcher)
                    .on_input(&mut self.feature_ctx, Features::Vpn, now_ms, FeatureWorkerInput::TunPkt(pkt));
            }
            Input::Event(LogicEvent::Feature(is_broadcast, to)) => {
                let feature = to.to_feature();
                self.features
                    .input(&mut self.switcher)
                    .on_input(&mut self.feature_ctx, feature, now_ms, FeatureWorkerInput::FromController(is_broadcast, to));
            }
            Input::Event(LogicEvent::Service(service, to)) => {
                self.services
                    .input(&mut self.switcher)
                    .on_input(&self.service_ctx, now_ms, service, ServiceWorkerInput::FromController(to));
            }
            Input::Event(LogicEvent::ExtFeaturesEvent(worker, userdata, event)) => {
                assert_eq!(self.worker_id, worker);
                self.queue.push_back(Output::Ext(ExtOut::FeaturesEvent(userdata, event)));
            }
            Input::Event(LogicEvent::ExtServicesEvent(worker, service, userdata, event)) => {
                assert_eq!(self.worker_id, worker);
                self.queue.push_back(Output::Ext(ExtOut::ServicesEvent(service, userdata, event)));
            }
            Input::Event(LogicEvent::NetNeighbour(pair, control)) => {
                let buf: Result<Vec<u8>, ()> = (&control).try_into();
                if let Ok(buf) = buf {
                    self.queue.push_back(NetOutput::UdpPacket(pair, buf.into()).into());
                }
            }
            Input::Event(LogicEvent::NetDirect(feature, pair, _conn, meta, buf)) => {
                let header = meta.to_header(feature as u8, RouteRule::Direct, self.feature_ctx.node_id);
                let conn = return_if_none!(self.conns.get_mut(&pair));
                let msg = TransportMsg::build_raw(header, buf);
                if let Some(pkt) = Self::build_send_to_from_mut(now_ms, conn, pair, msg.take()) {
                    self.queue.push_back(pkt.into());
                }
            }
            Input::Event(LogicEvent::NetRoute(feature, rule, meta, buf)) => self.outgoing_route(now_ms, feature, rule, meta, buf),
            Input::Event(LogicEvent::Pin(conn, node, pair, secure)) => {
                self.conns.insert(pair, DataPlaneConnection::new(node, conn, pair, secure));
                self.conns_reverse.insert(conn, pair);
            }
            Input::Event(LogicEvent::UnPin(conn)) => {
                if let Some(addr) = self.conns_reverse.remove(&conn) {
                    log::info!("UnPin: conn: {} <--> addr: {}", conn, addr);
                    self.conns.remove(&addr);
                }
            }
        }
    }

    pub fn on_shutdown(&mut self, now_ms: u64) {
        if self.shutdown {
            return;
        }
        log::info!("[DataPlane] Shutdown");
        self.features.input(&mut self.switcher).on_shutdown(&mut self.feature_ctx, now_ms);
        self.services.input(&mut self.switcher).on_shutdown(&self.service_ctx, now_ms);
        self.shutdown = true;
    }

    fn incoming_route(&mut self, now_ms: u64, pair: NetPair, mut buf: Buffer) {
        let conn = return_if_none!(self.conns.get_mut(&pair));
        if TransportMsgHeader::is_secure(buf[0]) {
            return_if_none!(conn.decrypt_if_need(now_ms, &mut buf));
        }
        let header = return_if_err!(TransportMsgHeader::try_from(&buf as &[u8]));
        let action = self.feature_ctx.router.derive_action(&header.route, header.from_node, Some(conn.node()));
        log::debug!("[DataPlane] Incoming rule: {:?} from: {pair}, node {:?} => action {:?}", header.route, header.from_node, action);
        match action {
            RouteAction::Reject => {}
            RouteAction::Local => {
                let feature = return_if_none!(header.feature.try_into().ok());
                log::debug!("Incoming message for feature: {feature:?} from: {pair}");
                self.features
                    .input(&mut self.switcher)
                    .on_network_raw(&mut self.feature_ctx, feature, now_ms, conn.conn(), pair, header, buf);
            }
            RouteAction::Next(pair) => {
                if !TransportMsgHeader::decrease_ttl(&mut buf) {
                    log::debug!("TTL is 0, drop packet");
                }
                let target_conn = return_if_none!(self.conns.get_mut(&pair));
                if let Some(out) = Self::build_send_to_from_mut(now_ms, target_conn, pair, buf) {
                    self.queue.push_back(out.into());
                }
            }
            RouteAction::Broadcast(local, pairs) => {
                if !TransportMsgHeader::decrease_ttl(&mut buf) {
                    log::debug!("TTL is 0, drop packet");
                    return;
                }
                if local {
                    if let Ok(feature) = header.feature.try_into() {
                        log::debug!("Incoming broadcast feature: {feature:?} from: {pair}");
                        self.features
                            .input(&mut self.switcher)
                            .on_network_raw(&mut self.feature_ctx, feature, now_ms, conn.conn(), pair, header, buf.clone());
                    }
                }
                if !pairs.is_empty() {
                    log::debug!("Incoming broadcast from: {pair} forward to: {pairs:?}");
                    if let Some(out) = self.build_send_to_multi_from_mut(now_ms, pairs, buf) {
                        self.queue.push_back(out.into());
                    }
                }
            }
        }
    }

    fn outgoing_route(&mut self, now_ms: u64, feature: Features, rule: RouteRule, mut meta: NetOutgoingMeta, buf: Buffer) {
        match self.feature_ctx.router.derive_action(&rule, Some(self.feature_ctx.node_id), None) {
            RouteAction::Reject => {
                log::debug!("[DataPlane] outgoing route rule {:?} is rejected", rule);
            }
            RouteAction::Local => {
                log::debug!("[DataPlane] outgoing route rule {:?} is processed locally", rule);
                let meta = meta.to_incoming(self.feature_ctx.node_id);
                self.features
                    .input(&mut self.switcher)
                    .on_input(&mut self.feature_ctx, feature, now_ms, FeatureWorkerInput::Local(meta, buf));
            }
            RouteAction::Next(remote) => {
                log::debug!("[DataPlane] outgoing route rule {:?} is go with remote {remote}", rule);
                let header = meta.to_header(feature as u8, rule, self.feature_ctx.node_id);
                let msg = TransportMsg::build_raw(header, buf);
                let conn = return_if_none!(self.conns.get_mut(&remote));
                if let Some(out) = Self::build_send_to_from_mut(now_ms, conn, remote, msg.take()) {
                    self.queue.push_back(out.into());
                }
            }
            RouteAction::Broadcast(local, remotes) => {
                log::debug!("[DataPlane] outgoing route rule {:?} is go with local {local} and remotes {:?}", rule, remotes);
                meta.source = true; //Force enable source for broadcast

                let header = meta.to_header(feature as u8, rule, self.feature_ctx.node_id);
                if local {
                    let meta = meta.to_incoming(self.feature_ctx.node_id);
                    self.features
                        .input(&mut self.switcher)
                        .on_input(&mut self.feature_ctx, feature, now_ms, FeatureWorkerInput::Local(meta, buf.clone()));
                }
                let msg = TransportMsg::build_raw(header, buf);
                if let Some(out) = self.build_send_to_multi_from_mut(now_ms, remotes, msg.take()) {
                    self.queue.push_back(out.into());
                }
            }
        }
    }

    fn pop_features(&mut self, now_ms: u64) {
        let out = return_if_none!(self.features.pop_output(now_ms, &mut self.switcher));
        let (feature, out) = match out {
            features::Output::Output(feature, out) => (feature, out),
            features::Output::OnResourceEmpty => {
                log::info!("[DataPlane] Features OnResourceEmpty");
                return;
            }
        };
        match out {
            FeatureWorkerOutput::ForwardControlToController(service, control) => self.queue.push_back(LogicControl::FeaturesControl(service, control).into()),
            FeatureWorkerOutput::ForwardNetworkToController(conn, header, msg) => self.queue.push_back(LogicControl::NetRemote(feature, conn, header, msg).into()),
            FeatureWorkerOutput::ForwardLocalToController(header, buf) => self.queue.push_back(LogicControl::NetLocal(feature, header, buf).into()),
            FeatureWorkerOutput::ToController(control) => self.queue.push_back(LogicControl::Feature(control).into()),
            FeatureWorkerOutput::Event(actor, event) => match actor {
                FeatureControlActor::Controller(userdata) => self.queue.push_back(Output::Control(LogicControl::ExtFeaturesEvent(userdata, event))),
                FeatureControlActor::Worker(worker, userdata) => {
                    if self.worker_id == worker {
                        self.queue.push_back(Output::Ext(ExtOut::FeaturesEvent(userdata, event)));
                    } else {
                        self.queue.push_back(Output::Worker(worker, CrossWorker::Feature(userdata, event)));
                    }
                }
                FeatureControlActor::Service(service) => {
                    self.services
                        .input(&mut self.switcher)
                        .on_input(&self.service_ctx, now_ms, service, ServiceWorkerInput::FeatureEvent(event));
                }
            },
            FeatureWorkerOutput::SendDirect(conn, meta, buf) => {
                if let Some(addr) = self.conns_reverse.get(&conn) {
                    let conn = self.conns.get_mut(addr).expect("Should have");
                    let header = meta.to_header(feature as u8, RouteRule::Direct, self.feature_ctx.node_id);
                    let msg = TransportMsg::build_raw(header, buf);
                    self.queue.push_back(Self::build_send_to_from_mut(now_ms, conn, *addr, msg.take()).expect("Should have output").into())
                }
            }
            FeatureWorkerOutput::SendRoute(rule, ttl, buf) => {
                log::info!("SendRoute: {:?}", rule);
                self.outgoing_route(now_ms, feature, rule, ttl, buf);
            }
            FeatureWorkerOutput::RawDirect(conn, buf) => {
                if let Some(pair) = self.conns_reverse.get(&conn) {
                    let conn = self.conns.get_mut(pair).expect("Should have conn");
                    self.queue.push_back(Self::build_send_to(now_ms, conn, *pair, buf).expect("Should ok for convert RawDirect").into());
                }
            }
            FeatureWorkerOutput::RawBroadcast(conns, buf) => {
                let addrs = conns.iter().filter_map(|conn| self.conns_reverse.get(conn)).cloned().collect();
                let out = self.build_send_to_multi(now_ms, addrs, buf).map(|e| e.into()).unwrap_or(Output::Continue);
                self.queue.push_back(out);
            }
            FeatureWorkerOutput::RawDirect2(pair, buf) => {
                if let Some(conn) = self.conns.get_mut(&pair) {
                    self.queue.push_back(Self::build_send_to(now_ms, conn, pair, buf).expect("Should ok for convert RawDirect2").into());
                }
            }
            FeatureWorkerOutput::RawBroadcast2(pairs, buf) => {
                let out = self.build_send_to_multi(now_ms, pairs, buf).map(|e| e.into()).unwrap_or(Output::Continue);
                self.queue.push_back(out);
            }
            #[cfg(feature = "vpn")]
            FeatureWorkerOutput::TunPkt(pkt) => self.queue.push_back(NetOutput::TunPacket(pkt).into()),
            FeatureWorkerOutput::OnResourceEmpty => {
                log::info!("[DataPlane] Feature {feature:?} OnResourceEmpty");
            }
        }
    }

    fn pop_services(&mut self, now_ms: u64) {
        let out = return_if_none!(self.services.pop_output(now_ms, &mut self.switcher));
        let (service, out) = match out {
            services::Output::Output(service, out) => (service, out),
            services::Output::OnResourceEmpty => {
                log::info!("[DataPlane] Services OnResourceEmpty");
                return;
            }
        };
        match out {
            ServiceWorkerOutput::ForwardControlToController(actor, control) => self.queue.push_back(LogicControl::ServicesControl(actor, service, control).into()),
            ServiceWorkerOutput::ForwardFeatureEventToController(event) => self.queue.push_back(LogicControl::ServiceEvent(service, event).into()),
            ServiceWorkerOutput::ToController(tc) => self.queue.push_back(LogicControl::Service(service, tc).into()),
            ServiceWorkerOutput::FeatureControl(control) => {
                let feature = control.to_feature();
                self.features
                    .input(&mut self.switcher)
                    .on_input(&mut self.feature_ctx, feature, now_ms, FeatureWorkerInput::Control(FeatureControlActor::Service(service), control));
            }
            ServiceWorkerOutput::Event(actor, event) => match actor {
                ServiceControlActor::Controller(userdata) => self.queue.push_back(Output::Control(LogicControl::ExtServicesEvent(service, userdata, event))),
                ServiceControlActor::Worker(worker, userdata) => {
                    if self.worker_id == worker {
                        self.queue.push_back(Output::Ext(ExtOut::ServicesEvent(service, userdata, event)));
                    } else {
                        self.queue.push_back(Output::Worker(worker, CrossWorker::Service(service, userdata, event)));
                    }
                }
            },
            ServiceWorkerOutput::OnResourceEmpty => {
                log::info!("[DataPlane] Service {service} OnResourceEmpty");
            }
        }
    }

    fn build_send_to_from_mut(now: u64, conn: &mut DataPlaneConnection, pair: NetPair, mut buf: Buffer) -> Option<NetOutput> {
        conn.encrypt_if_need(now, &mut buf)?;
        Some(NetOutput::UdpPacket(pair, buf))
    }

    fn build_send_to_multi_from_mut(&mut self, now: u64, mut pairs: Vec<NetPair>, mut buf: Buffer) -> Option<NetOutput> {
        if TransportMsgHeader::is_secure(buf[0]) {
            let first = pairs.pop()?;
            for pair in pairs {
                if let Some(conn) = self.conns.get_mut(&pair) {
                    let mut buf = Buffer::build(&buf, 0, 12 + 16);
                    if conn.encrypt_if_need(now, &mut buf).is_some() {
                        let out = NetOutput::UdpPacket(pair, buf);
                        self.queue.push_back(Output::Net(out));
                    }
                }
            }
            let conn = self.conns.get_mut(&first)?;
            conn.encrypt_if_need(now, &mut buf)?;
            Some(NetOutput::UdpPacket(first, buf))
        } else {
            Some(NetOutput::UdpPackets(pairs, buf))
        }
    }

    fn build_send_to_multi(&mut self, now: u64, pairs: Vec<NetPair>, buf: Buffer) -> Option<NetOutput> {
        if TransportMsgHeader::is_secure(buf[0]) {
            let buf = Buffer::build(&buf, 0, 12 + 16);
            self.build_send_to_multi_from_mut(now, pairs, buf)
        } else {
            Some(NetOutput::UdpPackets(pairs, buf))
        }
    }

    fn build_send_to(now: u64, conn: &mut DataPlaneConnection, pair: NetPair, buf: Buffer) -> Option<NetOutput> {
        if TransportMsgHeader::is_secure(buf[0]) {
            let buf = Buffer::build(&buf, 0, 12 + 16);
            Self::build_send_to_from_mut(now, conn, pair, buf)
        } else {
            Some(NetOutput::UdpPacket(pair, buf))
        }
    }
}

impl<UserData, SC, SE, TC, TW> TaskSwitcherChild<Output<UserData, SC, SE, TC>> for DataPlane<UserData, SC, SE, TC, TW>
where
    UserData: 'static + Copy + Eq + Hash + Debug,
{
    type Time = u64;

    fn empty_event(&self) -> Output<UserData, SC, SE, TC> {
        Output::OnResourceEmpty
    }

    fn is_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty() && self.features.is_empty() && self.services.is_empty()
    }

    fn pop_output(&mut self, now: u64) -> Option<Output<UserData, SC, SE, TC>> {
        return_if_some!(self.queue.pop_front());

        while let Some(current) = self.switcher.current() {
            match current.try_into().ok()? {
                TaskType::Feature => self.pop_features(now),
                TaskType::Service => self.pop_services(now),
            }

            return_if_some!(self.queue.pop_front());
        }

        None
    }
}
