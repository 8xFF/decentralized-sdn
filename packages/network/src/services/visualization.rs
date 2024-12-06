use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    net::SocketAddr,
};

use atm0s_sdn_identity::{ConnId, NodeId};
use atm0s_sdn_router::{RouteRule, ServiceBroadcastLevel};
use sans_io_runtime::collections::DynamicDeque;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    base::{
        ConnectionEvent, NetOutgoingMeta, Service, ServiceBuilder, ServiceControlActor, ServiceCtx, ServiceInput, ServiceOutput, ServiceSharedInput, ServiceWorker, ServiceWorkerCtx,
        ServiceWorkerInput, ServiceWorkerOutput, Ttl,
    },
    features::{data, FeaturesControl, FeaturesEvent},
};

pub const SERVICE_ID: u8 = 1;
pub const SERVICE_NAME: &str = "visualization";

const NODE_TIMEOUT_MS: u64 = 10000; // after 10 seconds of no ping, node is considered dead
const NODE_PING_MS: u64 = 5000;
const NODE_PING_TTL: u8 = 5;

const DATA_PORT: u16 = 0;

fn data_cmd<UserData, SE, TW>(cmd: data::Control) -> ServiceOutput<UserData, FeaturesControl, SE, TW> {
    ServiceOutput::FeatureControl(FeaturesControl::Data(cmd))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub conn: ConnId,
    pub dest: NodeId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub rtt_ms: u32,
}

struct NodeInfo<Info> {
    last_ping_ms: u64,
    info: Info,
    conns: Vec<ConnectionInfo>,
}

#[derive(Debug, Clone)]
pub enum Control<Info> {
    Subscribe,
    GetAll,
    UpdateInfo(Info),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<Info> {
    GotAll(Vec<(NodeId, Info, Vec<ConnectionInfo>)>),
    NodeChanged(NodeId, Info, Vec<ConnectionInfo>),
    NodeRemoved(NodeId),
}

#[derive(Debug, Serialize, Deserialize)]
enum Message<Info> {
    Snapshot(NodeId, Info, Vec<ConnectionInfo>),
}

pub struct VisualizationService<UserData, SC, SE, TC, TW, Info> {
    info: Info,
    last_ping: u64,
    broadcast_seq: u16,
    queue: VecDeque<ServiceOutput<UserData, FeaturesControl, SE, TW>>,
    conns: BTreeMap<ConnId, ConnectionInfo>,
    network_nodes: BTreeMap<NodeId, NodeInfo<Info>>,
    subscribers: Vec<ServiceControlActor<UserData>>,
    shutdown: bool,
    _tmp: std::marker::PhantomData<(SC, TC)>,
}

impl<UserData: Copy, SC, SE, TC, TW, Info: Clone> VisualizationService<UserData, SC, SE, TC, TW, Info>
where
    SC: From<Control<Info>> + TryInto<Control<Info>>,
    SE: From<Event<Info>> + TryInto<Event<Info>>,
{
    pub fn new(info: Info) -> Self {
        Self {
            info,
            broadcast_seq: 0,
            last_ping: 0,
            conns: BTreeMap::new(),
            network_nodes: BTreeMap::new(),
            queue: VecDeque::from([ServiceOutput::FeatureControl(FeaturesControl::Data(data::Control::DataListen(DATA_PORT)))]),
            subscribers: Vec::new(),
            shutdown: false,
            _tmp: std::marker::PhantomData,
        }
    }

    fn fire_event(&mut self, event: Event<Info>) {
        for sub in self.subscribers.iter() {
            self.queue.push_back(ServiceOutput::Event(*sub, event.clone().into()));
        }
    }
}

impl<UserData: Copy + Eq, SC, SE, TC, TW, Info> Service<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for VisualizationService<UserData, SC, SE, TC, TW, Info>
where
    Info: Debug + Clone + Serialize + DeserializeOwned,
    SC: From<Control<Info>> + TryInto<Control<Info>>,
    SE: From<Event<Info>> + TryInto<Event<Info>>,
{
    fn is_service_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn service_id(&self) -> u8 {
        SERVICE_ID
    }

    fn service_name(&self) -> &str {
        SERVICE_NAME
    }

    fn on_shared_input<'a>(&mut self, ctx: &ServiceCtx, now: u64, input: ServiceSharedInput) {
        match input {
            ServiceSharedInput::Tick(_) => {
                let mut to_remove = Vec::new();
                for (node, info) in self.network_nodes.iter() {
                    if now >= NODE_TIMEOUT_MS + info.last_ping_ms {
                        log::warn!("[Visualization] Node {} is dead after timeout {NODE_TIMEOUT_MS} ms", node);
                        to_remove.push(*node);
                    }
                }
                for node in to_remove {
                    self.fire_event(Event::NodeRemoved(node));
                    self.network_nodes.remove(&node);
                }

                if now >= self.last_ping + NODE_PING_MS {
                    log::debug!("[Visualization] Sending Snapshot to collector with interval {NODE_PING_MS} ms with {} conns", self.conns.len());
                    self.last_ping = now;
                    let msg = Message::Snapshot(ctx.node_id, self.info.clone(), self.conns.values().cloned().collect::<Vec<_>>());
                    let seq = self.broadcast_seq;
                    self.broadcast_seq = self.broadcast_seq.wrapping_add(1);
                    self.queue.push_back(data_cmd(data::Control::DataSendRule(
                        DATA_PORT,
                        RouteRule::ToServices(SERVICE_ID, ServiceBroadcastLevel::Global, seq),
                        NetOutgoingMeta::new(false, Ttl(NODE_PING_TTL), 0, true),
                        bincode::serialize(&msg).expect("Should to bytes"),
                    )));
                }
            }
            ServiceSharedInput::Connection(ConnectionEvent::Connecting(_ctx)) => {}
            ServiceSharedInput::Connection(ConnectionEvent::ConnectError(_ctx, _err)) => {}
            ServiceSharedInput::Connection(ConnectionEvent::Connected(ctx, _)) => {
                log::info!("[Visualization] New connection from {} to {}, set default rtt_ms to 1000ms", ctx.pair, ctx.node);
                self.conns.insert(
                    ctx.conn,
                    ConnectionInfo {
                        conn: ctx.conn,
                        dest: ctx.node,
                        local: ctx.pair.local,
                        remote: ctx.pair.remote,
                        rtt_ms: 1000,
                    },
                );
            }
            ServiceSharedInput::Connection(ConnectionEvent::Stats(ctx, stats)) => {
                log::debug!("[Visualization] Update rtt_ms for connection from {} to {} to {}ms", ctx.pair, ctx.node, stats.rtt_ms);
                let entry = self.conns.entry(ctx.conn).or_insert(ConnectionInfo {
                    conn: ctx.conn,
                    dest: ctx.node,
                    local: ctx.pair.local,
                    remote: ctx.pair.remote,
                    rtt_ms: 1000,
                });
                entry.rtt_ms = stats.rtt_ms;
            }
            ServiceSharedInput::Connection(ConnectionEvent::Disconnected(ctx)) => {
                log::info!("[Visualization] Connection from {} to {} is disconnected", ctx.pair, ctx.node);
                self.conns.remove(&ctx.conn);
            }
        }
    }

    fn on_input(&mut self, _ctx: &ServiceCtx, now: u64, input: ServiceInput<UserData, FeaturesEvent, SC, TC>) {
        match input {
            ServiceInput::FeatureEvent(FeaturesEvent::Data(data::Event::Recv(_port, meta, buf))) => {
                if !meta.secure {
                    log::warn!("[Visualization] reject unsecure message");
                    return;
                }
                if let Ok(msg) = bincode::deserialize::<Message<Info>>(&buf) {
                    match msg {
                        Message::Snapshot(from, info, conns) => {
                            log::debug!("[Visualization] Got snapshot from {} with info {:?} {} connections", from, info, conns.len());
                            self.fire_event(Event::NodeChanged(from, info.clone(), conns.clone()));
                            self.network_nodes.insert(from, NodeInfo { last_ping_ms: now, info, conns });
                        }
                    }
                }
            }
            ServiceInput::Control(actor, control) => {
                let mut push_all = || {
                    let all = self.network_nodes.iter().map(|(k, v)| (*k, v.info.clone(), v.conns.clone())).collect();
                    self.queue.push_back(ServiceOutput::Event(actor, Event::GotAll(all).into()));
                };
                if let Ok(control) = control.try_into() {
                    match control {
                        Control::GetAll => {
                            push_all();
                        }
                        Control::Subscribe => {
                            if !self.subscribers.contains(&actor) {
                                self.subscribers.push(actor);
                                log::info!("[Visualization] New subscriber, sending snapshot with {} nodes", self.network_nodes.len());
                                push_all();
                            }
                        }
                        Control::UpdateInfo(info) => {
                            self.info = info;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn on_shutdown(&mut self, _ctx: &ServiceCtx, _now: u64) {
        log::info!("[VisualizationService] Shutdown");
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceOutput<UserData, FeaturesControl, SE, TW>> {
        self.queue.pop_front()
    }
}

pub struct VisualizationServiceWorker<UserData, SC, SE, TC> {
    queue: DynamicDeque<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>, 8>,
    shutdown: bool,
}

impl<UserData, SC, SE, TC, TW> ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for VisualizationServiceWorker<UserData, SC, SE, TC> {
    fn is_service_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn service_id(&self) -> u8 {
        SERVICE_ID
    }

    fn service_name(&self) -> &str {
        SERVICE_NAME
    }

    fn on_tick(&mut self, _ctx: &ServiceWorkerCtx, _now: u64, _tick_count: u64) {}

    fn on_input(&mut self, _ctx: &ServiceWorkerCtx, _now: u64, input: ServiceWorkerInput<UserData, FeaturesEvent, SC, TW>) {
        match input {
            ServiceWorkerInput::Control(actor, control) => self.queue.push_back(ServiceWorkerOutput::ForwardControlToController(actor, control)),
            ServiceWorkerInput::FeatureEvent(event) => self.queue.push_back(ServiceWorkerOutput::ForwardFeatureEventToController(event)),
            ServiceWorkerInput::FromController(_) => {}
        }
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>> {
        self.queue.pop_front()
    }

    fn on_shutdown(&mut self, _ctx: &ServiceWorkerCtx, _now: u64) {
        self.shutdown = true;
    }
}

pub struct VisualizationServiceBuilder<UserData, SC, SE, TC, TW, Info> {
    info: Info,
    collector: bool,
    _tmp: std::marker::PhantomData<(UserData, SC, SE, TC, TW, Info)>,
}

impl<UserData, SC, SE, TC, TW, Info> VisualizationServiceBuilder<UserData, SC, SE, TC, TW, Info> {
    pub fn new(info: Info, collector: bool) -> Self {
        log::info!("[Visualization] started as collector node => will receive metric from all other nodes");
        Self {
            info,
            collector,
            _tmp: std::marker::PhantomData,
        }
    }
}

impl<UserData, SC, SE, TC, TW, Info> ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for VisualizationServiceBuilder<UserData, SC, SE, TC, TW, Info>
where
    UserData: 'static + Debug + Send + Sync + Copy + Eq,
    Info: 'static + Debug + Serialize + DeserializeOwned + Clone + Send + Sync,
    SC: 'static + Debug + Send + Sync + From<Control<Info>> + TryInto<Control<Info>>,
    SE: 'static + Debug + Send + Sync + From<Event<Info>> + TryInto<Event<Info>>,
    TC: 'static + Debug + Send + Sync,
    TW: 'static + Debug + Send + Sync,
{
    fn service_id(&self) -> u8 {
        SERVICE_ID
    }

    fn service_name(&self) -> &str {
        SERVICE_NAME
    }

    fn discoverable(&self) -> bool {
        self.collector
    }

    fn create(&self) -> Box<dyn Service<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>> {
        Box::new(VisualizationService::new(self.info.clone()))
    }

    fn create_worker(&self) -> Box<dyn ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>> {
        Box::new(VisualizationServiceWorker {
            queue: Default::default(),
            shutdown: false,
        })
    }
}

#[cfg(test)]
mod test {
    use atm0s_sdn_identity::{ConnId, NodeId};
    use atm0s_sdn_router::{RouteRule, ServiceBroadcastLevel};
    use serde::{Deserialize, Serialize};

    use crate::{
        base::{ConnectionCtx, ConnectionEvent, MockDecryptor, MockEncryptor, NetIncomingMeta, NetOutgoingMeta, SecureContext, Service, ServiceCtx, ServiceInput, ServiceSharedInput, Ttl},
        data_plane::NetPair,
        features::{
            data::{Control as DataControl, Event as DataEvent},
            FeaturesEvent,
        },
        services::visualization::{data_cmd, Message, DATA_PORT, NODE_PING_MS, NODE_PING_TTL, NODE_TIMEOUT_MS},
    };

    use super::{Control, Event, VisualizationService, SERVICE_ID};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    struct Info(u8);

    fn data_event(event: DataEvent) -> ServiceInput<(), FeaturesEvent, Control<Info>, ()> {
        ServiceInput::FeatureEvent(FeaturesEvent::Data(event))
    }

    fn connected_event(node: NodeId) -> ConnectionEvent {
        ConnectionEvent::Connected(
            ConnectionCtx {
                conn: ConnId::from_in(0, node as u64),
                node,
                pair: NetPair::new_str("1.1.1.1:1000", "2.2.2.2:2000").expect("Should parse pair"),
            },
            SecureContext {
                encryptor: Box::new(MockEncryptor::new()),
                decryptor: Box::new(MockDecryptor::new()),
            },
        )
    }

    fn disconnected_event(node: NodeId) -> ConnectionEvent {
        ConnectionEvent::Disconnected(ConnectionCtx {
            conn: ConnId::from_in(0, node as u64),
            node,
            pair: NetPair::new_str("1.1.1.1:1000", "2.2.2.2:2000").expect("Should parse pair"),
        })
    }

    #[test]
    fn agent_should_prediotic_sending_snapshot() {
        let node_info = Info(1);
        let node_id = 1;
        let ctx = ServiceCtx { node_id, session: 0 };
        let mut service = VisualizationService::<(), Control<Info>, Event<Info>, (), (), _>::new(node_info.clone());

        assert_eq!(service.pop_output2(0), Some(data_cmd(DataControl::DataListen(DATA_PORT))));
        assert_eq!(service.pop_output2(0), None);

        service.on_shared_input(&ctx, NODE_PING_MS, ServiceSharedInput::Tick(0));
        assert_eq!(
            service.pop_output2(NODE_PING_MS),
            Some(data_cmd(DataControl::DataSendRule(
                DATA_PORT,
                RouteRule::ToServices(SERVICE_ID, ServiceBroadcastLevel::Global, 0),
                NetOutgoingMeta::new(false, Ttl(NODE_PING_TTL), 0, true),
                bincode::serialize(&Message::Snapshot(node_id, node_info.clone(), vec![])).expect("Should to bytes")
            )))
        );

        service.on_shared_input(&ctx, NODE_PING_MS * 2, ServiceSharedInput::Tick(0));
        assert_eq!(
            service.pop_output2(NODE_PING_MS * 2),
            Some(data_cmd(DataControl::DataSendRule(
                DATA_PORT,
                RouteRule::ToServices(SERVICE_ID, ServiceBroadcastLevel::Global, 1),
                NetOutgoingMeta::new(false, Ttl(NODE_PING_TTL), 0, true),
                bincode::serialize(&Message::Snapshot(node_id, node_info.clone(), vec![])).expect("Should to bytes")
            )))
        );
    }

    #[test]
    fn agent_handle_connection_event() {
        let node_info = Info(1);
        let node_id = 1;
        let mut service = VisualizationService::<(), Control<Info>, Event<Info>, (), (), _>::new(node_info);

        let node2 = 2;
        let node3 = 3;

        let ctx = ServiceCtx { node_id, session: 0 };
        service.on_shared_input(&ctx, 110, ServiceSharedInput::Connection(connected_event(node2)));
        service.on_shared_input(&ctx, 110, ServiceSharedInput::Connection(connected_event(node3)));

        assert_eq!(service.conns.len(), 2);

        service.on_shared_input(&ctx, 210, ServiceSharedInput::Connection(disconnected_event(node3)));
        assert_eq!(service.conns.len(), 1);

        //TODO check with Snapshot msg too
    }

    #[test]
    fn collector_handle_snapshot_correct() {
        let node_info = Info(1);
        let node_id = 1;
        let ctx = ServiceCtx { node_id, session: 0 };
        let mut service = VisualizationService::<(), Control<Info>, Event<Info>, (), (), _>::new(node_info.clone());

        let node2_info = Info(2);
        let node2 = 2;

        let snapshot = Message::Snapshot(node2, node2_info, vec![]);
        let buf = bincode::serialize(&snapshot).expect("Should to bytes");
        service.on_input(&ctx, 100, data_event(DataEvent::Recv(DATA_PORT, NetIncomingMeta::new(None, NODE_PING_TTL.into(), 0, true), buf)));

        assert_eq!(service.network_nodes.len(), 1);

        //auto delete after timeout
        service.on_shared_input(&ctx, 100 + NODE_TIMEOUT_MS, ServiceSharedInput::Tick(0));
        assert_eq!(service.network_nodes.len(), 0);
    }
}
