//! This service implement a node discovery using service-broadcast feature
//! Each node will be have a list of broadcast service target and broadcast level.
//! Example: (Service 1, Global), (Service 2, Inner Zone)
//!
//! Each nodes based on configured above list will be try to broadcast it address in order to allow other nodes to connect to it.
//!
//! The origin idea of this method is for avoiding network partition. By rely on service broadcast, each node can adverise its address to other nodes even if network is incomplete state.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
};

use atm0s_sdn_identity::{ConnId, NodeAddr, NodeId};
use atm0s_sdn_router::{RouteRule, ServiceBroadcastLevel};
use sans_io_runtime::collections::DynamicDeque;

use crate::{
    base::{
        ConnectionEvent, NetOutgoingMeta, Service, ServiceBuilder, ServiceCtx, ServiceId, ServiceInput, ServiceOutput, ServiceSharedInput, ServiceWorker, ServiceWorkerCtx, ServiceWorkerInput,
        ServiceWorkerOutput, Ttl,
    },
    features::{data::Control as DataControl, neighbours::Control as NeighbourControl, FeaturesControl, FeaturesEvent},
};

const DATA_PORT: u16 = 2;
pub const SERVICE_ID: u8 = 2;
pub const SERVICE_NAME: &str = "manual2_discovery";

fn neighbour_control<UserData, SE, TW>(c: NeighbourControl) -> ServiceOutput<UserData, FeaturesControl, SE, TW> {
    ServiceOutput::FeatureControl(FeaturesControl::Neighbours(c))
}

fn data_control<UserData, SE, TW>(c: DataControl) -> ServiceOutput<UserData, FeaturesControl, SE, TW> {
    ServiceOutput::FeatureControl(FeaturesControl::Data(c))
}

#[derive(Debug, Clone)]
pub struct AdvertiseTarget {
    pub service: ServiceId,
    pub level: ServiceBroadcastLevel,
}

impl AdvertiseTarget {
    pub fn new(service: ServiceId, level: ServiceBroadcastLevel) -> Self {
        Self { service, level }
    }
}

pub struct Manual2DiscoveryService<UserData, SC, SE, TC, TW> {
    node_addr: NodeAddr,
    targets: Vec<AdvertiseTarget>,
    queue: VecDeque<ServiceOutput<UserData, FeaturesControl, SE, TW>>,
    remote_nodes: HashMap<NodeId, HashSet<ConnId>>,
    shutdown: bool,
    broadcast_seq: u16,
    broadcast_interval: u64,
    last_broadcast: u64,
    _tmp: std::marker::PhantomData<(SC, TC, TW)>,
}

impl<UserData, SC, SE, TC, TW> Manual2DiscoveryService<UserData, SC, SE, TC, TW> {
    pub fn new(node_addr: NodeAddr, targets: Vec<AdvertiseTarget>, interval: u64) -> Self {
        Self {
            node_addr,
            targets,
            queue: VecDeque::from_iter([data_control(DataControl::DataListen(DATA_PORT))]),
            remote_nodes: Default::default(),
            shutdown: false,
            broadcast_seq: 0,
            broadcast_interval: interval,
            last_broadcast: 0,
            _tmp: std::marker::PhantomData,
        }
    }
}

impl<UserData, SC, SE, TC: Debug, TW: Debug> Service<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for Manual2DiscoveryService<UserData, SC, SE, TC, TW> {
    fn is_service_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn service_id(&self) -> u8 {
        SERVICE_ID
    }

    fn service_name(&self) -> &str {
        SERVICE_NAME
    }

    fn on_shared_input<'a>(&mut self, _ctx: &ServiceCtx, now: u64, input: ServiceSharedInput) {
        match input {
            ServiceSharedInput::Tick(_) => {
                if now >= self.last_broadcast + self.broadcast_interval {
                    self.last_broadcast = now;
                    self.broadcast_seq = self.broadcast_seq.wrapping_add(1);
                    for target in &self.targets {
                        log::debug!("[Manual2DiscoveryService] advertise to service {} with level {:?}", target.service, target.level);
                        self.queue.push_back(data_control(DataControl::DataSendRule(
                            DATA_PORT,
                            RouteRule::ToServices(*target.service, target.level, self.broadcast_seq),
                            NetOutgoingMeta::new(true, Ttl::default(), 0, true),
                            self.node_addr.to_vec(),
                        )));
                    }
                }
            }
            ServiceSharedInput::Connection(connection_event) => match connection_event {
                ConnectionEvent::Connecting(connection_ctx) => {
                    self.remote_nodes.entry(connection_ctx.node).or_default().insert(connection_ctx.conn);
                }
                ConnectionEvent::ConnectError(connection_ctx, _neighbours_connect_error) => {
                    let entry = self.remote_nodes.entry(connection_ctx.node).or_default();
                    entry.remove(&connection_ctx.conn);
                    if entry.is_empty() {
                        log::warn!("[Manual2DiscoveryService] Node {} connect failed all connections => remove", connection_ctx.node);
                        self.remote_nodes.remove(&connection_ctx.node);
                    }
                }
                ConnectionEvent::Connected(_connection_ctx, _secure_context) => {}
                ConnectionEvent::Stats(_connection_ctx, _connection_stats) => {}
                ConnectionEvent::Disconnected(connection_ctx) => {
                    let entry = self.remote_nodes.entry(connection_ctx.node).or_default();
                    entry.remove(&connection_ctx.conn);
                    if entry.is_empty() {
                        self.remote_nodes.remove(&connection_ctx.node);
                        log::info!("[Manual2DiscoveryService] Node {} disconnected all connections => remove", connection_ctx.node);
                    }
                }
            },
        }
    }

    fn on_input(&mut self, _ctx: &ServiceCtx, _now: u64, input: ServiceInput<UserData, FeaturesEvent, SC, TC>) {
        match input {
            ServiceInput::Control(_, _) => {}
            ServiceInput::FromWorker(_) => {}
            ServiceInput::FeatureEvent(event) => {
                if let FeaturesEvent::Data(event) = event {
                    match event {
                        crate::features::data::Event::Pong(_, _) => todo!(),
                        crate::features::data::Event::Recv(port, meta, data) => {
                            // ignore other port
                            if port != DATA_PORT {
                                log::warn!("[Manual2DiscoveryService] Recv from other port => ignore");
                                return;
                            }
                            // ignore unsecure
                            if !meta.secure {
                                log::warn!("[Manual2DiscoveryService] Recv from unsecure node => ignore");
                                return;
                            }
                            if let Some(source) = meta.source {
                                // ignore self
                                if source == self.node_addr.node_id() {
                                    return;
                                }
                                // ignore already connected
                                if self.remote_nodes.contains_key(&source) {
                                    log::warn!("[Manual2DiscoveryService] Recv from already connected node {source} => ignore");
                                    return;
                                }
                            } else {
                                // ignore anonymous
                                log::warn!("[Manual2DiscoveryService] Recv from anonymous node => ignore");
                                return;
                            }
                            if let Some(addr) = NodeAddr::from_vec(&data) {
                                log::info!("[Manual2DiscoveryService] Node {} advertised => try connect {addr:?}", addr.node_id());
                                self.queue.push_back(neighbour_control(NeighbourControl::ConnectTo(addr, false)));
                            } else {
                                log::warn!("[Manual2DiscoveryService] Node {:?} advertised invalid address => ignore", meta.source);
                            }
                        }
                    }
                }
            }
        }
    }

    fn on_shutdown(&mut self, _ctx: &ServiceCtx, _now: u64) {
        log::info!("[Manual2DiscoveryService] Shutdown");
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceOutput<UserData, FeaturesControl, SE, TW>> {
        self.queue.pop_front()
    }
}

pub struct Manual2DiscoveryServiceWorker<UserData, SC, SE, TC> {
    queue: DynamicDeque<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>, 8>,
    shutdown: bool,
}

impl<UserData, SC, SE, TC, TW> ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for Manual2DiscoveryServiceWorker<UserData, SC, SE, TC> {
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

    fn on_shutdown(&mut self, _ctx: &ServiceWorkerCtx, _now: u64) {
        log::info!("[Manual2DiscoveryServiceWorker] Shutdown");
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>> {
        self.queue.pop_front()
    }
}

pub struct Manual2DiscoveryServiceBuilder<UserData, SC, SE, TC, TW> {
    _tmp: std::marker::PhantomData<(UserData, SC, SE, TC, TW)>,
    node_addr: NodeAddr,
    targets: Vec<AdvertiseTarget>,
    interval: u64,
}

impl<UserData, SC, SE, TC, TW> Manual2DiscoveryServiceBuilder<UserData, SC, SE, TC, TW> {
    pub fn new(node_addr: NodeAddr, targets: Vec<AdvertiseTarget>, interval: u64) -> Self {
        Self {
            _tmp: std::marker::PhantomData,
            node_addr,
            targets,
            interval,
        }
    }
}

impl<UserData, SC, SE, TC, TW> ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for Manual2DiscoveryServiceBuilder<UserData, SC, SE, TC, TW>
where
    UserData: 'static + Debug + Send + Sync,
    SC: 'static + Debug + Send + Sync,
    SE: 'static + Debug + Send + Sync,
    TC: 'static + Debug + Send + Sync,
    TW: 'static + Debug + Send + Sync,
{
    fn service_id(&self) -> u8 {
        SERVICE_ID
    }

    fn service_name(&self) -> &str {
        SERVICE_NAME
    }

    fn create(&self) -> Box<dyn Service<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>> {
        Box::new(Manual2DiscoveryService::new(self.node_addr.clone(), self.targets.clone(), self.interval))
    }

    fn create_worker(&self) -> Box<dyn ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>> {
        Box::new(Manual2DiscoveryServiceWorker {
            queue: Default::default(),
            shutdown: false,
        })
    }
}
