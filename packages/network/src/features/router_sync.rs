use std::collections::{HashMap, VecDeque};

use atm0s_sdn_identity::{ConnId, NodeId};
use atm0s_sdn_router::{
    core::{DestDelta, Metric, RegistryDelta, RegistryDestDelta, Router, RouterDelta, RouterDump, RouterSync, TableDelta},
    shadow::ShadowRouterDelta,
};
use derivative::Derivative;
use sans_io_runtime::{collections::DynamicDeque, TaskSwitcherChild};

use crate::{
    base::{ConnectionEvent, Feature, FeatureContext, FeatureInput, FeatureOutput, FeatureSharedInput, FeatureWorker, FeatureWorkerContext, FeatureWorkerInput, FeatureWorkerOutput, NetOutgoingMeta},
    data_plane::NetPair,
};

pub const FEATURE_ID: u8 = 2;
pub const FEATURE_NAME: &str = "router_sync";

const INIT_RTT_MS: u16 = 1000;
const INIT_BW: u32 = 100_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    DumpRouter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    DumpRouter(Box<RouterDump>),
}

pub type ToWorker = ShadowRouterDelta<NetPair>;
pub type ToController = ();

pub type Output<UserData> = FeatureOutput<UserData, Event, ToWorker>;
pub type WorkerOutput<UserData> = FeatureWorkerOutput<UserData, Control, Event, ToController>;

pub struct RouterSyncFeature<UserData> {
    router: Router,
    conns: HashMap<ConnId, (NodeId, NetPair, Metric)>,
    queue: VecDeque<Output<UserData>>,
    services: Vec<u8>,
    shutdown: bool,
}

impl<UserData> RouterSyncFeature<UserData> {
    pub fn new(node: NodeId, services: Vec<u8>) -> Self {
        log::info!("[RouterSync] started node {} with public services {:?}", node, services);

        Self {
            router: Router::new(node),
            services,
            conns: HashMap::new(),
            queue: VecDeque::new(),
            shutdown: false,
        }
    }

    fn send_sync_to(router: &Router, queue: &mut VecDeque<Output<UserData>>, conn: ConnId, node: NodeId) {
        let sync = router.create_sync(node);
        log::debug!("[RouterSync] send sync to {node} content {sync:?}");
        queue.push_back(FeatureOutput::SendDirect(
            conn,
            NetOutgoingMeta::new(false, 1.into(), 0, true),
            bincode::serialize(&sync).expect("").into(),
        ));
    }
}

impl<UserData> Feature<UserData, Control, Event, ToController, ToWorker> for RouterSyncFeature<UserData> {
    fn on_shared_input(&mut self, _ctx: &FeatureContext, _now: u64, input: FeatureSharedInput) {
        match input {
            FeatureSharedInput::Tick(tick_count) => {
                if tick_count < 1 {
                    //we need to wait all workers to be ready
                    return;
                }

                while let Some(service) = self.services.pop() {
                    log::info!("[RouterSync] register local service {}", service);
                    self.router.register_service(service);
                }

                for (conn, (node, _, _)) in self.conns.iter() {
                    Self::send_sync_to(&self.router, &mut self.queue, *conn, *node);
                }
            }
            FeatureSharedInput::Connection(event) => match event {
                ConnectionEvent::Connecting(_ctx) => {}
                ConnectionEvent::ConnectError(_ctx, _err) => {}
                ConnectionEvent::Connected(ctx, _) => {
                    log::info!("[RouterSync] Connection {} connected", ctx.pair);
                    let metric = Metric::new(INIT_RTT_MS, vec![ctx.node], INIT_BW);
                    self.conns.insert(ctx.conn, (ctx.node, ctx.pair, metric.clone()));
                    self.router.set_direct(ctx.conn, metric);
                    Self::send_sync_to(&self.router, &mut self.queue, ctx.conn, ctx.node);
                }
                ConnectionEvent::Stats(ctx, stats) => {
                    log::debug!("[RouterSync] Connection {} stats rtt_ms {}", ctx.pair, stats.rtt_ms);
                    let metric = Metric::new(stats.rtt_ms as u16, vec![ctx.node], INIT_BW);
                    self.conns.insert(ctx.conn, (ctx.node, ctx.pair, metric.clone()));
                    self.router.set_direct(ctx.conn, metric);
                }
                ConnectionEvent::Disconnected(ctx) => {
                    log::info!("[RouterSync] Connection {} disconnected", ctx.pair);
                    self.conns.remove(&ctx.conn);
                    self.router.del_direct(ctx.conn);
                }
            },
        }
    }

    fn on_input(&mut self, _ctx: &FeatureContext, _now_ms: u64, input: FeatureInput<'_, UserData, Control, ToController>) {
        match input {
            FeatureInput::FromWorker(_) => {}
            FeatureInput::Control(actor, control) => match control {
                Control::DumpRouter => {
                    self.queue.push_back(FeatureOutput::Event(actor, Event::DumpRouter(Box::new(self.router.dump()))));
                }
            },
            FeatureInput::Net(ctx, meta, buf) => {
                if !meta.secure {
                    log::warn!("[RouterSync] reject unsecure message");
                    return;
                }
                if let Some((node, remote, metric)) = self.conns.get(&ctx.conn) {
                    if let Ok(sync) = bincode::deserialize::<RouterSync>(&buf) {
                        log::debug!("[RouterSync] Receive sync from {node} {remote:?}");
                        self.router.apply_sync(ctx.conn, metric.clone(), sync);
                    } else {
                        log::warn!("[RouterSync] Receive invalid sync from {}", ctx.pair);
                    }
                } else {
                    log::warn!("[RouterSync] Receive sync from unknown connection {}", ctx.pair);
                }
            }
            FeatureInput::Local(..) => {}
        }
    }

    fn on_shutdown(&mut self, _ctx: &FeatureContext, _now: u64) {
        log::info!("[RouterSync] Shutdown");
        self.shutdown = true;
    }
}

impl<UserData> TaskSwitcherChild<Output<UserData>> for RouterSyncFeature<UserData> {
    type Time = u64;

    fn is_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn empty_event(&self) -> Output<UserData> {
        Output::OnResourceEmpty
    }

    fn pop_output(&mut self, _now: u64) -> Option<Output<UserData>> {
        if let Some(rule) = self.router.pop_delta() {
            log::debug!("[RouterSync] broadcast to all workers {:?}", rule);
            let rule = match rule {
                RouterDelta::Table(layer, TableDelta(index, DestDelta::SetBestPath(conn))) => ShadowRouterDelta::SetTable {
                    layer,
                    index,
                    next: self.conns.get(&conn)?.1,
                },
                RouterDelta::Table(layer, TableDelta(index, DestDelta::DelBestPath)) => ShadowRouterDelta::DelTable { layer, index },
                RouterDelta::Registry(RegistryDelta::SetServiceLocal(service)) => ShadowRouterDelta::SetServiceLocal { service },
                RouterDelta::Registry(RegistryDelta::DelServiceLocal(service)) => ShadowRouterDelta::DelServiceLocal { service },
                RouterDelta::Registry(RegistryDelta::ServiceRemote(service, RegistryDestDelta::SetServicePath(conn, dest, score))) => {
                    let conn = self.conns.get(&conn)?;
                    ShadowRouterDelta::SetServiceRemote {
                        service,
                        conn: conn.1,
                        next: conn.0,
                        dest,
                        score,
                    }
                }
                RouterDelta::Registry(RegistryDelta::ServiceRemote(service, RegistryDestDelta::DelServicePath(conn))) => ShadowRouterDelta::DelServiceRemote {
                    service,
                    conn: self.conns.get(&conn)?.1,
                },
            };
            return Some(FeatureOutput::ToWorker(true, rule));
        }
        self.queue.pop_front()
    }
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub struct RouterSyncFeatureWorker<UserData> {
    queue: DynamicDeque<WorkerOutput<UserData>, 1>,
    shutdown: bool,
}

impl<UserData> FeatureWorker<UserData, Control, Event, ToController, ToWorker> for RouterSyncFeatureWorker<UserData> {
    fn on_input(&mut self, ctx: &mut FeatureWorkerContext, _now: u64, input: FeatureWorkerInput<UserData, Control, ToWorker>) {
        match input {
            FeatureWorkerInput::Control(service, control) => self.queue.push_back(FeatureWorkerOutput::ForwardControlToController(service, control)),
            FeatureWorkerInput::Network(conn, header, msg) => self.queue.push_back(FeatureWorkerOutput::ForwardNetworkToController(conn, header, msg)),
            FeatureWorkerInput::FromController(_, delta) => {
                log::debug!("[RouterSyncWorker] apply router delta {:?}", delta);
                ctx.router.apply_delta(delta);
            }
            FeatureWorkerInput::Local(_header, _msg) => {
                log::warn!("No handler for local message in {}", FEATURE_NAME);
            }
            #[cfg(feature = "vpn")]
            FeatureWorkerInput::TunPkt(_buf) => {
                log::warn!("No handler for tun packet in {}", FEATURE_NAME);
            }
        }
    }

    fn on_shutdown(&mut self, _ctx: &mut FeatureWorkerContext, _now: u64) {
        log::info!("[RouterSyncFeatureWorker] Shutdown");
        self.shutdown = true;
    }
}

impl<UserData> TaskSwitcherChild<WorkerOutput<UserData>> for RouterSyncFeatureWorker<UserData> {
    type Time = u64;

    fn is_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn empty_event(&self) -> WorkerOutput<UserData> {
        WorkerOutput::OnResourceEmpty
    }

    fn pop_output(&mut self, _now: u64) -> Option<WorkerOutput<UserData>> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use atm0s_sdn_router::core::{Metric, RegistrySync, RouterSync, TableSync};

    #[test]
    fn router_sync_should_fit_udp() {
        const MAX_SIZE: usize = 1200;
        const NUMBER_SERVICES: usize = 2;
        const NUMBER_NEIGHBORS: usize = 10;
        const NUMBER_NODE_PATH: u32 = 3;

        let mut service_sync = RegistrySync(vec![]);
        let mut table_sync = [None, None, None, None];

        for _ in 0..NUMBER_SERVICES {
            service_sync.0.push((rand::random(), Metric::new(0, (0..NUMBER_NODE_PATH).collect::<Vec<_>>(), 0)));
        }

        for i in &mut table_sync {
            let mut table = TableSync(vec![]);
            for _ in 0..NUMBER_NEIGHBORS {
                table.0.push((rand::random(), Metric::new(0, (0..NUMBER_NODE_PATH).collect::<Vec<_>>(), 0)));
            }
            *i = Some(table);
        }

        let sync = RouterSync(service_sync, table_sync);
        let sync_msg_len = bincode::serialize(&sync).expect("").len();
        assert!(sync_msg_len <= MAX_SIZE, "SYNC msg not fit in UDP {} vs {}", sync_msg_len, MAX_SIZE);
    }
}
