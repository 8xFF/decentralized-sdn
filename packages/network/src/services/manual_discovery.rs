use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
};

use atm0s_sdn_identity::{ConnId, NodeAddr, NodeId};
use atm0s_sdn_utils::hash::hash_str;
use sans_io_runtime::collections::DynamicDeque;

use crate::{
    base::{ConnectionEvent, Service, ServiceBuilder, ServiceCtx, ServiceInput, ServiceOutput, ServiceSharedInput, ServiceWorker, ServiceWorkerCtx, ServiceWorkerInput, ServiceWorkerOutput},
    features::{
        dht_kv::{Control as KvControl, Event as KvEvent, Key, Map, MapControl, MapEvent},
        neighbours::Control as NeighbourControl,
        FeaturesControl, FeaturesEvent,
    },
};

const RETRY_CONNECT_MS: u64 = 60_000; //60 seconds
const WAIT_DISCONNECT_MS: u64 = 60_000; //60 seconds

pub const SERVICE_ID: u8 = 0;
pub const SERVICE_NAME: &str = "manual_discovery";

fn kv_control<UserData, SE, TW>(c: KvControl) -> ServiceOutput<UserData, FeaturesControl, SE, TW> {
    ServiceOutput::FeatureControl(FeaturesControl::DhtKv(c))
}

fn neighbour_control<UserData, SE, TW>(c: NeighbourControl) -> ServiceOutput<UserData, FeaturesControl, SE, TW> {
    ServiceOutput::FeatureControl(FeaturesControl::Neighbours(c))
}

pub struct ManualDiscoveryService<UserData, SC, SE, TC, TW> {
    node_addr: NodeAddr,
    queue: VecDeque<ServiceOutput<UserData, FeaturesControl, SE, TW>>,
    nodes: HashMap<NodeId, NodeAddr>,
    conns: HashMap<NodeId, Vec<ConnId>>,
    removing_list: HashMap<NodeId, u64>,
    last_retry_ms: u64,
    shutdown: bool,
    _tmp: std::marker::PhantomData<(SC, TC, TW)>,
}

impl<UserData, SC, SE, TC, TW> ManualDiscoveryService<UserData, SC, SE, TC, TW> {
    pub fn new(node_addr: NodeAddr, local_tags: Vec<String>, connect_tags: Vec<String>) -> Self {
        log::info!("Creating ManualDiscoveryService for node {node_addr} with local tags {local_tags:?} and connect tags {connect_tags:?}");

        let mut queue = VecDeque::new();

        for local_tag in local_tags.iter() {
            let map = Map(hash_str(local_tag));
            log::info!("Setting local tag: {local_tag} by set key {map}");
            queue.push_back(kv_control(KvControl::MapCmd(map, MapControl::Set(Key(0), node_addr.to_vec()))));
        }

        for connect_tag in connect_tags.iter() {
            let map = Map(hash_str(connect_tag));
            log::info!("Setting connect tag: {connect_tag} by sub key {map}");
            queue.push_back(kv_control(KvControl::MapCmd(map, MapControl::Sub)));
        }

        Self {
            node_addr,
            nodes: HashMap::new(),
            conns: HashMap::new(),
            queue,
            removing_list: HashMap::new(),
            last_retry_ms: 0,
            shutdown: false,
            _tmp: std::marker::PhantomData,
        }
    }

    fn check_nodes(&mut self, now: u64) {
        if self.last_retry_ms + RETRY_CONNECT_MS <= now {
            self.last_retry_ms = now;
            for (node, addr) in self.nodes.iter() {
                if !self.conns.contains_key(node) {
                    log::warn!("ManualDiscoveryService node {node} not connected, retry connect");
                    self.queue.push_back(neighbour_control(NeighbourControl::ConnectTo(addr.clone(), false)));
                }
            }
        }

        let mut will_disconnect = vec![];
        for (node, ts) in self.removing_list.iter() {
            if now >= *ts + WAIT_DISCONNECT_MS && !self.nodes.contains_key(node) {
                log::info!("ManualDiscoveryService node {node} still in removing_list => send Disconnect");
                self.queue.push_back(neighbour_control(NeighbourControl::DisconnectFrom(*node)));
                will_disconnect.push(*node);
            }
        }

        for node in will_disconnect {
            self.removing_list.remove(&node);
        }
    }
}

impl<UserData, SC, SE, TC: Debug, TW: Debug> Service<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for ManualDiscoveryService<UserData, SC, SE, TC, TW> {
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
            ServiceSharedInput::Tick(_) => self.check_nodes(now),
            ServiceSharedInput::Connection(ConnectionEvent::Connected(ctx, _)) => {
                let entry = self.conns.entry(ctx.node).or_default();
                entry.push(ctx.conn);
            }
            ServiceSharedInput::Connection(ConnectionEvent::Disconnected(ctx)) => {
                let entry = self.conns.entry(ctx.node).or_default();
                entry.retain(|&conn| conn != ctx.conn);

                if entry.is_empty() {
                    log::info!("ManualDiscoveryService node {} disconnected all connections => remove", ctx.node);
                    self.conns.remove(&ctx.node);
                }
            }
            _ => {}
        }
    }

    fn on_input(&mut self, _ctx: &ServiceCtx, now: u64, input: ServiceInput<UserData, FeaturesEvent, SC, TC>) {
        if let ServiceInput::FeatureEvent(FeaturesEvent::DhtKv(KvEvent::MapEvent(map, event))) = input {
            match event {
                MapEvent::OnSet(_, source, value) => {
                    if source == self.node_addr.node_id() {
                        return;
                    }
                    if let Some(addr) = NodeAddr::from_vec(&value) {
                        log::info!("ManualDiscoveryService node {source} added tag {map} => connect {addr}");
                        self.nodes.insert(source, addr.clone());
                        self.queue.push_back(neighbour_control(NeighbourControl::ConnectTo(addr, false)));
                        self.removing_list.remove(&source);
                    }
                }
                MapEvent::OnDel(_, source) => {
                    self.nodes.remove(&source);
                    self.removing_list.entry(source).or_insert_with(|| {
                        log::info!("ManualDiscoveryService node {source} removed tag {map} => push to removing_list");
                        now
                    });
                }
                MapEvent::OnRelaySelected(node) => {
                    log::info!("ManualDiscoveryService relay {node} selected for tag {map}");
                }
            }
        }
    }

    fn on_shutdown(&mut self, _ctx: &ServiceCtx, _now: u64) {
        log::info!("[ManualDiscoveryService] Shutdown");
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceOutput<UserData, FeaturesControl, SE, TW>> {
        self.queue.pop_front()
    }
}

pub struct ManualDiscoveryServiceWorker<UserData, SC, SE, TC> {
    queue: DynamicDeque<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>, 8>,
    shutdown: bool,
}

impl<UserData, SC, SE, TC, TW> ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for ManualDiscoveryServiceWorker<UserData, SC, SE, TC> {
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
        log::info!("[ManualDiscoveryServiceWorker] Shutdown");
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceWorkerOutput<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC>> {
        self.queue.pop_front()
    }
}

pub struct ManualDiscoveryServiceBuilder<UserData, SC, SE, TC, TW> {
    _tmp: std::marker::PhantomData<(UserData, SC, SE, TC, TW)>,
    node_addr: NodeAddr,
    local_tags: Vec<String>,
    connect_tags: Vec<String>,
}

impl<UserData, SC, SE, TC, TW> ManualDiscoveryServiceBuilder<UserData, SC, SE, TC, TW> {
    pub fn new(node_addr: NodeAddr, local_tags: Vec<String>, connect_tags: Vec<String>) -> Self {
        Self {
            _tmp: std::marker::PhantomData,
            node_addr,
            local_tags,
            connect_tags,
        }
    }
}

impl<UserData, SC, SE, TC, TW> ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW> for ManualDiscoveryServiceBuilder<UserData, SC, SE, TC, TW>
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
        Box::new(ManualDiscoveryService::new(self.node_addr.clone(), self.local_tags.clone(), self.connect_tags.clone()))
    }

    fn create_worker(&self) -> Box<dyn ServiceWorker<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>> {
        Box::new(ManualDiscoveryServiceWorker {
            queue: Default::default(),
            shutdown: false,
        })
    }
}

#[cfg(test)]
mod test {
    use atm0s_sdn_identity::{ConnId, NodeAddr, NodeAddrBuilder, Protocol};
    use atm0s_sdn_utils::hash::hash_str;

    use crate::{
        base::{Service, ServiceCtx, ServiceInput, ServiceOutput, ServiceSharedInput},
        features::{
            dht_kv::{self, Key, Map, MapControl, MapEvent},
            neighbours, FeaturesControl, FeaturesEvent,
        },
        services::manual_discovery::{RETRY_CONNECT_MS, WAIT_DISCONNECT_MS},
    };

    use super::ManualDiscoveryService;

    fn node_addr(node: u32) -> NodeAddr {
        let mut builder = NodeAddrBuilder::new(node);
        builder.add_protocol(Protocol::Ip4([127, 0, 0, 1].into()));
        builder.add_protocol(Protocol::Udp(node as u16));
        builder.addr()
    }

    fn map_cmd<SE, TC>(map: Map, control: MapControl) -> ServiceOutput<(), FeaturesControl, SE, TC> {
        ServiceOutput::FeatureControl(FeaturesControl::DhtKv(dht_kv::Control::MapCmd(map, control)))
    }

    fn map_event<SC, TC>(map: Map, event: dht_kv::MapEvent) -> ServiceInput<(), FeaturesEvent, SC, TC> {
        ServiceInput::FeatureEvent(FeaturesEvent::DhtKv(dht_kv::Event::MapEvent(map, event)))
    }

    fn neighbour_cmd<SE, TC>(control: neighbours::Control) -> ServiceOutput<(), FeaturesControl, SE, TC> {
        ServiceOutput::FeatureControl(FeaturesControl::Neighbours(control))
    }

    fn neighbour_event<SC, TC>(event: neighbours::Event) -> ServiceInput<(), FeaturesEvent, SC, TC> {
        ServiceInput::FeatureEvent(FeaturesEvent::Neighbours(event))
    }

    #[test]
    fn should_send_connect() {
        let addr1 = node_addr(100);
        let addr2 = node_addr(101);

        let ctx = ServiceCtx { node_id: 100, session: 0 };
        let mut service = ManualDiscoveryService::<(), (), (), (), ()>::new(addr1.clone(), vec!["local".into()], vec!["connect".into()]);
        let local_map = Map(hash_str("local"));
        let connect_map = Map(hash_str("connect"));

        assert_eq!(service.pop_output2(0), Some(map_cmd(local_map, MapControl::Set(Key(0), addr1.to_vec()))));
        assert_eq!(service.pop_output2(0), Some(map_cmd(connect_map, MapControl::Sub)));

        service.on_input(&ctx, 100, map_event(connect_map, MapEvent::OnSet(Key(1), 2, addr2.to_vec())));
        assert_eq!(service.pop_output2(100), Some(neighbour_cmd(neighbours::Control::ConnectTo(addr2, false))));
    }

    #[test]
    fn should_wait_disconnect_after_remove() {
        let addr1 = node_addr(100);
        let addr2 = node_addr(101);

        let ctx = ServiceCtx { node_id: 100, session: 0 };
        let mut service = ManualDiscoveryService::<(), (), (), (), ()>::new(addr1.clone(), vec!["local".into()], vec!["connect".into()]);
        let local_map = Map(hash_str("local"));
        let connect_map = Map(hash_str("connect"));

        assert_eq!(service.pop_output2(0), Some(map_cmd(local_map, MapControl::Set(Key(0), addr1.to_vec()))));
        assert_eq!(service.pop_output2(0), Some(map_cmd(connect_map, MapControl::Sub)));

        // add node
        service.on_input(&ctx, 100, map_event(connect_map, MapEvent::OnSet(Key(1), addr2.node_id(), addr2.to_vec())));
        assert_eq!(service.pop_output2(100), Some(neighbour_cmd(neighbours::Control::ConnectTo(addr2.clone(), false))));

        // fake connected
        service.on_input(&ctx, 110, neighbour_event(neighbours::Event::Connected(addr2.node_id(), ConnId::from_out(0, 0))));

        // remove node
        service.on_shared_input(&ctx, 200, ServiceSharedInput::Tick(0));
        assert_eq!(service.pop_output2(200), None);

        // fake removed key
        service.on_input(&ctx, 300, map_event(connect_map, MapEvent::OnDel(Key(1), addr2.node_id())));
        assert_eq!(service.pop_output2(300), None);

        // wait disconnect
        service.on_shared_input(&ctx, 300 + WAIT_DISCONNECT_MS, ServiceSharedInput::Tick(0));
        assert_eq!(service.pop_output2(300 + WAIT_DISCONNECT_MS), Some(neighbour_cmd(neighbours::Control::DisconnectFrom(addr2.node_id()))));
        assert_eq!(service.pop_output2(300 + WAIT_DISCONNECT_MS), None);
    }

    #[test]
    fn should_reconnect_after_disconnected() {
        let addr1 = node_addr(100);
        let addr2 = node_addr(101);

        let ctx = ServiceCtx { node_id: 100, session: 0 };
        let mut service = ManualDiscoveryService::<(), (), (), (), ()>::new(addr1.clone(), vec!["local".into()], vec!["connect".into()]);
        let local_map = Map(hash_str("local"));
        let connect_map = Map(hash_str("connect"));

        assert_eq!(service.pop_output2(0), Some(map_cmd(local_map, MapControl::Set(Key(0), addr1.to_vec()))));
        assert_eq!(service.pop_output2(0), Some(map_cmd(connect_map, MapControl::Sub)));

        service.on_input(&ctx, 100, map_event(connect_map, MapEvent::OnSet(Key(1), 2, addr2.to_vec())));
        assert_eq!(service.pop_output2(100), Some(neighbour_cmd(neighbours::Control::ConnectTo(addr2.clone(), false))));

        service.on_shared_input(&ctx, 200, ServiceSharedInput::Tick(0));
        assert_eq!(service.pop_output2(200), None);

        service.on_input(&ctx, 300, neighbour_event(neighbours::Event::Disconnected(addr2.node_id(), ConnId::from_out(0, 0))));

        service.on_shared_input(&ctx, 300, ServiceSharedInput::Tick(0));
        assert_eq!(service.pop_output2(300), None);

        service.on_shared_input(&ctx, RETRY_CONNECT_MS, ServiceSharedInput::Tick(0));
        assert_eq!(service.pop_output2(RETRY_CONNECT_MS), Some(neighbour_cmd(neighbours::Control::ConnectTo(addr2.clone(), false))));
        assert_eq!(service.pop_output2(RETRY_CONNECT_MS), None);
    }
}
