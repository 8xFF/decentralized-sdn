use std::sync::Arc;

use atm0s_sdn_network::{
    base::{Service, ServiceBuilder, ServiceCtx, ServiceInput, ServiceOutput, ServiceSharedInput, ServiceWorker, ServiceWorkerCtx, ServiceWorkerInput, ServiceWorkerOutput},
    features::{
        alias::{self, FoundLocation},
        neighbours, FeaturesControl, FeaturesEvent,
    },
    ExtIn, ExtOut,
};
use atm0s_sdn_router::ServiceBroadcastLevel;

use crate::simulator::{NetworkSimulator, TestNode};

mod simulator;

#[derive(Default)]
struct MockService {
    shutdown: bool,
}

impl Service<(), FeaturesControl, FeaturesEvent, (), (), (), ()> for MockService {
    fn is_service_empty(&self) -> bool {
        self.shutdown
    }

    fn service_id(&self) -> u8 {
        0
    }

    fn service_name(&self) -> &str {
        "mock"
    }

    fn on_input(&mut self, _ctx: &ServiceCtx, _now: u64, _input: ServiceInput<(), FeaturesEvent, (), ()>) {}

    fn on_shared_input<'a>(&mut self, _ctx: &ServiceCtx, _now: u64, _input: ServiceSharedInput) {}

    fn on_shutdown(&mut self, _ctx: &ServiceCtx, _now: u64) {
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceOutput<(), FeaturesControl, (), ()>> {
        None
    }
}

#[derive(Default)]
struct MockServiceWorker {
    shutdown: bool,
}

impl ServiceWorker<(), FeaturesControl, FeaturesEvent, (), (), (), ()> for MockServiceWorker {
    fn is_service_empty(&self) -> bool {
        self.shutdown
    }

    fn service_id(&self) -> u8 {
        0
    }

    fn service_name(&self) -> &str {
        "mock"
    }

    fn on_tick(&mut self, _ctx: &ServiceWorkerCtx, _now: u64, _tick_count: u64) {}

    fn on_input(&mut self, _ctx: &ServiceWorkerCtx, _now: u64, _input: ServiceWorkerInput<(), FeaturesEvent, (), ()>) {}

    fn on_shutdown(&mut self, _ctx: &ServiceWorkerCtx, _now: u64) {
        self.shutdown = true;
    }

    fn pop_output2(&mut self, _now: u64) -> Option<ServiceWorkerOutput<(), FeaturesControl, FeaturesEvent, (), (), ()>> {
        None
    }
}

struct MockServiceBuilder;

impl ServiceBuilder<(), FeaturesControl, FeaturesEvent, (), (), (), ()> for MockServiceBuilder {
    fn service_id(&self) -> u8 {
        0
    }

    fn service_name(&self) -> &str {
        "mock"
    }

    fn create(&self) -> Box<dyn Service<(), FeaturesControl, FeaturesEvent, (), (), (), ()>> {
        Box::new(MockService::default())
    }

    fn create_worker(&self) -> Box<dyn ServiceWorker<(), FeaturesControl, FeaturesEvent, (), (), (), ()>> {
        Box::new(MockServiceWorker::default())
    }
}

#[test]
fn feature_alias_single_node() {
    let node1 = 1;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(MockServiceBuilder)]));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    let alias = 1000;
    let service = 0;
    let level = ServiceBroadcastLevel::Global;

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Register { alias, service, level })));
    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Query { alias, service, level })));
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((node1, ExtOut::FeaturesEvent((), FeaturesEvent::Alias(alias::Event::QueryResult(alias, Some(FoundLocation::Local))))))
    );
}

#[test]
fn feature_alias_timeout() {
    let node1 = 1;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(MockServiceBuilder)]));

    sim.process(10);

    let alias_v = 1000;
    let service = 0;
    let level = ServiceBroadcastLevel::Global;

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Query { alias: alias_v, service, level })));
    sim.process(10);
    sim.process(alias::HINT_TIMEOUT_MS);
    sim.process(alias::SCAN_TIMEOUT_MS);
    assert_eq!(sim.pop_res(), Some((node1, ExtOut::FeaturesEvent((), FeaturesEvent::Alias(alias::Event::QueryResult(alias_v, None))))));
}

#[test]
fn feature_alias_two_nodes() {
    let node1 = 1;
    let node2 = 2;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);
    sim.enable_log(log::LevelFilter::Debug);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(MockServiceBuilder)]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(MockServiceBuilder)]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    let alias_v = 1000;
    let service = 0;
    let level = ServiceBroadcastLevel::Global;

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Register { alias: alias_v, service, level })));
    sim.process(10);
    sim.process(alias::HINT_TIMEOUT_MS);
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Query { alias: alias_v, service, level })));
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((
            node2,
            ExtOut::FeaturesEvent((), FeaturesEvent::Alias(alias::Event::QueryResult(alias_v, Some(FoundLocation::RemoteHint(node1)))))
        ))
    );
}

#[test]
fn feature_alias_three_nodes() {
    // node1 <-> node2 <-> node3
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(MockServiceBuilder)]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(MockServiceBuilder)]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    let alias = 1000;
    let service = 0;
    let level = ServiceBroadcastLevel::Global;

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Register { alias, service, level })));
    sim.process(10);

    let addr3 = sim.add_node(TestNode::new(node3, 1236, vec![Arc::new(MockServiceBuilder)]));
    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr3, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    sim.control(node3, ExtIn::FeaturesControl((), FeaturesControl::Alias(alias::Control::Query { alias, service, level })));
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((
            node3,
            ExtOut::FeaturesEvent((), FeaturesEvent::Alias(alias::Event::QueryResult(alias, Some(FoundLocation::RemoteScan(node1)))))
        ))
    );
}
