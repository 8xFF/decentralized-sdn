use std::sync::Arc;

use atm0s_sdn_identity::ConnId;
use atm0s_sdn_network::{
    features::{neighbours, FeaturesControl, FeaturesEvent},
    services::manual_discovery::ManualDiscoveryServiceBuilder,
    ExtIn, ExtOut,
};

use crate::simulator::{build_addr, NetworkSimulator, TestNode};

mod simulator;

#[test]
fn service_manual_discovery_three_nodes() {
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(
        node1,
        1234,
        vec![Arc::new(ManualDiscoveryServiceBuilder::new(build_addr(node1), vec![], vec!["demo".to_string()]))],
    ));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(ManualDiscoveryServiceBuilder::new(build_addr(node2), vec![], vec![]))]));
    let addr3 = sim.add_node(TestNode::new(
        node3,
        1236,
        vec![Arc::new(ManualDiscoveryServiceBuilder::new(build_addr(node3), vec!["demo".to_string()], vec![]))],
    ));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr3, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node2, ConnId::from_out(0, 1000))))
        ))
    );
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node3, ConnId::from_out(0, 1005))))
        ))
    );
}
