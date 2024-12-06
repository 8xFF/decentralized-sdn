use std::sync::Arc;

use atm0s_sdn_identity::ConnId;
use atm0s_sdn_network::{
    features::{neighbours, FeaturesControl, FeaturesEvent},
    services::manual2_discovery::{AdvertiseTarget, Manual2DiscoveryServiceBuilder},
    ExtIn, ExtOut,
};
use atm0s_sdn_router::ServiceBroadcastLevel;

use crate::simulator::{build_addr, NetworkSimulator, TestNode};

mod simulator;

/// We create 3node with init connection: A -- B -- C
/// A advertise it address to global then C should connect to A
#[test]
fn service_manual_discovery2_three_nodes() {
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(
        node1,
        1234,
        vec![Arc::new(Manual2DiscoveryServiceBuilder::new(
            build_addr(node1),
            vec![AdvertiseTarget {
                service: 2.into(),
                level: ServiceBroadcastLevel::Global,
            }],
            100,
        ))],
    ));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(Manual2DiscoveryServiceBuilder::new(build_addr(node2), vec![], 100))]));
    let _addr3 = sim.add_node(TestNode::new(node3, 1236, vec![Arc::new(Manual2DiscoveryServiceBuilder::new(build_addr(node3), vec![], 100))]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2.clone(), false))));
    sim.control(node3, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..10 {
        sim.process(500);
    }

    let mut out = vec![];
    while let Some((node, out_event)) = sim.pop_res() {
        out.push((node, out_event));
    }

    out.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        out,
        vec![
            (
                node1,
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node2, ConnId::from_out(0, 1000))))
            ),
            (
                node1,
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node3, ConnId::from_in(0, 3005))))
            ),
        ]
    );
}
