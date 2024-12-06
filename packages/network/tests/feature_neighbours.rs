use atm0s_sdn_identity::ConnId;
use atm0s_sdn_network::{
    features::{neighbours, FeaturesControl, FeaturesEvent},
    ExtIn, ExtOut,
};

use crate::simulator::{NetworkSimulator, TestNode};

mod simulator;

#[test]
fn feature_neighbours_two_nodes() {
    let node1 = 1;
    let node2 = 2;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..4 {
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
                node2,
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node1, ConnId::from_in(0, 1000))))
            ),
        ]
    );
}

#[test]
fn feature_neighbours_request_seed_node() {
    let node1 = 1;
    let node2 = 2;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::Sub)));

    // connect to node2 as seed node
    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, true))));

    // For sync
    for _i in 0..4 {
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
                node2,
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Connected(node1, ConnId::from_in(0, 1000))))
            ),
        ]
    );

    // simulate node2 disconnect from node1
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::DisconnectFrom(node1))));

    // For sync
    for _i in 0..4 {
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
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Disconnected(node2, ConnId::from_out(0, 1000))))
            ),
            (node1, ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::SeedAddressNeeded))),
            (
                node2,
                ExtOut::FeaturesEvent((), FeaturesEvent::Neighbours(neighbours::Event::Disconnected(node1, ConnId::from_in(0, 1000))))
            ),
        ]
    );
}
