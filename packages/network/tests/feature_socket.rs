use atm0s_sdn_network::{
    features::{neighbours, socket, FeaturesControl, FeaturesEvent},
    ExtIn, ExtOut,
};

use crate::simulator::{NetworkSimulator, TestNode};

mod simulator;

#[test]
fn feature_socket_single_node() {
    let node1 = 1;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10000))));
    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10001))));
    sim.control(
        node1,
        ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::SendTo(10001, node1, 10000, vec![1, 2, 3, 4].into(), 0))),
    );
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::FeaturesEvent((), FeaturesEvent::Socket(socket::Event::RecvFrom(10000, node1, 10001, vec![1, 2, 3, 4].into(), 0)))
        ))
    );
}

#[test]
fn feature_socket_two_nodes() {
    let node1 = 1;
    let node2 = 2;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10000))));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10001))));
    sim.process(10);
    sim.control(
        node2,
        ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::SendTo(10001, node1, 10000, vec![1, 2, 3, 4].into(), 0))),
    );
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::FeaturesEvent((), FeaturesEvent::Socket(socket::Event::RecvFrom(10000, node2, 10001, vec![1, 2, 3, 4].into(), 0)))
        ))
    );
}

#[test]
fn feature_socket_three_nodes() {
    // node1 <-> node2 <-> node3
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![]));
    let addr3 = sim.add_node(TestNode::new(node3, 1233, vec![]));

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr3, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10000))));
    sim.control(node3, ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::Bind(10001))));
    sim.process(10);
    sim.control(
        node3,
        ExtIn::FeaturesControl((), FeaturesControl::Socket(socket::Control::SendTo(10001, node1, 10000, vec![1, 2, 3, 4].into(), 0))),
    );
    sim.process(10);
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::FeaturesEvent((), FeaturesEvent::Socket(socket::Event::RecvFrom(10000, node3, 10001, vec![1, 2, 3, 4].into(), 0)))
        ))
    );
}
