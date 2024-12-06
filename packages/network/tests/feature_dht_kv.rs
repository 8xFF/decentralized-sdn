use atm0s_sdn_network::{
    features::{
        dht_kv::{Control, Event, Key, Map, MapControl, MapEvent},
        neighbours, FeaturesControl, FeaturesEvent,
    },
    ExtIn, ExtOut,
};

use crate::simulator::{NetworkSimulator, TestNode};

mod simulator;

fn control(control: Control) -> ExtIn<(), ()> {
    ExtIn::FeaturesControl((), FeaturesControl::DhtKv(control))
}

fn event(event: Event) -> ExtOut<(), ()> {
    ExtOut::FeaturesEvent((), FeaturesEvent::DhtKv(event))
}

#[test]
fn feature_dht_kv_single_node() {
    let node_id = 1;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);
    sim.add_node(TestNode::new(node_id, 1234, vec![]));

    sim.process(100);

    let key = Map(1000);
    let sub_key = Key(2000);
    let value = vec![1, 2, 3, 4];

    sim.control(node_id, control(Control::MapCmd(key, MapControl::Sub)));
    sim.process(100);
    assert_eq!(sim.pop_res(), Some((node_id, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node_id))))));

    sim.control(node_id, control(Control::MapCmd(key, MapControl::Set(sub_key, value.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node_id, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node_id, value))))));
    assert_eq!(sim.pop_res(), None);
}

#[test]
fn feature_dht_kv_single_node_sub_after() {
    let node_id = 1;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);
    sim.add_node(TestNode::new(node_id, 1234, vec![]));

    sim.process(100);

    let key = Map(1000);
    let sub_key = Key(2000);
    let value = vec![1, 2, 3, 4];

    sim.control(node_id, control(Control::MapCmd(key, MapControl::Set(sub_key, value.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), None);

    sim.control(node_id, control(Control::MapCmd(key, MapControl::Sub)));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node_id, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node_id, value))))));
    assert_eq!(sim.pop_res(), Some((node_id, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node_id))))));
    assert_eq!(sim.pop_res(), None);
}

#[test]
fn feature_dht_kv_two_nodes() {
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

    let key = Map(1);
    let sub_key = Key(2000);
    let value = vec![1, 2, 3, 4];

    sim.control(node1, control(Control::MapCmd(key, MapControl::Sub)));
    sim.process(100);
    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node1))))));

    sim.control(node2, control(Control::MapCmd(key, MapControl::Set(sub_key, value.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node2, value))))));
    assert_eq!(sim.pop_res(), None);
}

#[test]
fn feature_dht_kv_two_nodes_sub_after() {
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

    let key = Map(1);
    let sub_key = Key(2000);
    let value = vec![1, 2, 3, 4];

    sim.control(node2, control(Control::MapCmd(key, MapControl::Set(sub_key, value.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), None);

    sim.control(node1, control(Control::MapCmd(key, MapControl::Sub)));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node1))))));
    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node2, value))))));
    assert_eq!(sim.pop_res(), None);
}

#[test]
fn feature_dht_kv_move_key_other_relay() {
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let mut sim = NetworkSimulator::<(), (), (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![]));
    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync
    for _i in 0..4 {
        sim.process(500);
    }

    let key = Map(3);
    let sub_key = Key(2000);
    let value = vec![1, 2, 3, 4];

    sim.control(node1, control(Control::MapCmd(key, MapControl::Sub)));
    sim.process(100);
    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node2))))));
    assert_eq!(sim.pop_res(), None);

    sim.control(node2, control(Control::MapCmd(key, MapControl::Set(sub_key, value.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node2, value))))));
    assert_eq!(sim.pop_res(), None);

    log::info!("add new node3 => data should move to node3");
    let addr3 = sim.add_node(TestNode::new(node3, 1235, vec![]));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr3, false))));

    // For sync table
    for _i in 0..4 {
        sim.process(500);
    }

    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnRelaySelected(node3))))));
    assert_eq!(sim.pop_res(), None);

    // Now set new value should be relay to node3
    let value2 = vec![1, 2, 3, 4, 5];
    sim.control(node2, control(Control::MapCmd(key, MapControl::Set(sub_key, value2.clone()))));
    sim.process(100);

    assert_eq!(sim.pop_res(), Some((node1, event(Event::MapEvent(key, MapEvent::OnSet(sub_key, node2, value2))))));
    assert_eq!(sim.pop_res(), None);
}
