use std::sync::Arc;

use atm0s_sdn_identity::{ConnId, NodeId};
use atm0s_sdn_network::{
    features::{neighbours, FeaturesControl},
    services::visualization::{self, ConnectionInfo, Control, Event, VisualizationServiceBuilder},
    ExtIn, ExtOut,
};
use serde::{Deserialize, Serialize};

use crate::simulator::{node_to_addr, NetworkSimulator, TestNode};

mod simulator;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct NodeInfo(u8);

fn node_changed(node: NodeId, info: NodeInfo, remotes: &[(NodeId, ConnId)]) -> ExtOut<(), Event<NodeInfo>> {
    ExtOut::ServicesEvent(
        visualization::SERVICE_ID.into(),
        (),
        Event::NodeChanged(
            node,
            info,
            remotes
                .iter()
                .map(|(n, c)| ConnectionInfo {
                    conn: *c,
                    dest: *n,
                    local: node_to_addr(node),
                    remote: node_to_addr(*n),
                    rtt_ms: 0,
                })
                .collect(),
        ),
    )
}

#[test]
fn service_visualization_simple() {
    let node1 = 1;
    let node2 = 2;
    let node1_info = NodeInfo(1);
    let node2_info = NodeInfo(2);
    let mut sim = NetworkSimulator::<Control<NodeInfo>, Event<NodeInfo>, (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(VisualizationServiceBuilder::new(node1_info.clone(), true))]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(VisualizationServiceBuilder::new(node2_info.clone(), false))]));

    sim.control(node1, ExtIn::ServicesControl(visualization::SERVICE_ID.into(), (), Control::Subscribe));

    // For sync, snapshot and subscribe process
    for _i in 0..6 {
        sim.process(1000);
    }

    assert_eq!(sim.pop_res(), Some((node1, ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::GotAll(vec![])))));
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::NodeChanged(node1, node1_info.clone(), vec![]))
        ))
    );

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));

    // For sync route table and snapshot process
    for _i in 0..5 {
        sim.process(1000);
    }

    assert_eq!(sim.pop_res(), Some((node1, node_changed(node1, node1_info, &[(node2, ConnId::from_out(0, 1000))]))));

    assert_eq!(sim.pop_res(), Some((node1, node_changed(node2, node2_info, &[(node1, ConnId::from_in(0, 1000))]))));
}

/// 3 nodes: Master <--> Master <--> Agent
///
/// All collectors should receive the same information about the agents
///
#[test]
fn service_visualization_multi_collectors() {
    let node1 = 1;
    let node2 = 2;
    let node3 = 3;
    let node1_info = NodeInfo(1);
    let node2_info = NodeInfo(2);
    let node3_info = NodeInfo(3);
    let mut sim = NetworkSimulator::<Control<NodeInfo>, Event<NodeInfo>, (), ()>::new(0);

    let _addr1 = sim.add_node(TestNode::new(node1, 1234, vec![Arc::new(VisualizationServiceBuilder::new(node1_info.clone(), true))]));
    let addr2 = sim.add_node(TestNode::new(node2, 1235, vec![Arc::new(VisualizationServiceBuilder::new(node2_info.clone(), true))]));
    let addr3 = sim.add_node(TestNode::new(node3, 1236, vec![Arc::new(VisualizationServiceBuilder::new(node3_info.clone(), false))]));

    sim.control(node1, ExtIn::ServicesControl(visualization::SERVICE_ID.into(), (), Control::Subscribe));
    sim.control(node2, ExtIn::ServicesControl(visualization::SERVICE_ID.into(), (), Control::Subscribe));

    // For sync, snapshot and subscribe process
    for _i in 0..6 {
        sim.process(1000);
    }

    assert_eq!(sim.pop_res(), Some((node1, ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::GotAll(vec![])))));
    assert_eq!(sim.pop_res(), Some((node2, ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::GotAll(vec![])))));
    assert_eq!(
        sim.pop_res(),
        Some((
            node1,
            ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::NodeChanged(node1, node1_info.clone(), vec![]))
        ))
    );
    assert_eq!(
        sim.pop_res(),
        Some((
            node2,
            ExtOut::ServicesEvent(visualization::SERVICE_ID.into(), (), Event::NodeChanged(node2, node2_info.clone(), vec![]))
        ))
    );

    sim.control(node1, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr2, false))));
    sim.control(node2, ExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(addr3, false))));

    // For sync route table and snapshot process
    for _i in 0..5 {
        sim.process(1000);
    }

    let mut node1_events = vec![];
    let mut node2_events = vec![];

    while let Some((node, e)) = sim.pop_res() {
        if node == node1 {
            node1_events.push(e);
        } else if node == node2 {
            node2_events.push(e);
        } else {
            panic!("Unexpected node: {}", node);
        }
    }

    let get_key = |a: &ExtOut<(), Event<NodeInfo>>| -> u32 {
        match a {
            ExtOut::ServicesEvent(_service, _, Event::NodeChanged(node, _, _)) => *node,
            _ => panic!("Unexpected event: {:?}", a),
        }
    };

    node1_events.sort_by_key(get_key);
    node2_events.sort_by_key(get_key);

    assert_eq!(
        node1_events,
        vec![
            node_changed(node1, node1_info.clone(), &[(node2, ConnId::from_out(0, 1000))]),
            node_changed(node2, node2_info.clone(), &[(node1, ConnId::from_in(0, 1000)), (node3, ConnId::from_out(0, 2000))]),
            node_changed(node3, node3_info.clone(), &[(node2, ConnId::from_in(0, 2000))]),
        ]
    );

    assert_eq!(
        node2_events,
        vec![
            node_changed(node1, node1_info, &[(node2, ConnId::from_out(0, 1000))]),
            node_changed(node2, node2_info, &[(node1, ConnId::from_in(0, 1000)), (node3, ConnId::from_out(0, 2000))]),
            node_changed(node3, node3_info, &[(node2, ConnId::from_in(0, 2000))]),
        ]
    );
}
