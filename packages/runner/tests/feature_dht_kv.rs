use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use atm0s_sdn::{
    features::{
        dht_kv::{self, MapControl, MapEvent},
        neighbours, FeaturesControl, FeaturesEvent,
    },
    secure::StaticKeyAuthorization,
    services::visualization,
    NodeAddr, NodeId, SdnBuilder, SdnController, SdnControllerUtils, SdnExtOut, SdnOwner,
};
use sans_io_runtime::backend::PollingBackend;

type UserInfo = u32;
type SC = visualization::Control<UserInfo>;
type SE = visualization::Event<UserInfo>;
type TC = ();
type TW = ();

fn process(nodes: &mut [&mut SdnController<(), SC, SE, TC, TW>], timeout_ms: u64) {
    let mut count = 0;
    while count < timeout_ms / 10 {
        std::thread::sleep(Duration::from_millis(10));
        count += 1;
        for node in nodes.iter_mut() {
            if node.process().is_none() {
                panic!("Node is shutdown");
            }
        }
    }
}

fn expect_event(node: &mut SdnController<(), SC, SE, TC, TW>, expected: dht_kv::Event) {
    match node.pop_event() {
        Some(SdnExtOut::FeaturesEvent((), FeaturesEvent::DhtKv(event))) => {
            assert_eq!(event, expected)
        }
        Some(event) => {
            panic!("Unexpected event: {:?}", event)
        }
        None => {
            panic!("No event")
        }
    }
}

fn build_node(node_id: NodeId, udp_port: u16) -> (SdnController<(), SC, SE, TC, TW>, NodeAddr) {
    let addrs = [
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, udp_port)),
        SocketAddr::new(local_ip_address::local_ip().expect("Should have ip-v4"), udp_port),
    ];
    let mut builder = SdnBuilder::<(), SC, SE, TC, TW, UserInfo>::new(node_id, &addrs, vec![]);
    builder.set_authorization(StaticKeyAuthorization::new("password-here"));
    let node_addr = builder.node_addr();
    let node_info = node_id;
    let node = builder.build::<PollingBackend<SdnOwner, 16, 16>>(2, node_info);
    (node, node_addr)
}

#[test]
fn test_single_node() {
    let (mut node, _node_addr) = build_node(1, 10000);
    std::thread::sleep(Duration::from_millis(100));
    node.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Sub)));
    process(&mut [&mut node], 100);
    expect_event(&mut node, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnRelaySelected(1)));

    node.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Set(2000.into(), vec![1, 2, 3]))));
    process(&mut [&mut node], 100);
    expect_event(&mut node, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnSet(2000.into(), 1, vec![1, 2, 3])));
}

#[test]
fn test_two_nodes() {
    let node1_id = 1;
    let node2_id = 2;
    let (mut node1, node_addr1) = build_node(node1_id, 11000);
    let (mut node2, _node_addr2) = build_node(node2_id, 11001);

    node2.feature_control((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(node_addr1, false)));

    process(&mut [&mut node1, &mut node2], 100);
    log::info!("sending map cmd Sub");
    node1.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Sub)));
    process(&mut [&mut node1, &mut node2], 100);
    expect_event(&mut node1, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnRelaySelected(node1_id)));

    node2.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Set(2000.into(), vec![1, 2, 3]))));
    process(&mut [&mut node1, &mut node2], 100);

    expect_event(&mut node1, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnSet(2000.into(), node2_id, vec![1, 2, 3])));
}

#[test]
fn test_three_nodes() {
    let node1_id = 1;
    let node2_id = 2;
    let node3_id = 3;
    let (mut node1, node_addr1) = build_node(node1_id, 12000);
    let (mut node2, node_addr2) = build_node(node2_id, 12001);
    let (mut node3, _node_addr3) = build_node(node3_id, 12002);

    node2.feature_control((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(node_addr1, false)));
    node3.feature_control((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(node_addr2, false)));

    process(&mut [&mut node1, &mut node2, &mut node3], 100);
    log::info!("sending map cmd Sub");
    node2.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Sub)));
    process(&mut [&mut node1, &mut node2, &mut node3], 100);

    expect_event(&mut node2, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnRelaySelected(node1_id)));

    node3.feature_control((), FeaturesControl::DhtKv(dht_kv::Control::MapCmd(1000.into(), MapControl::Set(2000.into(), vec![1, 2, 3]))));
    process(&mut [&mut node1, &mut node2, &mut node3], 100);

    expect_event(&mut node2, dht_kv::Event::MapEvent(1000.into(), MapEvent::OnSet(2000.into(), node3_id, vec![1, 2, 3])));
}
