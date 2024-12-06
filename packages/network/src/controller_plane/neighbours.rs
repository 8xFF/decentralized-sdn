use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use atm0s_sdn_identity::{ConnId, NodeAddr, NodeId, Protocol};
use sans_io_runtime::TaskSwitcherChild;

use crate::{
    base::{self, Authorization, ConnectionCtx, HandshakeBuilder, NeighboursControl, NeighboursControlCmds, SecureContext},
    data_plane::NetPair,
};

use self::connection::{ConnectionEvent, NeighbourConnection};

mod connection;

pub enum Input {
    ConnectTo(NodeAddr),
    DisconnectFrom(NodeId),
    Control(NetPair, NeighboursControl),
}

pub enum Output {
    Control(NetPair, NeighboursControl),
    Event(base::ConnectionEvent),
    OnResourceEmpty,
}

pub struct NeighboursManager {
    node_id: NodeId,
    bind_addrs: Vec<SocketAddr>,
    connections: HashMap<NetPair, NeighbourConnection>,
    neighbours: HashMap<ConnId, ConnectionCtx>,
    queue: VecDeque<Output>,
    shutdown: bool,
    authorization: Arc<dyn Authorization>,
    handshake_builder: Arc<dyn HandshakeBuilder>,
    random: Box<dyn rand::RngCore>,
}

impl NeighboursManager {
    pub fn new(node_id: NodeId, bind_addrs: Vec<SocketAddr>, authorization: Arc<dyn Authorization>, handshake_builder: Arc<dyn HandshakeBuilder>, random: Box<dyn rand::RngCore>) -> Self {
        Self {
            node_id,
            bind_addrs,
            connections: HashMap::new(),
            neighbours: HashMap::new(),
            queue: VecDeque::new(),
            shutdown: false,
            authorization,
            handshake_builder,
            random,
        }
    }

    pub fn conn(&self, conn: ConnId) -> Option<&ConnectionCtx> {
        self.neighbours.get(&conn)
    }

    pub fn on_tick(&mut self, now_ms: u64, _tick_count: u64) {
        for conn in self.connections.values_mut() {
            conn.on_tick(now_ms);
        }
    }

    pub fn on_input(&mut self, now_ms: u64, input: Input) {
        match input {
            Input::ConnectTo(addr) => {
                if addr.node_id() == self.node_id {
                    log::warn!("[Neighbours] Attempt to connect to self");
                    return;
                }
                let dest_node = addr.node_id();
                let dests = get_node_addr_dests(addr);
                for local in &self.bind_addrs {
                    for remote in &dests {
                        if local.is_ipv4() != remote.is_ipv4() {
                            continue;
                        }

                        let pair = NetPair::new(*local, *remote);
                        if self.connections.contains_key(&pair) {
                            continue;
                        }
                        log::info!("[Neighbours] Sending connect request from {local} to {remote}, dest_node {dest_node}");
                        let session_id = self.random.next_u64();
                        let conn = NeighbourConnection::new_outgoing(self.handshake_builder.clone(), self.node_id, dest_node, session_id, pair, now_ms);
                        self.queue.push_back(Output::Event(base::ConnectionEvent::Connecting(conn.ctx())));
                        self.connections.insert(pair, conn);
                    }
                }
            }
            Input::DisconnectFrom(node) => {
                for conn in self.connections.values_mut() {
                    if conn.dest_node() == node {
                        conn.disconnect(now_ms);
                    }
                }
            }
            Input::Control(addr, control) => {
                let cmd: NeighboursControlCmds = match control.validate(now_ms, &*self.authorization) {
                    Ok(cmd) => cmd,
                    Err(_) => {
                        log::warn!("[Neighbours] Invalid control from {:?}", addr);
                        return;
                    }
                };

                log::debug!("[NeighboursManager] received Control(addr: {:?}, cmd: {:?})", addr, cmd);
                if let Some(conn) = self.connections.get_mut(&addr) {
                    conn.on_input(now_ms, control.from, cmd);
                } else {
                    match cmd {
                        NeighboursControlCmds::ConnectRequest { session, .. } => {
                            let mut conn = NeighbourConnection::new_incoming(self.handshake_builder.clone(), self.node_id, control.from, session, addr, now_ms);
                            conn.on_input(now_ms, control.from, cmd);
                            self.queue.push_back(Output::Event(base::ConnectionEvent::Connecting(conn.ctx())));
                            self.connections.insert(addr, conn);
                        }
                        _ => {
                            log::warn!("[Neighbours] Neighbour connection not found for control {:?}", control);
                        }
                    }
                }
            }
        }
    }

    pub fn on_shutdown(&mut self, now_ms: u64) {
        if self.shutdown {
            return;
        }
        self.shutdown = true;
        for conn in self.connections.values_mut() {
            conn.disconnect(now_ms);
        }
    }
}

impl TaskSwitcherChild<Output> for NeighboursManager {
    type Time = u64;

    fn empty_event(&self) -> Output {
        Output::OnResourceEmpty
    }

    fn is_empty(&self) -> bool {
        self.shutdown && self.connections.is_empty() && self.queue.is_empty()
    }

    fn pop_output(&mut self, _now: u64) -> Option<Output> {
        if let Some(output) = self.queue.pop_front() {
            return Some(output);
        }

        let mut to_remove = Vec::new();
        for (remote, conn) in self.connections.iter_mut() {
            while let Some(output) = conn.pop_output() {
                match output {
                    connection::Output::Event(event) => {
                        let event = match event {
                            ConnectionEvent::Connected(encryptor, decryptor) => {
                                let ctx = conn.ctx();
                                self.neighbours.insert(ctx.conn, ctx.clone());
                                Some(base::ConnectionEvent::Connected(ctx, SecureContext { encryptor, decryptor }))
                            }
                            ConnectionEvent::ConnectError(err) => {
                                to_remove.push(*remote);
                                Some(base::ConnectionEvent::ConnectError(conn.ctx(), err))
                            }
                            ConnectionEvent::Stats(stats) => {
                                let ctx = conn.ctx();
                                Some(base::ConnectionEvent::Stats(ctx, stats))
                            }
                            ConnectionEvent::Disconnected => {
                                let ctx = conn.ctx();
                                self.neighbours.remove(&ctx.conn);
                                to_remove.push(*remote);
                                Some(base::ConnectionEvent::Disconnected(ctx))
                            }
                        };
                        if let Some(event) = event {
                            self.queue.push_back(Output::Event(event));
                        }
                    }
                    connection::Output::Net(now_ms, remote, cmd) => {
                        log::debug!("[NeighboursManager] pop_output Net(remote: {:?}, cmd: {:?})", remote, cmd);
                        self.queue.push_back(Output::Control(remote, NeighboursControl::build(now_ms, self.node_id, cmd, &*self.authorization)));
                    }
                }
            }
        }

        for remote in to_remove {
            self.connections.remove(&remote);
        }

        self.queue.pop_front()
    }
}

fn get_node_addr_dests(addr: NodeAddr) -> Vec<SocketAddr> {
    let mut dests = Vec::new();
    log::info!("Connect to: addr {}", addr);
    let mut dest_ip = None;
    for part in addr.multiaddr().iter() {
        match part {
            Protocol::Ip4(i) => {
                dest_ip = Some(IpAddr::V4(i));
            }
            Protocol::Ip6(i) => {
                dest_ip = Some(IpAddr::V6(i));
            }
            Protocol::Udp(port) => {
                if let Some(ip) = dest_ip {
                    dests.push(SocketAddr::new(ip, port));
                }
            }
            _ => {}
        }
    }
    dests
}
