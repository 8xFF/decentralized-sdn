//!
//! This is common mode for testing perpose
//! We will create a node with a controller and single worker, which is enough for testing
//!

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::{collections::VecDeque, net::IpAddr};

use atm0s_sdn_identity::{NodeAddr, NodeAddrBuilder, NodeId, Protocol};
use atm0s_sdn_network::{
    base::{GenericBuffer, GenericBufferMut},
    controller_plane::{self, ControllerPlane},
    data_plane::{self, DataPlane},
    ExtIn, ExtOut,
};
use log::{LevelFilter, Metadata, Record};
use parking_lot::Mutex;

static CONTEXT_LOGGER: ContextLogger = ContextLogger { node: Mutex::new(None) };

struct ContextLogger {
    node: Mutex<Option<NodeId>>,
}

impl ContextLogger {
    pub fn set_ctx(&self, node: NodeId) {
        *self.node.lock() = Some(node);
    }

    pub fn clear_ctx(&self) {
        *self.node.lock() = None;
    }
}

impl log::Log for ContextLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Some(node) = self.node.lock().as_ref() {
                println!("[Node {}] {} - {}", node, record.level(), record.args());
            } else {
                println!("[------] {} - {}", record.level(), record.args());
            }
        }
    }
    fn flush(&self) {}
}

struct AutoContext(NodeId);
impl AutoContext {
    pub fn new(node: NodeId) -> Self {
        CONTEXT_LOGGER.set_ctx(node);
        Self(node)
    }
}

impl Drop for AutoContext {
    fn drop(&mut self) {
        CONTEXT_LOGGER.clear_ctx();
    }
}

pub enum TestNodeIn<'a> {
    Ext(ExtIn),
    Udp(SocketAddr, GenericBuffer<'a>),
    #[allow(unused)]
    Tun(GenericBufferMut<'a>),
}

pub enum TestNodeOut<'a> {
    Ext(ExtOut),
    Udp(Vec<SocketAddr>, GenericBuffer<'a>),
    Tun(GenericBuffer<'a>),
}

pub struct TestNode<TC, TW> {
    node_id: NodeId,
    controller: ControllerPlane<TC, TW>,
    worker: DataPlane<TC, TW>,
}

impl<TC, TW: Clone> TestNode<TC, TW> {
    pub fn new(node_id: NodeId, session: u64) -> Self {
        let _log = AutoContext::new(node_id);
        let controller = ControllerPlane::new(node_id, session);
        let worker = DataPlane::new(node_id);
        Self { node_id, controller, worker }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn addr(&self) -> NodeAddr {
        let mut builder = NodeAddrBuilder::new(self.node_id);
        builder.add_protocol(Protocol::Ip4(Ipv4Addr::LOCALHOST));
        builder.add_protocol(Protocol::Udp(self.node_id as u16));
        builder.addr()
    }

    pub fn tick(&mut self, now: u64) {
        let _log = AutoContext::new(self.node_id);
        self.controller.on_tick(now);
        self.worker.on_tick(now);
    }

    pub fn on_input<'a>(&mut self, now: u64, input: TestNodeIn<'a>) -> Option<TestNodeOut<'a>> {
        let _log = AutoContext::new(self.node_id);
        match input {
            TestNodeIn::Ext(ext_in) => {
                self.controller.on_event(now, controller_plane::Input::Ext(ext_in));
                let out = self.controller.pop_output(now)?;
                self.process_controller_output(now, out)
            }
            TestNodeIn::Udp(addr, buf) => {
                let out = self.worker.on_event(now, data_plane::Input::Net(data_plane::NetInput::UdpPacket(addr, buf)))?;
                self.process_worker_output(now, out)
            }
            TestNodeIn::Tun(buf) => {
                let out = self.worker.on_event(now, data_plane::Input::Net(data_plane::NetInput::TunPacket(buf)))?;
                self.process_worker_output(now, out)
            }
        }
    }

    pub fn pop_output<'a>(&mut self, now: u64) -> Option<TestNodeOut<'a>> {
        let _log = AutoContext::new(self.node_id);
        let mut keep_running = true;
        while keep_running {
            keep_running = false;

            if let Some(output) = self.controller.pop_output(now) {
                keep_running = true;
                if let Some(out) = self.process_controller_output(now, output) {
                    return Some(out);
                }
            }

            if let Some(output) = self.worker.pop_output(now) {
                keep_running = true;
                if let Some(out) = self.process_worker_output(now, output) {
                    return Some(out);
                }
            }
        }
        None
    }

    fn process_controller_output<'a>(&mut self, now: u64, output: controller_plane::Output<TW>) -> Option<TestNodeOut<'a>> {
        match output {
            controller_plane::Output::Event(e) => {
                let output = self.worker.on_event(now, data_plane::Input::Event(e))?;
                self.process_worker_output(now, output)
            }
            controller_plane::Output::Ext(out) => Some(TestNodeOut::Ext(out)),
            controller_plane::Output::ShutdownSuccess => None,
        }
    }

    fn process_worker_output<'a>(&mut self, now: u64, output: data_plane::Output<'a, TC>) -> Option<TestNodeOut<'a>> {
        match output {
            data_plane::Output::Ext(out) => Some(TestNodeOut::Ext(out)),
            data_plane::Output::Control(control) => {
                self.controller.on_event(now, controller_plane::Input::Control(control));
                let output = self.controller.pop_output(now)?;
                self.process_controller_output(now, output)
            }
            data_plane::Output::Net(out) => match out {
                data_plane::NetOutput::UdpPacket(dest, buf) => Some(TestNodeOut::Udp(vec![dest], buf)),
                data_plane::NetOutput::UdpPackets(dest, buf) => Some(TestNodeOut::Udp(dest, buf)),
                data_plane::NetOutput::TunPacket(buf) => Some(TestNodeOut::Tun(buf)),
            },
            data_plane::Output::ShutdownResponse => None,
            data_plane::Output::Continue => None,
        }
    }
}

fn addr_to_node(addr: SocketAddr) -> NodeId {
    addr.port() as u32
}

fn node_to_addr(node: NodeId) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), node as u16)
}

pub struct NetworkSimulator<TC: Clone, TW: Clone> {
    clock_ms: u64,
    intput: VecDeque<(NodeId, ExtIn)>,
    output: VecDeque<(NodeId, ExtOut)>,
    nodes: Vec<TestNode<TC, TW>>,
    nodes_index: HashMap<NodeId, usize>,
}

impl<TC: Clone, TW: Clone> NetworkSimulator<TC, TW> {
    pub fn new(started_ms: u64) -> Self {
        Self {
            clock_ms: started_ms,
            intput: VecDeque::new(),
            output: VecDeque::new(),
            nodes: Vec::new(),
            nodes_index: HashMap::new(),
        }
    }

    #[allow(unused)]
    pub fn enable_log(&self, level: LevelFilter) {
        log::set_logger(&CONTEXT_LOGGER).expect("Should set global logger");
        log::set_max_level(level);
    }

    pub fn control(&mut self, node: NodeId, control: ExtIn) {
        self.intput.push_back((node, control));
    }

    pub fn pop_res(&mut self) -> Option<(NodeId, ExtOut)> {
        self.output.pop_front()
    }

    pub fn add_node(&mut self, node: TestNode<TC, TW>) -> NodeAddr {
        let index = self.nodes.len();
        self.nodes_index.insert(node.node_id(), index);
        let addr = node.addr();
        self.nodes.push(node);
        addr
    }

    pub fn process(&mut self, delta: u64) {
        self.clock_ms += delta;
        log::debug!("Tick {} ms", self.clock_ms);
        for node in self.nodes.iter_mut() {
            node.tick(self.clock_ms);
        }

        self.pop_outputs();

        if !self.intput.is_empty() {
            while let Some((node, input)) = self.intput.pop_front() {
                self.process_input(node, TestNodeIn::Ext(input));
            }

            self.pop_outputs();
        }
    }

    fn process_input<'a>(&mut self, node: NodeId, input: TestNodeIn<'a>) -> Option<()> {
        let index = self.nodes_index.get(&node).expect("Node not found");
        let output = self.nodes[*index].on_input(self.clock_ms, input)?;
        match output {
            TestNodeOut::Ext(out) => {
                self.output.push_back((node, out));
                Some(())
            }
            TestNodeOut::Udp(dests, data) => {
                let source_addr = node_to_addr(node);
                for dest in dests {
                    let dest_node = addr_to_node(dest);
                    self.process_input(dest_node, TestNodeIn::Udp(source_addr, data.clone()));
                }
                Some(())
            }
            TestNodeOut::Tun(_) => todo!(),
        }
    }

    fn pop_outputs(&mut self) {
        let mut keep_running = true;
        while keep_running {
            keep_running = false;
            for index in 0..self.nodes.len() {
                let node = self.nodes[index].node_id();
                if self.pop_output(node).is_some() {
                    keep_running = true;
                }
            }
        }
    }

    fn pop_output<'a>(&mut self, node: NodeId) -> Option<()> {
        let index = self.nodes_index.get(&node).expect("Node not found");
        let output = self.nodes[*index].pop_output(self.clock_ms)?;
        match output {
            TestNodeOut::Ext(out) => {
                self.output.push_back((node, out));
                Some(())
            }
            TestNodeOut::Udp(dests, data) => {
                let source_addr = node_to_addr(node);
                for dest in dests {
                    log::debug!("Send UDP packet from {} to {}, buf len {}", source_addr, dest, data.len());
                    let dest_node = addr_to_node(dest);
                    self.process_input(dest_node, TestNodeIn::Udp(source_addr, data.clone()));
                }
                Some(())
            }
            TestNodeOut::Tun(_) => todo!(),
        }
    }
}