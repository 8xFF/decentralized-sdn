use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    hash::Hash,
};

use atm0s_sdn_identity::{ConnId, NodeAddr, NodeId};
use derivative::Derivative;
use sans_io_runtime::{collections::DynamicDeque, TaskSwitcherChild};

use crate::base::{ConnectionEvent, Feature, FeatureContext, FeatureControlActor, FeatureInput, FeatureOutput, FeatureSharedInput, FeatureWorker, FeatureWorkerInput, FeatureWorkerOutput};

pub const FEATURE_ID: u8 = 0;
pub const FEATURE_NAME: &str = "neighbours_api";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Sub,
    UnSub,
    ConnectTo(NodeAddr, bool),
    DisconnectFrom(NodeId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Connected(NodeId, ConnId),
    Disconnected(NodeId, ConnId),
    SeedAddressNeeded,
}

#[derive(Debug, Clone)]
pub struct ToWorker;

#[derive(Debug, Clone)]
pub struct ToController;

pub type Output<UserData> = FeatureOutput<UserData, Event, ToWorker>;
pub type WorkerOutput<UserData> = FeatureWorkerOutput<UserData, Control, Event, ToController>;

#[derive(Debug, Derivative)]
#[derivative(Default(bound = ""))]
pub struct NeighboursFeature<UserData> {
    subs: Vec<FeatureControlActor<UserData>>,
    output: VecDeque<Output<UserData>>,
    seeds: HashSet<NodeId>,
    nodes: HashMap<NodeId, HashSet<ConnId>>,
    shutdown: bool,
}

impl<UserData: Debug + Copy + Hash + Eq> NeighboursFeature<UserData> {
    fn check_need_more_seeds(&mut self) {
        if self.seeds.is_empty() {
            return;
        }
        for node in self.seeds.iter() {
            if self.nodes.contains_key(node) {
                return;
            }
        }

        log::info!("[Neighbours] All seeds {:?} disconnected or connect error => need update seeds", self.seeds);
        self.seeds.clear();
        for sub in self.subs.iter() {
            self.output.push_back(FeatureOutput::Event(*sub, Event::SeedAddressNeeded));
        }
    }
}

impl<UserData: Debug + Copy + Hash + Eq> Feature<UserData, Control, Event, ToController, ToWorker> for NeighboursFeature<UserData> {
    fn on_shared_input(&mut self, _ctx: &FeatureContext, _now: u64, input: FeatureSharedInput) {
        match input {
            FeatureSharedInput::Connection(ConnectionEvent::Connecting(ctx)) => {
                log::info!("[Neighbours] Node {} connection {} connecting", ctx.node, ctx.pair);
                self.nodes.entry(ctx.node).or_default().insert(ctx.conn);
            }
            FeatureSharedInput::Connection(ConnectionEvent::ConnectError(ctx, _)) => {
                log::info!("[Neighbours] Node {} connection {} connect error", ctx.node, ctx.pair);
                let entry = self.nodes.entry(ctx.node).or_default();
                entry.remove(&ctx.conn);
                if entry.is_empty() {
                    log::info!("[Neighbours] Node {} connect error all connections => remove", ctx.node);
                    self.nodes.remove(&ctx.node);
                }

                self.check_need_more_seeds();
            }
            FeatureSharedInput::Connection(ConnectionEvent::Connected(ctx, _)) => {
                log::info!("[Neighbours] Node {} connection {} connected", ctx.node, ctx.pair);
                for sub in self.subs.iter() {
                    self.output.push_back(FeatureOutput::Event(*sub, Event::Connected(ctx.node, ctx.conn)));
                }
            }
            FeatureSharedInput::Connection(ConnectionEvent::Disconnected(ctx)) => {
                log::info!("[Neighbours] Node {} connection {} disconnected", ctx.node, ctx.pair);
                let entry = self.nodes.entry(ctx.node).or_default();
                entry.remove(&ctx.conn);
                if entry.is_empty() {
                    log::info!("[Neighbours] Node {} disconnected all connections => remove", ctx.node);
                    self.nodes.remove(&ctx.node);
                }
                for sub in self.subs.iter() {
                    self.output.push_back(FeatureOutput::Event(*sub, Event::Disconnected(ctx.node, ctx.conn)));
                }

                self.check_need_more_seeds();
            }
            _ => {}
        }
    }

    fn on_input(&mut self, _ctx: &FeatureContext, _now_ms: u64, input: FeatureInput<'_, UserData, Control, ToController>) {
        if let FeatureInput::Control(actor, control) = input {
            match control {
                Control::Sub => {
                    if !self.subs.contains(&actor) {
                        log::info!("[Neighbours] Sub to neighbours from {:?}", actor);
                        self.subs.push(actor);
                    }
                }
                Control::UnSub => {
                    if let Some(pos) = self.subs.iter().position(|x| *x == actor) {
                        log::info!("[Neighbours] UnSub to neighbours from {:?}", actor);
                        self.subs.swap_remove(pos);
                    }
                }
                Control::ConnectTo(addr, is_seed) => {
                    if is_seed {
                        self.seeds.insert(addr.node_id());
                    }
                    self.output.push_back(FeatureOutput::NeighboursConnectTo(addr));
                }
                Control::DisconnectFrom(node) => {
                    self.output.push_back(FeatureOutput::NeighboursDisconnectFrom(node));
                }
            }
        }
    }

    fn on_shutdown(&mut self, _ctx: &FeatureContext, _now: u64) {
        log::info!("[NeighboursFeature] Shutdown");
        self.shutdown = true;
    }
}

impl<UserData> TaskSwitcherChild<Output<UserData>> for NeighboursFeature<UserData> {
    type Time = u64;

    fn is_empty(&self) -> bool {
        self.shutdown && self.output.is_empty()
    }

    fn empty_event(&self) -> Output<UserData> {
        Output::OnResourceEmpty
    }

    fn pop_output(&mut self, _now: u64) -> Option<Output<UserData>> {
        self.output.pop_front()
    }
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub struct NeighboursFeatureWorker<UserData> {
    queue: DynamicDeque<WorkerOutput<UserData>, 1>,
    shutdown: bool,
}

impl<UserData> FeatureWorker<UserData, Control, Event, ToController, ToWorker> for NeighboursFeatureWorker<UserData> {
    fn on_input(&mut self, _ctx: &mut crate::base::FeatureWorkerContext, _now: u64, input: crate::base::FeatureWorkerInput<UserData, Control, ToWorker>) {
        match input {
            FeatureWorkerInput::Control(actor, control) => self.queue.push_back(FeatureWorkerOutput::ForwardControlToController(actor, control)),
            FeatureWorkerInput::Network(conn, header, buf) => self.queue.push_back(FeatureWorkerOutput::ForwardNetworkToController(conn, header, buf)),
            #[cfg(feature = "vpn")]
            FeatureWorkerInput::TunPkt(..) => {}
            FeatureWorkerInput::FromController(..) => {
                log::warn!("No handler for FromController");
            }
            FeatureWorkerInput::Local(header, buf) => self.queue.push_back(FeatureWorkerOutput::ForwardLocalToController(header, buf)),
        }
    }

    fn on_shutdown(&mut self, _ctx: &mut crate::base::FeatureWorkerContext, _now: u64) {
        self.shutdown = true;
    }
}

impl<UserData> TaskSwitcherChild<WorkerOutput<UserData>> for NeighboursFeatureWorker<UserData> {
    type Time = u64;

    fn is_empty(&self) -> bool {
        self.shutdown && self.queue.is_empty()
    }

    fn empty_event(&self) -> WorkerOutput<UserData> {
        WorkerOutput::OnResourceEmpty
    }

    fn pop_output(&mut self, _now: u64) -> Option<WorkerOutput<UserData>> {
        self.queue.pop_front()
    }
}
