mod control;
mod feature;
mod msg;
mod secure;
mod service;

use atm0s_sdn_identity::{ConnId, NodeId};
pub use control::*;
pub use feature::*;
pub use msg::*;
pub use sans_io_runtime::Buffer;
pub use secure::*;
pub use service::*;

use crate::data_plane::NetPair;

#[derive(Debug, Clone)]
pub struct ConnectionCtx {
    pub conn: ConnId,
    pub node: NodeId,
    pub pair: NetPair,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStats {
    pub rtt_ms: u32,
}

#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    Connecting(ConnectionCtx),
    ConnectError(ConnectionCtx, NeighboursConnectError),
    Connected(ConnectionCtx, SecureContext),
    Stats(ConnectionCtx, ConnectionStats),
    Disconnected(ConnectionCtx),
}
