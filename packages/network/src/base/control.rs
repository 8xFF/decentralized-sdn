use atm0s_sdn_identity::NodeId;
use bincode::Options;
use serde::{Deserialize, Serialize};

use super::Authorization;

const MSG_TIMEOUT_MS: u64 = 10000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NeighboursConnectError {
    AlreadyConnected,
    InvalidSignature,
    InvalidData,
    InvalidState,
    Timeout,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NeighboursDisconnectReason {
    Shutdown,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NeighboursControlCmds {
    ConnectRequest { to: NodeId, session: u64, handshake: Vec<u8> },
    ConnectResponse { session: u64, result: Result<Vec<u8>, NeighboursConnectError> },
    Ping { session: u64, seq: u64, sent_ms: u64 },
    Pong { session: u64, seq: u64, sent_ms: u64 },
    DisconnectRequest { session: u64, reason: NeighboursDisconnectReason },
    DisconnectResponse { session: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighboursControl {
    pub from: NodeId,
    pub cmd: Vec<u8>,
    pub signature: Vec<u8>,
}

impl NeighboursControl {
    #[allow(clippy::result_unit_err)]
    pub fn validate(&self, now: u64, auth: &dyn Authorization) -> Result<NeighboursControlCmds, ()> {
        auth.validate(self.from, &self.cmd, &self.signature).ok_or(())?;
        let (ts, cmd) = bincode::DefaultOptions::new().with_limit(1499).deserialize::<(u64, NeighboursControlCmds)>(&self.cmd).map_err(|_| ())?;
        if ts + MSG_TIMEOUT_MS < now {
            return Err(());
        }
        Ok(cmd)
    }

    pub fn build(now: u64, from: NodeId, cmd: NeighboursControlCmds, auth: &dyn Authorization) -> Self {
        let cmd = bincode::DefaultOptions::new().with_limit(1499).serialize(&(now, cmd)).unwrap();
        let signature = auth.sign(&cmd);
        Self { from, cmd, signature }
    }
}

impl TryFrom<&[u8]> for NeighboursControl {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.first() == Some(&255) {
            bincode::DefaultOptions::new().with_limit(1499).deserialize(&value[1..]).map_err(|_| ())
        } else {
            Err(())
        }
    }
}

impl TryInto<Vec<u8>> for &NeighboursControl {
    type Error = ();

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let mut buf = Vec::with_capacity(1500);
        buf.push(255);
        bincode::DefaultOptions::new().with_limit(1499).serialize_into(&mut buf, &self).map_err(|_| ())?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::secure::StaticKeyAuthorization;

    use super::*;

    #[test]
    fn test_neighbours_control() {
        let auth = StaticKeyAuthorization::new("demo_key");
        let cmd = NeighboursControlCmds::Ping {
            session: 1000,
            seq: 100,
            sent_ms: 100,
        };
        let control = NeighboursControl::build(0, 1, cmd.clone(), &auth);
        assert_eq!(control.validate(0, &auth), Ok(cmd));
        assert_eq!(control.validate(MSG_TIMEOUT_MS + 1, &auth), Err(()));
    }
}
