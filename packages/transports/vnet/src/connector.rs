use crate::earth::VnetEarth;
use bluesea_identity::Protocol;
use network::transport::{OutgoingConnectionError, TransportConnector, TransportConnectingOutgoing};
use std::sync::Arc;

pub struct VnetConnector<MSG> {
    pub(crate) port: u64,
    pub(crate) earth: Arc<VnetEarth<MSG>>,
}

impl<MSG> TransportConnector for VnetConnector<MSG>
where
    MSG: Send + Sync + 'static,
{
    fn connect_to(&self, node_id: bluesea_identity::NodeId, addr: bluesea_identity::NodeAddr) -> Result<TransportConnectingOutgoing, OutgoingConnectionError> {
        for protocol in &addr {
            match protocol {
                Protocol::Memory(port) => {
                    if let Some(conn_id) = self.earth.create_outgoing(self.port, node_id, port) {
                        return Ok(TransportConnectingOutgoing { conn_id });
                    } else {
                        return Err(OutgoingConnectionError::DestinationNotFound);
                    }
                }
                _ => {}
            }
        }
        Err(OutgoingConnectionError::UnsupportedProtocol)
    }
}
