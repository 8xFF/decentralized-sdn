use crate::connection::{TcpConnectionReceiver, TcpConnectionSender};
use crate::handshake::{outgoing_handshake, OutgoingHandshakeError};
use crate::msg::TcpMsg;
use crate::TCP_PROTOCOL_ID;
use async_bincode::futures::AsyncBincodeStream;
use async_std::channel::Sender;
use async_std::net::{Shutdown, TcpStream};
use atm0s_sdn_identity::{ConnId, NodeAddr, NodeAddrBuilder, NodeId, Protocol};
use atm0s_sdn_network::transport::{OutgoingConnectionError, TransportConnector, TransportEvent};
use atm0s_sdn_utils::error_handle::ErrorUtils;
use atm0s_sdn_utils::Timer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct TcpConnector {
    pub(crate) conn_id_seed: u64,
    pub(crate) node_id: NodeId,
    pub(crate) node_addr_builder: Arc<NodeAddrBuilder>,
    pub(crate) internal_tx: Sender<TransportEvent>,
    pub(crate) timer: Arc<dyn Timer>,
    pub(crate) pending_outgoing: HashMap<ConnId, (NodeId, NodeAddr, SocketAddr)>,
}

impl TcpConnector {}

impl TransportConnector for TcpConnector {
    fn create_pending_outgoing(&mut self, dest: NodeAddr) -> Vec<ConnId> {
        let mut res = vec![];
        let mut ip_v4 = None;
        for proto in dest.multiaddr().iter() {
            match proto {
                Protocol::Ip4(ip) => {
                    ip_v4 = Some(ip);
                }
                Protocol::Tcp(portnum) => match &ip_v4 {
                    Some(ip) => {
                        let uuid = self.conn_id_seed;
                        self.conn_id_seed += 1;
                        let conn_id = ConnId::from_out(TCP_PROTOCOL_ID, uuid);
                        res.push(conn_id);
                        self.pending_outgoing.insert(conn_id, (dest.node_id(), dest.clone(), SocketAddr::new(ip.clone().into(), portnum)));
                    }
                    None => {
                        log::error!("[TcpConnector] No ip4 address found in node addr {}", dest);
                    }
                },
                Protocol::Memory(_) => {}
                _ => {}
            }
        }
        res
    }

    fn continue_pending_outgoing(&mut self, conn_id: ConnId) {
        if let Some((remote_node_id, remote_node_addr, remote_addr)) = self.pending_outgoing.remove(&conn_id) {
            log::info!("[TcpConnector] connect to node {}", remote_node_addr);
            let timer = self.timer.clone();
            let node_id = self.node_id;
            let node_addr = self.node_addr_builder.addr();
            let conn_id = ConnId::from_out(TCP_PROTOCOL_ID, self.conn_id_seed);
            self.conn_id_seed += 1;
            let internal_tx = self.internal_tx.clone();
            async_std::task::spawn(async move {
                match TcpStream::connect(remote_addr).await {
                    Ok(socket) => {
                        let mut socket_read = AsyncBincodeStream::<_, TcpMsg, TcpMsg, _>::from(socket.clone()).for_async();
                        let socket_write = AsyncBincodeStream::<_, TcpMsg, TcpMsg, _>::from(socket.clone()).for_async();
                        match outgoing_handshake(remote_node_id, node_id, node_addr, &mut socket_read, conn_id, &internal_tx).await {
                            Ok(_) => {
                                let (connection_sender, unreliable_sender) = TcpConnectionSender::new(node_id, remote_node_id, remote_node_addr.clone(), conn_id, 1000, socket_write, timer.clone());
                                let connection_receiver = Box::new(TcpConnectionReceiver {
                                    remote_node_id,
                                    remote_addr: remote_node_addr,
                                    conn_id,
                                    socket: socket_read,
                                    timer,
                                    unreliable_sender,
                                });
                                internal_tx
                                    .send(TransportEvent::Outgoing(Arc::new(connection_sender), connection_receiver))
                                    .await
                                    .print_error("Should send Outgoing");
                            }
                            Err(err) => {
                                socket.shutdown(Shutdown::Both).print_error("Should shutdown socket");
                                internal_tx
                                    .send(TransportEvent::OutgoingError {
                                        node_id: remote_node_id,
                                        conn_id,
                                        err: match err {
                                            OutgoingHandshakeError::SocketError => OutgoingConnectionError::DestinationNotFound,
                                            OutgoingHandshakeError::Timeout => OutgoingConnectionError::AuthenticationError,
                                            OutgoingHandshakeError::WrongMsg => OutgoingConnectionError::AuthenticationError,
                                            OutgoingHandshakeError::Rejected => OutgoingConnectionError::AuthenticationError,
                                        },
                                    })
                                    .await
                                    .print_error("Should send OutgoingError");
                            }
                        }
                    }
                    Err(err) => {
                        log::error!("TcpStream connect error {}", err);
                        internal_tx
                            .send(TransportEvent::OutgoingError {
                                node_id: remote_node_id,
                                conn_id,
                                err: OutgoingConnectionError::DestinationNotFound,
                            })
                            .await
                            .print_error("Should send OutgoingError::DestinationNotFound");
                    }
                }
            });
        }
    }

    fn destroy_pending_outgoing(&mut self, conn_id: ConnId) {
        self.pending_outgoing.remove(&conn_id);
    }
}
