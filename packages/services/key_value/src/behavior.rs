use crate::handler::KeyValueConnectionHandler;
use crate::msg::{KeyValueBehaviorEvent, KeyValueMsg};
use crate::KEY_VALUE_SERVICE_ID;
use bluesea_identity::{ConnId, NodeId};
use network::behaviour::{ConnectionHandler, NetworkBehavior};
use network::msg::{MsgHeader, TransportMsg};
use network::transport::{ConnectionRejectReason, ConnectionSender, OutgoingConnectionError, RpcAnswer};
use network::BehaviorAgent;
use parking_lot::RwLock;
use std::sync::Arc;
use utils::Timer;

use self::simple_local::LocalStorage;
use self::simple_remote::RemoteStorage;

mod event_acks;
mod sdk;
mod simple_local;
mod simple_remote;

pub use sdk::KeyValueSdk;

#[allow(unused)]
pub struct KeyValueBehavior {
    node_id: NodeId,
    simple_remote: RemoteStorage,
    simple_local: Arc<RwLock<LocalStorage>>,
}

impl KeyValueBehavior {
    #[allow(unused)]
    pub fn new(node_id: NodeId, timer: Arc<dyn Timer>, sync_each_ms: u64) -> (Self, sdk::KeyValueSdk) {
        log::info!("[KeyValueBehaviour {}] created with sync_each_ms {}", node_id, sync_each_ms);
        let simple_local = Arc::new(RwLock::new(LocalStorage::new(timer.clone(), sync_each_ms)));
        let sdk = sdk::KeyValueSdk::new(simple_local.clone());

        (
            Self {
                node_id,
                simple_remote: RemoteStorage::new(timer),
                simple_local,
            },
            sdk,
        )
    }

    fn pop_all_events<BE, HE>(&mut self, agent: &BehaviorAgent<BE, HE>)
    where
        BE: Send + Sync + 'static,
        HE: Send + Sync + 'static,
    {
        while let Some(action) = self.simple_remote.pop_action() {
            log::debug!("[KeyValueBehavior {}] pop_all_events remote: {:?}", self.node_id, action);
            let mut header = MsgHeader::build_reliable(KEY_VALUE_SERVICE_ID, action.1, 0);
            header.from_node = Some(self.node_id);
            agent.send_to_net(TransportMsg::from_payload_bincode(header, &KeyValueMsg::Local(action.0)));
        }

        while let Some(action) = self.simple_local.write().pop_action() {
            log::debug!("[KeyValueBehavior {}] pop_all_events local: {:?}", self.node_id, action);
            let mut header = MsgHeader::build_reliable(KEY_VALUE_SERVICE_ID, action.1, 0);
            header.from_node = Some(self.node_id);
            agent.send_to_net(TransportMsg::from_payload_bincode(header, &KeyValueMsg::Remote(action.0)));
        }
    }

    fn process_key_value_msg<BE, HE>(&mut self, header: MsgHeader, msg: KeyValueMsg, agent: &BehaviorAgent<BE, HE>)
    where
        BE: Send + Sync + 'static,
        HE: Send + Sync + 'static,
    {
        match msg {
            KeyValueMsg::Remote(msg) => {
                if let Some(from) = header.from_node {
                    log::debug!("[KeyValueBehavior {}] process_key_value_msg remote: {:?} from {}", self.node_id, msg, from);
                    self.simple_remote.on_event(from, msg);
                    self.pop_all_events(agent);
                } else {
                    log::warn!("[KeyValueBehavior {}] process_key_value_msg remote: no from_node", self.node_id);
                }
            }
            KeyValueMsg::Local(msg) => {
                if let Some(from) = header.from_node {
                    log::debug!("[KeyValueBehavior {}] process_key_value_msg local: {:?} from {}", self.node_id, msg, from);
                    self.simple_local.write().on_event(from, msg);
                    self.pop_all_events(agent);
                } else {
                    log::warn!("[KeyValueBehavior {}] process_key_value_msg local: no from_node", self.node_id);
                }
            }
        }
    }
}

#[allow(unused)]
impl<BE, HE, Req, Res> NetworkBehavior<BE, HE, Req, Res> for KeyValueBehavior
where
    BE: From<KeyValueBehaviorEvent> + TryInto<KeyValueBehaviorEvent> + Send + Sync + 'static,
    HE: Send + Sync + 'static,
{
    fn service_id(&self) -> u8 {
        KEY_VALUE_SERVICE_ID
    }

    fn on_tick(&mut self, agent: &BehaviorAgent<BE, HE>, ts_ms: u64, interal_ms: u64) {
        log::debug!("[KeyValueBehavior {}] on_tick ts_ms {}, interal_ms {}", self.node_id, ts_ms, interal_ms);
        self.simple_remote.tick();
        self.simple_local.write().tick();
        self.pop_all_events(agent);
    }

    fn check_incoming_connection(&mut self, node: NodeId, conn_id: ConnId) -> Result<(), ConnectionRejectReason> {
        Ok(())
    }

    fn check_outgoing_connection(&mut self, node: NodeId, conn_id: ConnId) -> Result<(), ConnectionRejectReason> {
        Ok(())
    }

    fn on_local_msg(&mut self, agent: &BehaviorAgent<BE, HE>, msg: TransportMsg) {
        match msg.get_payload_bincode::<KeyValueMsg>() {
            Ok(kv_msg) => {
                log::debug!("[KeyValueBehavior {}] on_local_msg: {:?}", self.node_id, kv_msg);
                self.process_key_value_msg(msg.header, kv_msg, agent);
            }
            Err(e) => {
                log::error!("Error on get_payload_bincode: {:?}", e);
            }
        }
    }

    fn on_incoming_connection_connected(&mut self, agent: &BehaviorAgent<BE, HE>, conn: Arc<dyn ConnectionSender>) -> Option<Box<dyn ConnectionHandler<BE, HE>>> {
        Some(Box::new(KeyValueConnectionHandler::new()))
    }

    fn on_outgoing_connection_connected(&mut self, agent: &BehaviorAgent<BE, HE>, conn: Arc<dyn ConnectionSender>) -> Option<Box<dyn ConnectionHandler<BE, HE>>> {
        Some(Box::new(KeyValueConnectionHandler::new()))
    }

    fn on_incoming_connection_disconnected(&mut self, agent: &BehaviorAgent<BE, HE>, conn: Arc<dyn ConnectionSender>) {}

    fn on_outgoing_connection_disconnected(&mut self, agent: &BehaviorAgent<BE, HE>, conn: Arc<dyn ConnectionSender>) {}

    fn on_outgoing_connection_error(&mut self, agent: &BehaviorAgent<BE, HE>, node_id: NodeId, conn_id: ConnId, err: &OutgoingConnectionError) {}

    fn on_handler_event(&mut self, agent: &BehaviorAgent<BE, HE>, node_id: NodeId, conn_id: ConnId, event: BE) {
        if let Ok(msg) = event.try_into() {
            match msg {
                KeyValueBehaviorEvent::FromNode(header, msg) => {
                    self.process_key_value_msg(header, msg, agent);
                }
            }
        }
    }

    fn on_rpc(&mut self, agent: &BehaviorAgent<BE, HE>, req: Req, res: Box<dyn RpcAnswer<Res>>) -> bool {
        false
    }

    fn on_started(&mut self, _agent: &BehaviorAgent<BE, HE>) {}

    fn on_stopped(&mut self, _agent: &BehaviorAgent<BE, HE>) {}
}
