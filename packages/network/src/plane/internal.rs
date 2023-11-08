use std::{collections::VecDeque, fmt, sync::Arc};

use bluesea_identity::NodeId;
use utils::{awaker::Awaker, init_vec::init_vec};

use crate::{
    behaviour::{BehaviorContext, ConnectionHandler, NetworkBehavior, NetworkBehaviorAction},
    transport::{ConnectionReceiver, ConnectionSender, TransportEvent},
};

use super::NetworkPlaneInternalEvent;

#[derive(Debug)]
pub enum PlaneInternalError {
    InvalidServiceId(u8),
}

pub struct SpawnedConnection<BE, HE> {
    pub outgoing: bool,
    pub sender: Arc<dyn ConnectionSender>,
    pub receiver: Box<dyn ConnectionReceiver + Send>,
    pub handlers: Vec<Option<Box<dyn ConnectionHandler<BE, HE>>>>,
}

impl<BE, HE> fmt::Debug for SpawnedConnection<BE, HE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnedConnection")
            .field("outgoing", &self.outgoing)
            .field("sender", &self.sender.conn_id())
            .field("receiver", &self.receiver.conn_id())
            .field("handlers_count", &self.handlers.len())
            .finish()
    }
}

impl<BE, HE> PartialEq for SpawnedConnection<BE, HE> {
    fn eq(&self, other: &Self) -> bool {
        self.outgoing == other.outgoing && self.sender.conn_id() == other.sender.conn_id() && self.receiver.conn_id() == other.receiver.conn_id() && self.handlers.len() == other.handlers.len()
    }
}
impl<BE, HE> Eq for SpawnedConnection<BE, HE> {}

#[derive(Debug, Eq, PartialEq)]
pub enum PlaneInternalAction<BE, HE, SE> {
    SpawnConnection(SpawnedConnection<BE, HE>),
    BehaviorAction(u8, NetworkBehaviorAction<HE, SE>),
}

pub struct PlaneInternal<BE, HE, SE> {
    node_id: NodeId,
    action_queue: VecDeque<PlaneInternalAction<BE, HE, SE>>,
    behaviors: Vec<Option<(Box<dyn NetworkBehavior<BE, HE, SE> + Send + Sync>, BehaviorContext)>>,
}

impl<BE, HE, SE> PlaneInternal<BE, HE, SE> {
    pub fn new(node_id: NodeId, conf_behaviors: Vec<(Box<dyn NetworkBehavior<BE, HE, SE> + Send + Sync>, Arc<dyn Awaker>)>) -> Self {
        let mut behaviors: Vec<Option<(Box<dyn NetworkBehavior<BE, HE, SE> + Send + Sync>, BehaviorContext)>> = init_vec(256, || None);

        for (behavior, awake) in conf_behaviors {
            let service_id = behavior.service_id() as usize;
            if behaviors[service_id].is_none() {
                behaviors[service_id] = Some((behavior, BehaviorContext::new(service_id as u8, node_id, awake)));
            } else {
                panic!("Duplicate service {}", behavior.service_id())
            }
        }

        Self {
            node_id,
            action_queue: Default::default(),
            behaviors,
        }
    }

    pub fn started(&mut self, now_ms: u64) {
        log::info!("[NetworkPlane {}] started", self.node_id);
        for (behaviour, agent) in self.behaviors.iter_mut().flatten() {
            behaviour.on_started(agent, now_ms);
        }

        self.pop_behaviours_action(now_ms);
    }

    pub fn on_tick(&mut self, now_ms: u64, interval_ms: u64) {
        for (behaviour, context) in self.behaviors.iter_mut().flatten() {
            behaviour.on_tick(context, now_ms, interval_ms);
        }

        self.pop_behaviours_action(now_ms);
    }

    pub fn on_internal_event(&mut self, now_ms: u64, event: NetworkPlaneInternalEvent<BE>) -> Result<(), PlaneInternalError> {
        let res = match event {
            NetworkPlaneInternalEvent::AwakeBehaviour { service_id } => {
                log::debug!("[NetworkPlane {}] received NetworkPlaneInternalEvent::AwakeBehaviour service: {}", self.node_id, service_id);
                if let Some((behaviour, context)) = &mut self.behaviors[service_id as usize] {
                    behaviour.on_awake(context, now_ms);
                    Ok(())
                } else {
                    debug_assert!(false, "service not found {}", service_id);
                    Err(PlaneInternalError::InvalidServiceId(service_id))
                }
            }
            NetworkPlaneInternalEvent::IncomingDisconnected(node_id, conn_id) => {
                log::info!("[NetworkPlane {}] received NetworkPlaneInternalEvent::IncomingDisconnected({}, {})", self.node_id, node_id, conn_id);
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    behaviour.on_incoming_connection_disconnected(context, now_ms, node_id, conn_id);
                }
                Ok(())
            }
            NetworkPlaneInternalEvent::OutgoingDisconnected(node_id, conn_id) => {
                log::info!("[NetworkPlane {}] received NetworkPlaneInternalEvent::OutgoingDisconnected({}, {})", self.node_id, node_id, conn_id);
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    behaviour.on_outgoing_connection_disconnected(context, now_ms, node_id, conn_id);
                }
                Ok(())
            }
            NetworkPlaneInternalEvent::ToBehaviourFromHandler { service_id, node_id, conn_id, event } => {
                log::debug!(
                    "[NetworkPlane {}] received NetworkPlaneInternalEvent::ToBehaviour service: {}, from node: {} conn_id: {}",
                    self.node_id,
                    service_id,
                    node_id,
                    conn_id
                );
                if let Some((behaviour, context)) = &mut self.behaviors[service_id as usize] {
                    behaviour.on_handler_event(context, now_ms, node_id, conn_id, event);
                    Ok(())
                } else {
                    debug_assert!(false, "service not found {}", service_id);
                    Err(PlaneInternalError::InvalidServiceId(service_id))
                }
            }
            NetworkPlaneInternalEvent::ToBehaviourLocalMsg { service_id, msg } => {
                log::debug!("[NetworkPlane {}] received NetworkPlaneInternalEvent::ToBehaviourLocalMsg service: {}", self.node_id, service_id);
                if let Some((behaviour, context)) = &mut self.behaviors[service_id as usize] {
                    behaviour.on_local_msg(context, now_ms, msg);
                    Ok(())
                } else {
                    debug_assert!(false, "service not found {}", service_id);
                    Err(PlaneInternalError::InvalidServiceId(service_id))
                }
            }
        };

        self.pop_behaviours_action(now_ms);
        res
    }

    pub fn on_transport_event(&mut self, now_ms: u64, event: TransportEvent) -> Result<(), PlaneInternalError> {
        match event {
            TransportEvent::IncomingRequest(node, conn_id, acceptor) => {
                let mut rejected = false;
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    if let Err(err) = behaviour.check_incoming_connection(&context, now_ms, node, conn_id) {
                        acceptor.reject(err);
                        rejected = true;
                        break;
                    }
                }
                if !rejected {
                    acceptor.accept();
                }
            }
            TransportEvent::OutgoingRequest(node, conn_id, acceptor, local_uuid) => {
                let mut rejected = false;
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    if let Err(err) = behaviour.check_outgoing_connection(&context, now_ms, node, conn_id, local_uuid) {
                        acceptor.reject(err);
                        rejected = true;
                        break;
                    }
                }
                if !rejected {
                    acceptor.accept();
                }
            }
            TransportEvent::Incoming(sender, receiver) => {
                log::info!(
                    "[NetworkPlane {}] received TransportEvent::Incoming({}, {})",
                    self.node_id,
                    receiver.remote_node_id(),
                    receiver.conn_id()
                );
                let mut handlers: Vec<Option<Box<dyn ConnectionHandler<BE, HE>>>> = init_vec(256, || None);
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    handlers[behaviour.service_id() as usize] = behaviour.on_incoming_connection_connected(context, now_ms, sender.clone());
                }
                self.action_queue.push_back(PlaneInternalAction::SpawnConnection(SpawnedConnection {
                    outgoing: false,
                    sender,
                    receiver,
                    handlers,
                }));
            }
            TransportEvent::Outgoing(sender, receiver, local_uuid) => {
                log::info!(
                    "[NetworkPlane {}] received TransportEvent::Outgoing({}, {})",
                    self.node_id,
                    receiver.remote_node_id(),
                    receiver.conn_id()
                );
                let mut handlers: Vec<Option<Box<dyn ConnectionHandler<BE, HE>>>> = init_vec(256, || None);
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    handlers[behaviour.service_id() as usize] = behaviour.on_outgoing_connection_connected(context, now_ms, sender.clone(), local_uuid);
                }
                self.action_queue.push_back(PlaneInternalAction::SpawnConnection(SpawnedConnection {
                    outgoing: true,
                    sender,
                    receiver,
                    handlers,
                }));
            }
            TransportEvent::OutgoingError { local_uuid, node_id, conn_id, err } => {
                log::info!("[NetworkPlane {}] received TransportEvent::OutgoingError({}, {:?})", self.node_id, node_id, conn_id);
                for (behaviour, context) in self.behaviors.iter_mut().flatten() {
                    behaviour.on_outgoing_connection_error(context, now_ms, node_id, conn_id, local_uuid, &err);
                }
            }
        }

        self.pop_behaviours_action(now_ms);
        Ok(())
    }

    pub fn stopped(&mut self, now_ms: u64) {
        log::info!("[NetworkPlane {}] stopped", self.node_id);
        for (behaviour, agent) in self.behaviors.iter_mut().flatten() {
            behaviour.on_stopped(agent, now_ms);
        }

        self.pop_behaviours_action(now_ms);
    }

    pub fn pop_action(&mut self) -> Option<PlaneInternalAction<BE, HE, SE>> {
        self.action_queue.pop_front()
    }

    fn pop_behaviours_action(&mut self, now_ms: u64) {
        let mut sdk_msgs = vec![];
        for (behaviour, context) in self.behaviors.iter_mut().flatten() {
            while let Some(action) = behaviour.pop_action() {
                match action {
                    NetworkBehaviorAction::ToSdkService(service, msg) => {
                        sdk_msgs.push((context.service_id, service, msg));
                    }
                    _ => {
                        self.action_queue.push_back(PlaneInternalAction::BehaviorAction(context.service_id, action));
                    }
                }
            }
        }
        for (from, to, msg) in sdk_msgs {
            log::debug!("[NetworkPlane {}] received NetworkBehaviorAction::ToSdkService service: {}", self.node_id, from);
            if let Some((to_behaviour, to_context)) = &mut self.behaviors[to as usize] {
                to_behaviour.on_sdk_msg(to_context, now_ms, from, msg);
            } else {
                debug_assert!(false, "service not found {}", to);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use utils::awaker::{Awaker, MockAwaker};

    use crate::behaviour::MockNetworkBehavior;

    type BE = ();
    type HE = ();
    type SE = ();

    #[test]
    fn should_run_behaviors_on_started() {
        let mut mock_behavior_1 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_1.expect_on_started().times(1).return_const(());
        mock_behavior_1.expect_service_id().return_const(1);
        mock_behavior_1.expect_pop_action().returning(|| None);
        let mock_awaker_1: Arc<dyn Awaker> = Arc::new(MockAwaker::default());
        let mut mock_behavior_2 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_2.expect_on_started().times(1).return_const(());
        mock_behavior_2.expect_service_id().return_const(2);
        mock_behavior_2.expect_pop_action().returning(|| None);
        let mock_awaker_2: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut internal = super::PlaneInternal::new(1, vec![(mock_behavior_1, mock_awaker_1.clone()), (mock_behavior_2, mock_awaker_2.clone())]);

        internal.started(0);
    }

    #[test]
    fn should_tick_behaviors_on_tick() {
        let mut mock_behavior_1 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_1.expect_on_tick().times(1).return_const(());
        mock_behavior_1.expect_service_id().return_const(1);
        mock_behavior_1.expect_pop_action().returning(|| None);
        let mock_awaker_1: Arc<dyn Awaker> = Arc::new(MockAwaker::default());
        let mut mock_behavior_2 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_2.expect_on_tick().times(1).return_const(());
        mock_behavior_2.expect_service_id().return_const(2);
        mock_behavior_2.expect_pop_action().returning(|| None);
        let mock_awaker_2: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut internal = super::PlaneInternal::new(1, vec![(mock_behavior_1, mock_awaker_1.clone()), (mock_behavior_2, mock_awaker_2.clone())]);

        internal.on_tick(0, 0);
    }

    #[test]
    fn should_stop_behaviors_on_stop() {
        let mut mock_behavior_1 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_1.expect_on_stopped().once().return_const(());
        mock_behavior_1.expect_service_id().return_const(1);
        mock_behavior_1.expect_pop_action().returning(|| None);
        let mock_awaker_1: Arc<dyn Awaker> = Arc::new(MockAwaker::default());
        let mut mock_behavior_2 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_2.expect_on_stopped().once().return_const(());
        mock_behavior_2.expect_service_id().return_const(2);
        mock_behavior_2.expect_pop_action().returning(|| None);
        let mock_awaker_2: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut internal = super::PlaneInternal::new(1, vec![(mock_behavior_1, mock_awaker_1.clone()), (mock_behavior_2, mock_awaker_2.clone())]);

        internal.stopped(0);
    }

    #[test]
    fn should_pop_sdk_behaviors_actions() {
        let mut mb1_actions: Vec<super::NetworkBehaviorAction<HE, SE>> = vec![super::NetworkBehaviorAction::ToSdkService(2, ())];
        let mut mb2_actions: Vec<super::NetworkBehaviorAction<HE, SE>> = vec![];

        let mut mock_behavior_1 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_1.expect_service_id().return_const(1);
        mock_behavior_1.expect_pop_action().returning(move || mb1_actions.pop());
        mock_behavior_1.expect_on_sdk_msg().never();
        let mock_awaker_1: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut mock_behavior_2 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_2.expect_service_id().return_const(2);
        mock_behavior_2.expect_pop_action().returning(move || mb2_actions.pop());
        mock_behavior_2.expect_on_sdk_msg().once().return_const(());
        let mock_awaker_2: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut internal = super::PlaneInternal::new(1, vec![(mock_behavior_1, mock_awaker_1.clone()), (mock_behavior_2, mock_awaker_2.clone())]);

        internal.pop_behaviours_action(0);

        assert_eq!(internal.action_queue.len(), 0);
        assert_eq!(internal.pop_action(), None,);
    }

    #[test]
    fn should_pop_normal_behaviors_actions() {
        let mut mb1_actions: Vec<super::NetworkBehaviorAction<HE, SE>> = vec![super::NetworkBehaviorAction::CloseNode(1)];
        let mut mb2_actions: Vec<super::NetworkBehaviorAction<HE, SE>> = vec![super::NetworkBehaviorAction::CloseNode(2)];

        let mut mock_behavior_1 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_1.expect_service_id().return_const(1);
        mock_behavior_1.expect_pop_action().returning(move || mb1_actions.pop());
        mock_behavior_1.expect_on_sdk_msg().never();
        let mock_awaker_1: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut mock_behavior_2 = Box::new(MockNetworkBehavior::<BE, HE, SE>::new());
        mock_behavior_2.expect_service_id().return_const(2);
        mock_behavior_2.expect_pop_action().returning(move || mb2_actions.pop());
        mock_behavior_2.expect_on_sdk_msg().never();
        let mock_awaker_2: Arc<dyn Awaker> = Arc::new(MockAwaker::default());

        let mut internal = super::PlaneInternal::new(1, vec![(mock_behavior_1, mock_awaker_1.clone()), (mock_behavior_2, mock_awaker_2.clone())]);

        internal.pop_behaviours_action(0);

        assert_eq!(internal.action_queue.len(), 2);
        assert_eq!(internal.pop_action(), Some(super::PlaneInternalAction::BehaviorAction(1, super::NetworkBehaviorAction::CloseNode(1))));
        assert_eq!(internal.pop_action(), Some(super::PlaneInternalAction::BehaviorAction(2, super::NetworkBehaviorAction::CloseNode(2))));
    }
}
