#[cfg(test)]
mod tests {
    use crate::behaviour::{ConnectionHandler, NetworkBehavior};
    use crate::mock::MockTransport;
    use crate::msg::TransportMsg;
    use crate::plane::{NetworkPlane, NetworkPlaneConfig};
    use crate::transport::{ConnectionRejectReason, ConnectionSender, OutgoingConnectionError};
    use crate::BehaviorAgent;
    use bluesea_identity::{ConnId, NodeId};
    use bluesea_router::ForceLocalRouter;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use utils::option_handle::OptionUtils;
    use utils::SystemTimer;

    enum TestCrossNetworkMsg {}
    enum TestCrossBehaviorEvent {
        Test,
    }
    enum TestCrossHandleEvent {}

    #[derive(convert_enum::From, convert_enum::TryInto)]
    enum ImplTestCrossNetworkBehaviorEvent {
        Test(TestCrossBehaviorEvent),
    }

    #[derive(convert_enum::From, convert_enum::TryInto)]
    enum ImplTestCrossNetworkHandlerEvent {
        Test(TestCrossHandleEvent),
    }

    #[derive(convert_enum::From, convert_enum::TryInto)]
    enum ImplTestCrossNetworkMsg {
        Test(TestCrossNetworkMsg),
    }

    struct TestCrossNetworkBehavior {
        flag: Arc<AtomicU16>,
    }

    impl<BE, HE> NetworkBehavior<BE, HE> for TestCrossNetworkBehavior
    where
        BE: From<TestCrossBehaviorEvent> + TryInto<TestCrossBehaviorEvent> + Send + Sync + 'static,
        HE: From<TestCrossHandleEvent> + TryInto<TestCrossHandleEvent> + Send + Sync + 'static,
    {
        fn service_id(&self) -> u8 {
            1
        }
        fn on_tick(&mut self, _agent: &BehaviorAgent<BE, HE>, _ts_ms: u64, _interal_ms: u64) {}

        fn check_incoming_connection(&mut self, _node: NodeId, _conn_id: ConnId) -> Result<(), ConnectionRejectReason> {
            Ok(())
        }

        fn check_outgoing_connection(&mut self, _node: NodeId, _conn_id: ConnId) -> Result<(), ConnectionRejectReason> {
            Ok(())
        }

        fn on_local_event(&mut self, _agent: &BehaviorAgent<BE, HE>, _event: BE) {
            self.flag.fetch_add(1, Ordering::Relaxed);
        }

        fn on_local_msg(&mut self, _agent: &BehaviorAgent<BE, HE>, _msg: TransportMsg) {}

        fn on_incoming_connection_connected(&mut self, _agent: &BehaviorAgent<BE, HE>, _connection: Arc<dyn ConnectionSender>) -> Option<Box<dyn ConnectionHandler<BE, HE>>> {
            None
        }
        fn on_outgoing_connection_connected(&mut self, _agent: &BehaviorAgent<BE, HE>, _connection: Arc<dyn ConnectionSender>) -> Option<Box<dyn ConnectionHandler<BE, HE>>> {
            None
        }
        fn on_incoming_connection_disconnected(&mut self, _agent: &BehaviorAgent<BE, HE>, _connection: Arc<dyn ConnectionSender>) {}
        fn on_outgoing_connection_disconnected(&mut self, _agent: &BehaviorAgent<BE, HE>, _connection: Arc<dyn ConnectionSender>) {}
        fn on_outgoing_connection_error(&mut self, _agent: &BehaviorAgent<BE, HE>, _node_id: NodeId, _conn_id: ConnId, _err: &OutgoingConnectionError) {}
        fn on_handler_event(&mut self, _agent: &BehaviorAgent<BE, HE>, _node_id: NodeId, _conn_id: ConnId, _event: BE) {}

        fn on_started(&mut self, agent: &BehaviorAgent<BE, HE>) {
            agent.send_to_behaviour(TestCrossBehaviorEvent::Test.into());
        }

        fn on_stopped(&mut self, _agent: &BehaviorAgent<BE, HE>) {}
    }

    #[async_std::test]
    async fn test_local_event() {
        let flag = Arc::new(AtomicU16::new(0));
        let behavior = Box::new(TestCrossNetworkBehavior { flag: flag.clone() });

        let (mock, _faker, _output) = MockTransport::new();
        let transport = Box::new(mock);
        let timer = Arc::new(SystemTimer());

        let mut plane = NetworkPlane::<ImplTestCrossNetworkBehaviorEvent, ImplTestCrossNetworkHandlerEvent>::new(NetworkPlaneConfig {
            local_node_id: 0,
            tick_ms: 1000,
            behavior: vec![behavior],
            transport,
            timer,
            router: Arc::new(ForceLocalRouter()),
        });

        let join = async_std::task::spawn(async move {
            plane.started();
            while let Ok(_) = plane.recv().await {}
            plane.stopped();
        });

        async_std::task::sleep(Duration::from_millis(1000)).await;
        assert_eq!(flag.load(Ordering::Relaxed), 1);
        join.cancel().await.print_none("Should cancel join");
    }
}
