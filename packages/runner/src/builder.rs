use std::{
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use atm0s_sdn_identity::{NodeAddr, NodeAddrBuilder, NodeId, Protocol};
use atm0s_sdn_network::{
    base::{Authorization, HandshakeBuilder, ServiceBuilder},
    features::{FeaturesControl, FeaturesEvent},
    secure::{HandshakeBuilderXDA, StaticKeyAuthorization},
    services::{
        manual2_discovery::{self, AdvertiseTarget},
        manual_discovery, visualization,
    },
};
use rand::{thread_rng, RngCore};
use sans_io_runtime::backend::Backend;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    history::DataWorkerHistory,
    worker_inner::{ControllerCfg, SdnController, SdnInnerCfg, SdnOwner, SdnWorkerInner},
};

pub struct SdnBuilder<UserData, SC, SE, TC, TW, NodeInfo> {
    auth: Option<Arc<dyn Authorization>>,
    handshake: Option<Arc<dyn HandshakeBuilder>>,
    node_addr: NodeAddr,
    node_id: NodeId,
    session: u64,
    bind_addrs: Vec<SocketAddr>,
    tick_ms: u64,
    visualization_collector: bool,
    #[allow(clippy::type_complexity)]
    services: Vec<Arc<dyn ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>>>,
    #[cfg(feature = "vpn")]
    vpn_enable: bool,
    #[cfg(feature = "vpn")]
    vpn_ip: Option<(u8, u8, u8, u8)>,
    #[cfg(feature = "vpn")]
    vpn_netmask: Option<(u8, u8, u8, u8)>,
    _tmp: PhantomData<NodeInfo>,
}

impl<UserData, SC, SE, TC: Debug, TW: Debug, NodeInfo> SdnBuilder<UserData, SC, SE, TC, TW, NodeInfo>
where
    UserData: 'static + Clone + Debug + Send + Sync + Copy + Eq + Hash,
    NodeInfo: 'static + Clone + Debug + Send + Sync + Serialize + DeserializeOwned,
    SC: 'static + Clone + Debug + Send + Sync + From<visualization::Control<NodeInfo>> + TryInto<visualization::Control<NodeInfo>>,
    SE: 'static + Clone + Debug + Send + Sync + From<visualization::Event<NodeInfo>> + TryInto<visualization::Event<NodeInfo>>,
    TC: 'static + Clone + Send + Sync,
    TW: 'static + Clone + Send + Sync,
{
    pub fn new(node_id: NodeId, bind_addrs: &[SocketAddr], custom_ip: Vec<SocketAddr>) -> Self {
        let node_addr = generate_node_addr(node_id, bind_addrs, custom_ip);
        log::info!("Created node on addr {}", node_addr);

        Self {
            auth: None,
            handshake: None,
            node_addr,
            node_id,
            tick_ms: 1000,
            session: thread_rng().next_u64(),
            bind_addrs: bind_addrs.to_vec(),
            visualization_collector: false,
            services: vec![],
            #[cfg(feature = "vpn")]
            vpn_enable: false,
            #[cfg(feature = "vpn")]
            vpn_ip: None,
            #[cfg(feature = "vpn")]
            vpn_netmask: None,
            _tmp: PhantomData,
        }
    }

    pub fn node_addr(&self) -> NodeAddr {
        self.node_addr.clone()
    }

    /// Setting authorization
    pub fn set_authorization<A: Authorization + 'static>(&mut self, auth: A) {
        self.auth = Some(Arc::new(auth));
    }

    /// Setting handshake
    pub fn set_handshake<H: HandshakeBuilder + 'static>(&mut self, handshake: H) {
        self.handshake = Some(Arc::new(handshake));
    }

    /// Setting visualization collector mode
    pub fn set_visualization_collector(&mut self, value: bool) {
        self.visualization_collector = value;
    }

    /// Setting manual discovery
    pub fn set_manual_discovery(&mut self, local_tags: Vec<String>, connect_tags: Vec<String>) {
        self.add_service(Arc::new(manual_discovery::ManualDiscoveryServiceBuilder::new(self.node_addr.clone(), local_tags, connect_tags)));
    }

    /// Setting manual2 discovery
    pub fn set_manual2_discovery(&mut self, targets: Vec<AdvertiseTarget>, interval: u64) {
        self.add_service(Arc::new(manual2_discovery::Manual2DiscoveryServiceBuilder::new(self.node_addr.clone(), targets, interval)));
    }

    /// panic if the service already exists
    pub fn add_service(&mut self, service: Arc<dyn ServiceBuilder<UserData, FeaturesControl, FeaturesEvent, SC, SE, TC, TW>>) {
        for s in self.services.iter() {
            assert_ne!(s.service_id(), service.service_id(), "Service ({}, {}) already exists", service.service_id(), service.service_name());
        }
        self.services.push(service);
    }

    #[cfg(feature = "vpn")]
    pub fn enable_vpn(&mut self) {
        self.vpn_enable = true;
    }

    #[cfg(feature = "vpn")]
    pub fn set_vpn_ip(&mut self, ip: (u8, u8, u8, u8)) {
        self.vpn_ip = Some(ip);
    }

    #[cfg(feature = "vpn")]
    pub fn set_vpn_netmask(&mut self, netmask: (u8, u8, u8, u8)) {
        self.vpn_netmask = Some(netmask);
    }

    pub fn build<B: Backend<SdnOwner>>(mut self, workers: usize, info: NodeInfo) -> SdnController<UserData, SC, SE, TC, TW> {
        assert!(workers > 0);
        #[cfg(feature = "vpn")]
        let (tun_device, mut queue_fds) = {
            if self.vpn_enable {
                let vpn_ip = self.vpn_ip.unwrap_or((10, 33, 33, self.node_id as u8));
                let vpn_netmask = self.vpn_netmask.unwrap_or((255, 255, 255, 0));
                let mut tun_device = sans_io_runtime::backend::tun::create_tun(&format!("utun{}", self.node_id as u8), vpn_ip, vpn_netmask, 1400, workers);
                let mut queue_fds = std::collections::VecDeque::with_capacity(workers);
                for i in 0..workers {
                    queue_fds.push_back(tun_device.get_queue_fd(i).expect("Should have tun queue fd"));
                }
                (Some(tun_device), queue_fds)
            } else {
                (None, std::collections::VecDeque::new())
            }
        };

        self.add_service(Arc::new(visualization::VisualizationServiceBuilder::<UserData, SC, SE, TC, TW, NodeInfo>::new(
            info,
            self.visualization_collector,
        )));

        let history = Arc::new(DataWorkerHistory::default());

        let mut controller = SdnController::default();
        controller.add_worker::<SdnOwner, _, SdnWorkerInner<UserData, SC, SE, TC, TW>, B>(
            Duration::from_millis(1000),
            SdnInnerCfg {
                node_id: self.node_id,
                tick_ms: self.tick_ms,
                bind_addrs: self.bind_addrs.to_vec(),
                services: self.services.clone(),
                history: history.clone(),
                controller: Some(ControllerCfg {
                    session: self.session,
                    auth: self.auth.unwrap_or_else(|| Arc::new(StaticKeyAuthorization::new("unsecure"))),
                    handshake: self.handshake.unwrap_or_else(|| Arc::new(HandshakeBuilderXDA)),
                    #[cfg(feature = "vpn")]
                    vpn_tun_device: tun_device,
                }),
                #[cfg(feature = "vpn")]
                vpn_tun_fd: queue_fds.pop_front(),
            },
            None,
        );

        for _ in 1..workers {
            controller.add_worker::<SdnOwner, _, SdnWorkerInner<UserData, SC, SE, TC, TW>, B>(
                Duration::from_millis(1000),
                SdnInnerCfg {
                    node_id: self.node_id,
                    tick_ms: self.tick_ms,
                    bind_addrs: self.bind_addrs.to_vec(),
                    services: self.services.clone(),
                    history: history.clone(),
                    controller: None,
                    #[cfg(feature = "vpn")]
                    vpn_tun_fd: queue_fds.pop_front(),
                },
                None,
            );
        }
        controller
    }
}

pub fn generate_node_addr(node_id: u32, bind_addrs: &[SocketAddr], custom_ips: Vec<SocketAddr>) -> NodeAddr {
    let mut addr_builder = NodeAddrBuilder::new(node_id);
    for bind_addr in bind_addrs {
        match bind_addr.ip() {
            IpAddr::V4(ip) => {
                log::info!("Added ipv4 {}", ip);
                addr_builder.add_protocol(Protocol::Ip4(ip));
                addr_builder.add_protocol(Protocol::Udp(bind_addr.port()));
            }
            IpAddr::V6(ip) => {
                log::info!("Added ipv6 {}", ip);
                addr_builder.add_protocol(Protocol::Ip6(ip));
                addr_builder.add_protocol(Protocol::Udp(bind_addr.port()));
            }
        }
    }
    for ip in custom_ips {
        match ip {
            SocketAddr::V4(ip) => {
                log::info!("Added custom ipv4:\t{:?}", ip);
                addr_builder.add_protocol(Protocol::Ip4(*ip.ip()));
                addr_builder.add_protocol(Protocol::Udp(ip.port()));
            }
            SocketAddr::V6(ip) => {
                log::info!("Added custom ipv6:\t{:?}", ip);
                addr_builder.add_protocol(Protocol::Ip6(*ip.ip()));
                addr_builder.add_protocol(Protocol::Udp(ip.port()));
            }
        }
    }

    addr_builder.addr()
}
