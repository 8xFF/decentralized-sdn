#![allow(clippy::bool_assert_comparison)]

use std::{fmt::Debug, hash::Hash};

pub use atm0s_sdn_identity::{ConnDirection, ConnId, NodeAddr, NodeAddrBuilder, NodeId, NodeIdType, Protocol};
pub use atm0s_sdn_network::controller_plane::ControllerPlaneCfg;
pub use atm0s_sdn_network::data_plane::DataPlaneCfg;
use atm0s_sdn_network::features::FeaturesControl;
pub use atm0s_sdn_network::{
    base, features, secure, services,
    worker::{SdnWorker, SdnWorkerBusEvent, SdnWorkerCfg, SdnWorkerInput, SdnWorkerOutput},
};
pub use atm0s_sdn_network::{
    base::ServiceId,
    data_plane::{NetInput, NetOutput},
};
pub use atm0s_sdn_router::{shadow::ShadowRouterHistory, RouteRule, ServiceBroadcastLevel};
pub use sans_io_runtime;

mod builder;
mod history;
mod time;
mod worker_inner;

pub use builder::{generate_node_addr, SdnBuilder};
pub use history::DataWorkerHistory;
pub use time::{TimePivot, TimeTicker};
pub use worker_inner::{SdnChannel, SdnController, SdnEvent, SdnExtIn, SdnExtOut, SdnOwner};

pub trait SdnControllerUtils<UserData, SC> {
    fn feature_control(&mut self, userdata: UserData, cmd: FeaturesControl);
    fn service_control(&mut self, service: ServiceId, userdata: UserData, cmd: SC);
}

impl<
        UserData: 'static + Send + Sync + Copy + Eq + Hash + Debug,
        SC: 'static + Send + Sync + Clone,
        SE: 'static + Send + Sync + Clone,
        TC: 'static + Send + Sync + Clone,
        TW: 'static + Send + Sync + Clone,
    > SdnControllerUtils<UserData, SC> for SdnController<UserData, SC, SE, TC, TW>
{
    fn feature_control(&mut self, userdata: UserData, cmd: FeaturesControl) {
        self.send_to(0, SdnExtIn::FeaturesControl(userdata, cmd));
    }

    fn service_control(&mut self, service: ServiceId, userdata: UserData, cmd: SC) {
        self.send_to(0, SdnExtIn::ServicesControl(service, userdata, cmd));
    }
}
