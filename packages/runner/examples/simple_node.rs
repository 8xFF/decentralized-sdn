use atm0s_sdn_identity::{NodeAddr, NodeId};
use atm0s_sdn_network::{secure::StaticKeyAuthorization, services::visualization};
use clap::{Parser, ValueEnum};
use sans_io_runtime::backend::{PollBackend, PollingBackend};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use atm0s_sdn::{
    features::{neighbours, FeaturesControl},
    SdnBuilder, SdnExtIn, SdnOwner,
};

#[derive(Debug, Clone, ValueEnum)]
enum BackendType {
    Poll,
    Polling,
}

/// Simple program to running a node
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Node Id
    #[arg(short, long)]
    node_id: NodeId,

    /// Listen address
    #[arg(short, long)]
    bind_addr: SocketAddr,

    /// Address of node we should connect to
    #[arg(short, long)]
    seeds: Vec<NodeAddr>,

    /// Password for the network
    #[arg(short, long, default_value = "password")]
    password: String,

    /// Backend type
    #[arg(short, long, default_value = "polling")]
    backend: BackendType,

    /// Backend type
    #[arg(short, long)]
    vpn: bool,

    /// Workers
    #[arg(long, default_value_t = 2)]
    workers: usize,

    /// Custom IP
    #[arg(long)]
    custom_addrs: Vec<SocketAddr>,

    /// Local tags
    #[arg(long)]
    local_tags: Vec<String>,

    /// Connect tags
    #[arg(long)]
    connect_tags: Vec<String>,
}

type UserInfo = u32;
type SC = visualization::Control<UserInfo>;
type SE = visualization::Event<UserInfo>;
type TC = ();
type TW = ();

fn main() {
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term)).expect("Should register hook");
    let mut shutdown_wait = 0;
    let args = Args::parse();
    env_logger::builder().format_timestamp_millis().init();
    let mut builder = SdnBuilder::<(), SC, SE, TC, TW, UserInfo>::new(args.node_id, &[args.bind_addr], args.custom_addrs);
    builder.set_authorization(StaticKeyAuthorization::new(&args.password));

    builder.set_manual_discovery(args.local_tags, args.connect_tags);

    #[cfg(feature = "vpn")]
    if args.vpn {
        builder.enable_vpn();
    }

    let mut controller = match args.backend {
        BackendType::Poll => builder.build::<PollBackend<SdnOwner, 128, 128>>(args.workers, args.node_id),
        BackendType::Polling => builder.build::<PollingBackend<SdnOwner, 128, 128>>(args.workers, args.node_id),
    };

    for seed in args.seeds {
        controller.send_to(0, SdnExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(seed, true))));
    }

    while controller.process().is_some() {
        if term.load(Ordering::Relaxed) {
            if shutdown_wait == 200 {
                log::warn!("Force shutdown");
                break;
            }
            shutdown_wait += 1;
            controller.shutdown();
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    log::info!("Server shutdown");
}
