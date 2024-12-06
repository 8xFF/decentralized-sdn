use atm0s_sdn_identity::{NodeAddr, NodeId};
use atm0s_sdn_network::{
    features::{
        dht_kv::{Control, Key, Map, MapControl},
        FeaturesControl,
    },
    secure::StaticKeyAuthorization,
    services::visualization,
};
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

use atm0s_sdn::{features::neighbours, SdnBuilder, SdnExtIn, SdnOwner};

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

    /// Workers
    #[arg(short, long, default_value_t = 1)]
    workers: usize,

    /// Kv map id
    #[arg(long, default_value_t = 1)]
    kv_map: u64,

    /// This side will subscribe
    #[arg(long)]
    kv_subscribe: bool,

    /// This side will set
    #[arg(long)]
    kv_set: bool,
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
    let mut builder = SdnBuilder::<(), SC, SE, TC, TW, UserInfo>::new(args.node_id, &[args.bind_addr], vec![]);
    builder.set_authorization(StaticKeyAuthorization::new(&args.password));

    let mut controller = match args.backend {
        BackendType::Poll => builder.build::<PollBackend<SdnOwner, 128, 128>>(args.workers, args.node_id),
        BackendType::Polling => builder.build::<PollingBackend<SdnOwner, 128, 128>>(args.workers, args.node_id),
    };

    for seed in args.seeds {
        controller.send_to(0, SdnExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(seed, true))));
    }

    if args.kv_subscribe {
        controller.send_to(0, SdnExtIn::FeaturesControl((), FeaturesControl::DhtKv(Control::MapCmd(Map(args.kv_map), MapControl::Sub))));
    }

    let mut i: u32 = 0;
    while controller.process().is_some() {
        if term.load(Ordering::Relaxed) {
            if shutdown_wait == 200 {
                log::warn!("Force shutdown");
                break;
            }
            shutdown_wait += 1;
            controller.shutdown();
        }
        std::thread::sleep(Duration::from_millis(1));
        i += 1;

        if i % 1000 == 0 && args.kv_set {
            let data = i.to_be_bytes().to_vec();
            log::info!("Set key: {:?}", data);
            controller.send_to(
                0,
                SdnExtIn::FeaturesControl((), FeaturesControl::DhtKv(Control::MapCmd(Map(args.kv_map), MapControl::Set(Key(200), data)))),
            );
        }
        while let Some(out) = controller.pop_event() {
            log::info!("Got event: {:?}", out);
        }
    }

    log::info!("Server shutdown");
}
