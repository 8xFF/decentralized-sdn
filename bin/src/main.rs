#![allow(clippy::bool_assert_comparison)]

use atm0s_sdn::features::{neighbours, router_sync, FeaturesControl, FeaturesEvent};
use atm0s_sdn::secure::StaticKeyAuthorization;
use atm0s_sdn::services::manual2_discovery::AdvertiseTarget;
use atm0s_sdn::services::visualization;
use atm0s_sdn::{
    sans_io_runtime::backend::{PollBackend, PollingBackend},
    services::visualization::ConnectionInfo,
};
use atm0s_sdn::{NodeAddr, NodeId, SdnControllerUtils, SdnExtIn, ServiceBroadcastLevel};
use atm0s_sdn::{SdnBuilder, SdnExtOut, SdnOwner};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
#[cfg(not(feature = "embed"))]
use poem::endpoint::StaticFilesEndpoint;
#[cfg(feature = "embed")]
use poem::endpoint::{EmbeddedFileEndpoint, EmbeddedFilesEndpoint};
use poem::web::Json;
use poem::{
    get, handler,
    listener::TcpListener,
    web::{
        websocket::{Message, WebSocket},
        Data,
    },
    EndpointExt, IntoResponse, Route, Server,
};
#[cfg(feature = "embed")]
use rust_embed::RustEmbed;

use serde::{Deserialize, Serialize};
use std::time::Instant;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::{oneshot, Mutex};

#[cfg(feature = "embed")]
#[derive(RustEmbed)]
#[folder = "public"]
pub struct Files;

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
    #[arg(env, short, long, default_value_t = 0)]
    node_id: NodeId,

    /// Listen address
    #[arg(env, short, long, default_value_t = 10000)]
    udp_port: u16,

    /// Address of node we should connect to
    #[arg(env, short, long)]
    seeds: Vec<NodeAddr>,

    /// Password for the network
    #[arg(env, short, long, default_value = "password")]
    password: String,

    /// Backend type
    #[arg(env, long, default_value = "polling")]
    backend: BackendType,

    /// Enable Tun-Tap interface for create a vpn network between nodes
    #[arg(env, short, long)]
    vpn: bool,

    /// Workers
    #[arg(env, long, default_value_t = 2)]
    workers: usize,

    /// Custom IP
    #[arg(env, long)]
    custom_addrs: Vec<SocketAddr>,

    /// Web server addr
    #[arg(env, long, default_value = "0.0.0.0:3000")]
    web_addr: SocketAddr,

    /// Collector node, which will have UI for monitoring network structure
    #[arg(env, long)]
    collector: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VisualNodeInfo {
    uptime: u32,
}
type SC = visualization::Control<VisualNodeInfo>;
type SE = visualization::Event<VisualNodeInfo>;
type TC = ();
type TW = ();

#[derive(Debug, Clone, Serialize)]
struct ConnInfo {
    uuid: u64,
    outgoing: bool,
    dest: NodeId,
    remote: SocketAddr,
    rtt_ms: u32,
}

#[derive(Debug, Clone, Serialize)]
struct NodeInfo {
    id: NodeId,
    info: VisualNodeInfo,
    connections: Vec<ConnInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
enum WebsocketMessage {
    Snapshot(Vec<NodeInfo>),
    Update(NodeInfo),
    Delete(NodeId),
}

#[derive(Debug)]
struct WebsocketCtx {
    channel: tokio::sync::broadcast::Sender<WebsocketMessage>,
    snapshot: HashMap<NodeId, (VisualNodeInfo, Vec<ConnInfo>)>,
}

impl WebsocketCtx {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(10);
        Self {
            channel: tx,
            snapshot: HashMap::new(),
        }
    }

    pub fn set_snapshot(&mut self, snapshot: Vec<(NodeId, VisualNodeInfo, Vec<ConnectionInfo>)>) {
        self.snapshot.clear();
        for (id, info, connections) in snapshot {
            let conns = connections
                .into_iter()
                .map(|c| ConnInfo {
                    uuid: c.conn.session(),
                    outgoing: c.conn.is_outgoing(),
                    dest: c.dest,
                    remote: c.remote,
                    rtt_ms: c.rtt_ms,
                })
                .collect();
            self.snapshot.insert(id, (info, conns));
        }
    }

    pub fn set_node(&mut self, delta: (NodeId, VisualNodeInfo, Vec<ConnectionInfo>)) {
        let connections: Vec<ConnInfo> = delta
            .2
            .into_iter()
            .map(|c| ConnInfo {
                uuid: c.conn.session(),
                outgoing: c.conn.is_outgoing(),
                dest: c.dest,
                remote: c.remote,
                rtt_ms: c.rtt_ms,
            })
            .collect();
        self.snapshot.insert(delta.0, (delta.1.clone(), connections.clone()));
        if let Err(e) = self.channel.send(WebsocketMessage::Update(NodeInfo {
            id: delta.0,
            info: delta.1,
            connections,
        })) {
            log::debug!("Failed to send delta: {}", e);
        }
    }

    pub fn del_node(&mut self, id: NodeId) {
        self.snapshot.remove(&id);
        if let Err(e) = self.channel.send(WebsocketMessage::Delete(id)) {
            log::error!("Failed to send delta: {}", e);
        }
    }

    pub fn get_snapshot(&self) -> Vec<NodeInfo> {
        self.snapshot
            .iter()
            .map(|(id, (info, connections))| NodeInfo {
                id: *id,
                info: info.clone(),
                connections: connections.clone(),
            })
            .collect()
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WebsocketMessage> {
        self.channel.subscribe()
    }
}

#[handler]
fn ws(ws: WebSocket, ctx: Data<&Arc<Mutex<WebsocketCtx>>>) -> impl IntoResponse {
    let ctx = ctx.clone();
    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut _stream) = socket.split();
        let ctx = ctx.lock().await;
        let snapshot = ctx.get_snapshot();
        if let Err(e) = sink
            .send(Message::Text(serde_json::to_string(&WebsocketMessage::Snapshot(snapshot)).expect("should convert json")))
            .await
        {
            log::error!("Failed to send snapshot: {}", e);
            return;
        }
        let mut rx = ctx.subscribe();
        drop(ctx);
        while let Ok(event) = rx.recv().await {
            if let Err(e) = sink.send(Message::Text(serde_json::to_string(&event).expect("should convert json"))).await {
                log::error!("Failed to send delta: {}", e);
                return;
            }
        }
    })
}

#[handler]
async fn dump_router(ctx: Data<&UnboundedSender<oneshot::Sender<serde_json::Value>>>) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    ctx.0.send(tx).expect("should send");
    match tokio::time::timeout(Duration::from_millis(1000), rx).await {
        Ok(Ok(v)) => Json(serde_json::json!({
            "status": true,
            "data": v
        })),
        Ok(Err(e)) => Json(serde_json::json!({
            "status": false,
            "error": e.to_string()
        })),
        Err(_e) => Json(serde_json::json!({
            "status": false,
            "error": "timeout"
        })),
    }
}

#[allow(clippy::collapsible_match)]
#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term)).expect("Should register hook");
    let mut shutdown_wait = 0;
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let addrs = vec![SocketAddr::new(local_ip_address::local_ip().expect("Should have list interfaces"), args.udp_port)];
    let mut builder = SdnBuilder::<(), SC, SE, TC, TW, VisualNodeInfo>::new(args.node_id, &addrs, args.custom_addrs);

    builder.set_authorization(StaticKeyAuthorization::new(&args.password));
    builder.set_manual2_discovery(vec![AdvertiseTarget::new(2.into(), ServiceBroadcastLevel::Global)], 1000);

    if args.vpn {
        builder.enable_vpn();
    }

    builder.set_visualization_collector(args.collector);

    let node_info = VisualNodeInfo { uptime: 0 };
    let mut controller = match args.backend {
        BackendType::Poll => builder.build::<PollBackend<SdnOwner, 128, 128>>(args.workers, node_info),
        BackendType::Polling => builder.build::<PollingBackend<SdnOwner, 128, 128>>(args.workers, node_info),
    };

    for seed in args.seeds.iter() {
        controller.send_to(0, SdnExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(seed.clone(), true))));
    }

    let (dump_tx, mut dump_rx) = unbounded_channel::<oneshot::Sender<serde_json::Value>>();
    let ctx = Arc::new(Mutex::new(WebsocketCtx::new()));

    if args.collector {
        controller.service_control(visualization::SERVICE_ID.into(), (), visualization::Control::Subscribe);
        let ctx_c = ctx.clone();
        tokio::spawn(async move {
            let route = Route::new().at("/dump_router", get(dump_router).data(dump_tx)).at("/ws", get(ws.data(ctx_c)));

            #[cfg(not(feature = "embed"))]
            let route = route.nest("/", StaticFilesEndpoint::new("./public/").index_file("index.html"));

            #[cfg(feature = "embed")]
            let route = route.at("/", EmbeddedFileEndpoint::<Files>::new("index.html"));
            #[cfg(feature = "embed")]
            let route = route.nest("/", EmbeddedFilesEndpoint::<Files>::new());

            Server::new(TcpListener::bind(args.web_addr)).run(route).await
        });
    }

    let started_at = Instant::now();
    let mut count = 0;
    let mut wait_dump_router = vec![];
    while controller.process().is_some() {
        if term.load(Ordering::Relaxed) {
            if shutdown_wait == 200 {
                log::warn!("Force shutdown");
                break;
            }
            shutdown_wait += 1;
            controller.shutdown();
        }
        if count % 500 == 0 {
            //each 5 seconds
            controller.service_control(
                visualization::SERVICE_ID.into(),
                (),
                visualization::Control::UpdateInfo(VisualNodeInfo {
                    uptime: started_at.elapsed().as_secs() as u32,
                }),
            );
        }

        while let Ok(v) = dump_rx.try_recv() {
            controller.feature_control((), router_sync::Control::DumpRouter.into());
            wait_dump_router.push(v);
        }
        while let Some(event) = controller.pop_event() {
            match event {
                SdnExtOut::ServicesEvent(_service, (), event) => match event {
                    visualization::Event::GotAll(all) => {
                        log::info!("Got all: {:?}", all);
                        ctx.lock().await.set_snapshot(all);
                    }
                    visualization::Event::NodeChanged(node, info, changed) => {
                        log::debug!("Node changed: {:?} {:?}", node, changed);
                        ctx.lock().await.set_node((node, info, changed));
                    }
                    visualization::Event::NodeRemoved(node) => {
                        log::info!("Node removed: {:?}", node);
                        ctx.lock().await.del_node(node);
                    }
                },
                SdnExtOut::FeaturesEvent(_, event) => match event {
                    FeaturesEvent::RouterSync(event) => match event {
                        router_sync::Event::DumpRouter(value) => {
                            let json = serde_json::to_value(value).expect("should convert json");
                            while let Some(v) = wait_dump_router.pop() {
                                let _ = v.send(json.clone());
                            }
                        }
                    },
                    FeaturesEvent::Neighbours(event) => {
                        if let neighbours::Event::SeedAddressNeeded = event {
                            for seed in args.seeds.iter() {
                                controller.send_to(0, SdnExtIn::FeaturesControl((), FeaturesControl::Neighbours(neighbours::Control::ConnectTo(seed.clone(), true))));
                            }
                        }
                    }
                    _ => {}
                },
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        count += 1;
    }

    log::info!("Server shutdown");
}
