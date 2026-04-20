use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use jihuan_core::{config::AppConfig, Engine};

mod grpc;
mod http;
mod service;

#[derive(Parser, Debug)]
#[command(name = "jihuan-server", about = "JiHuan storage server")]
struct Args {
    /// Path to the config TOML file
    #[arg(short, long, env = "JIHUAN_CONFIG")]
    config: Option<PathBuf>,

    /// Data directory (overrides config)
    #[arg(long, env = "JIHUAN_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// HTTP listen address (overrides config)
    #[arg(long, env = "JIHUAN_HTTP_ADDR")]
    http_addr: Option<String>,

    /// gRPC listen address (overrides config)
    #[arg(long, env = "JIHUAN_GRPC_ADDR")]
    grpc_addr: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    // Load config
    let mut config = if let Some(cfg_path) = args.config {
        AppConfig::from_file(&cfg_path)?
    } else {
        jihuan_core::config::ConfigTemplate::general(
            args.data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./jihuan-data")),
        )
    };

    // Apply overrides
    if let Some(data_dir) = args.data_dir {
        config.storage.data_dir = data_dir.join("data");
        config.storage.meta_dir = data_dir.join("meta");
        config.storage.wal_dir = data_dir.join("wal");
    }
    if let Some(http_addr) = args.http_addr {
        config.server.http_addr = http_addr;
    }
    if let Some(grpc_addr) = args.grpc_addr {
        config.server.grpc_addr = grpc_addr;
    }

    tracing::info!(
        http_addr = %config.server.http_addr,
        grpc_addr = %config.server.grpc_addr,
        data_dir = %config.storage.data_dir.display(),
        "Starting JiHuan server"
    );

    // Install Prometheus metrics recorder and start the scrape endpoint
    let metrics_addr: SocketAddr = config
        .server
        .metrics_addr
        .parse()
        .unwrap_or_else(|_| "0.0.0.0:9090".parse().unwrap());

    if let Err(e) = PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
    {
        tracing::warn!(error = %e, "Failed to install Prometheus recorder; metrics disabled");
    } else {
        tracing::info!(
            "Prometheus metrics available at http://{}/metrics",
            metrics_addr
        );
    }

    // Open engine (registers metric descriptors)
    let engine = Arc::new(Engine::open(config.clone())?);

    // Start background GC
    let _gc_handle = engine.start_gc();

    // Parse addresses
    let http_addr: SocketAddr = config
        .server
        .http_addr
        .parse()
        .expect("Invalid HTTP address");
    let grpc_addr: SocketAddr = config
        .server
        .grpc_addr
        .parse()
        .expect("Invalid gRPC address");

    // Build HTTP router
    let http_router = http::router(engine.clone())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(tower_http::cors::CorsLayer::permissive());

    // Build gRPC server
    let file_svc = grpc::pb::file_service_server::FileServiceServer::new(
        grpc::file_service::FileServiceImpl::new(engine.clone()),
    );
    let admin_svc = grpc::pb::admin_service_server::AdminServiceServer::new(
        grpc::admin_service::AdminServiceImpl::new(engine.clone()),
    );
    let grpc_server = tonic::transport::Server::builder()
        .add_service(file_svc)
        .add_service(admin_svc);

    tracing::info!("HTTP server listening on {}", http_addr);
    tracing::info!("gRPC server listening on {}", grpc_addr);

    // Run both servers concurrently
    tokio::select! {
        result = axum::serve(
            tokio::net::TcpListener::bind(http_addr).await?,
            http_router,
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP server error");
            }
        },
        result = grpc_server.serve(grpc_addr) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "gRPC server error");
            }
        },
    }

    Ok(())
}
