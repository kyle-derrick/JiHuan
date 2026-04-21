use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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

    // Install Prometheus metrics recorder. We always create a recorder so that
    // counters/histograms registered by the engine have a sink; we also expose
    // the rendered text via an in-process handle (shared with the HTTP router
    // for same-origin `/api/metrics`).
    //
    // If `server.metrics_addr` is non-empty and parses, we additionally bind
    // a standalone `:9090/metrics` listener for Prometheus direct-scrape.
    // Set `metrics_addr = ""` in config to disable the standalone listener
    // (UI still has access via same-origin `/api/metrics`).
    let metrics_addr_str = config.server.metrics_addr.trim().to_string();
    let metrics_handle: Option<PrometheusHandle> = {
        let maybe_addr: Option<SocketAddr> = if metrics_addr_str.is_empty() {
            None
        } else {
            match metrics_addr_str.parse() {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        addr = %metrics_addr_str,
                        "Invalid metrics_addr; standalone listener disabled",
                    );
                    None
                }
            }
        };

        let build_result: Result<(metrics_exporter_prometheus::PrometheusRecorder, Option<_>), _> =
            if let Some(addr) = maybe_addr {
                PrometheusBuilder::new()
                    .with_http_listener(addr)
                    .build()
                    .map(|(rec, exporter)| (rec, Some(exporter)))
            } else {
                Ok((PrometheusBuilder::new().build_recorder(), None))
            };

        match build_result {
            Ok((recorder, exporter)) => {
                let handle = recorder.handle();
                if let Err(e) = metrics::set_global_recorder(recorder) {
                    tracing::warn!(error = %e, "Failed to set global metrics recorder");
                    None
                } else {
                    if let Some(exporter) = exporter {
                        tokio::spawn(async move {
                            if let Err(e) = exporter.await {
                                tracing::warn!(error = ?e, "Prometheus exporter exited");
                            }
                        });
                    }
                    if let Some(addr) = maybe_addr {
                        tracing::info!(
                            "Prometheus metrics available at http://{}/metrics (and same-origin /api/metrics)",
                            addr
                        );
                    } else {
                        tracing::info!(
                            "Prometheus standalone listener disabled; metrics available at same-origin /api/metrics only"
                        );
                    }
                    Some(handle)
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to build Prometheus recorder; metrics disabled");
                None
            }
        }
    };

    // Pin process start time for uptime reporting
    http::admin::init_start_time();

    // Open engine (registers metric descriptors)
    let engine = Arc::new(Engine::open(config.clone())?);

    // Start background GC
    let _gc_handle = engine.start_gc();

    // Bootstrap admin key. Policy: when auth is enabled, ensure at least one
    // enabled admin-scoped key exists. If not, mint one and print it to stderr
    // (one-shot). This covers three cases:
    //   • first boot, empty metadata → bootstrap fires
    //   • metadata has keys but none with `admin` → user locked themselves
    //     out (e.g. revoked their only admin key); bootstrap gives them a
    //     recovery credential
    //   • admin key already present → skip silently
    //
    // The decision is logged at INFO so operators can see why no banner
    // appears on subsequent boots. (Phase 2.1, updated Phase 2.2)
    if config.auth.enabled {
        let engine_for_bootstrap = engine.clone();
        let existing = tokio::task::spawn_blocking(move || {
            engine_for_bootstrap.metadata().list_api_keys()
        })
        .await
        .map_err(|e| anyhow::anyhow!("bootstrap admin key task: {e}"))?
        .map_err(|e| anyhow::anyhow!("bootstrap admin key list: {e}"))?;

        let total = existing.len();
        let admin_count = existing
            .iter()
            .filter(|k| k.enabled && k.scopes.iter().any(|s| s == "admin"))
            .count();

        if admin_count == 0 {
            let (raw_key, meta, _now) = http::auth::build_new_key(
                "bootstrap-admin",
                vec!["read".to_string(), "write".to_string(), "admin".to_string()],
            );
            let key_id = meta.key_id.clone();
            let engine_for_insert = engine.clone();
            tokio::task::spawn_blocking(move || {
                engine_for_insert.metadata().insert_api_key(&meta)
            })
            .await
            .map_err(|e| anyhow::anyhow!("bootstrap admin key insert task: {e}"))?
            .map_err(|e| anyhow::anyhow!("bootstrap admin key insert: {e}"))?;

            // Print prominently so the first-run operator cannot miss it.
            let banner = "═".repeat(72);
            eprintln!("\n{banner}");
            if total == 0 {
                eprintln!("  JiHuan bootstrap: no API keys found — creating initial admin key");
            } else {
                eprintln!("  JiHuan bootstrap: no admin-scoped key among {total} existing keys");
                eprintln!("  → minting a recovery admin key");
            }
            eprintln!("  key_id = {key_id}");
            eprintln!("  API KEY (save now — it will NEVER be shown again):");
            eprintln!("      {raw_key}");
            eprintln!("  Log in at  http://{}/ui/login  with this value as the password.",
                config.server.http_addr);
            eprintln!("  You can change the password later at  /ui/settings  → 修改登录密码.");
            eprintln!("{banner}\n");
            tracing::info!(%key_id, total, "Bootstrap admin key created");
        } else {
            tracing::info!(
                total_keys = total,
                admin_keys = admin_count,
                "Auth enabled; skipping bootstrap (admin key already present)"
            );
        }
    } else {
        tracing::warn!("Authentication is disabled; every request is accepted");
    }

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

    // Build CORS layer from config. Empty origins list ⇒ same-origin only
    // (no CORS headers emitted). "*" ⇒ dev-friendly `Any`. Otherwise each
    // entry is treated as an explicit allow-listed origin.
    let cors_layer = {
        use tower_http::cors::{AllowOrigin, CorsLayer};
        let origins = &config.server.cors_origins;
        if origins.is_empty() {
            // Still need a CorsLayer so that preflight OPTIONS are handled
            // uniformly — but with no allowed origin, the browser will refuse
            // cross-site usage. Same-origin XHR/fetch is unaffected.
            CorsLayer::new()
        } else if origins.iter().any(|o| o == "*") {
            CorsLayer::permissive()
        } else {
            let parsed: Vec<axum::http::HeaderValue> = origins
                .iter()
                .filter_map(|o| o.parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(parsed))
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        }
    };

    // Build HTTP router
    let http_router = http::router(engine.clone(), metrics_handle.clone())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(cors_layer);

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
