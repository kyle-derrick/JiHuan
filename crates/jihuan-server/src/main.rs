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
#[command(name = "jihuan-server", version, about = "JiHuan storage server")]
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
    // v0.4.4: start background auto-compaction loop if enabled in config.
    // When disabled, this is a no-op (returns None) and the handle is dropped.
    let _compaction_handle = engine.start_auto_compaction();

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
    // Parse listener addresses; a misconfigured address is a fatal
    // startup error — bubble it up with context so operators see the
    // offending string instead of a terse `.expect` panic.
    let http_addr: SocketAddr = config
        .server
        .http_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid http_addr {:?}: {}", config.server.http_addr, e))?;
    let grpc_addr: SocketAddr = config
        .server
        .grpc_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid grpc_addr {:?}: {}", config.server.grpc_addr, e))?;

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

    // Build gRPC server with auth interceptor (Phase 2.3).
    // The interceptor closure runs before every RPC method, validates the
    // API key in request metadata, and attaches an `AuthedKey` extension
    // that handlers read via `require_scope_grpc`. When auth is disabled
    // it injects a synthetic full-scope principal so the same `require_*`
    // calls keep compiling without per-method branching.
    let grpc_interceptor =
        grpc::auth_interceptor::make_interceptor(engine.clone(), config.auth.clone());
    let file_svc = grpc::pb::file_service_server::FileServiceServer::with_interceptor(
        grpc::file_service::FileServiceImpl::new(engine.clone()),
        grpc_interceptor.clone(),
    );
    let admin_svc = grpc::pb::admin_service_server::AdminServiceServer::with_interceptor(
        grpc::admin_service::AdminServiceImpl::new(engine.clone()),
        grpc_interceptor,
    );
    let grpc_server = tonic::transport::Server::builder()
        .add_service(file_svc)
        .add_service(admin_svc);

    tracing::info!("HTTP server listening on {}", http_addr);
    tracing::info!("gRPC server listening on {}", grpc_addr);

    // ── Graceful shutdown ────────────────────────────────────────────────────
    //
    // Signals (Ctrl-C on Windows, SIGINT / SIGTERM on Unix) otherwise kill the
    // process immediately — `Arc<Engine>` copies held in handlers never drop,
    // `Engine::Drop` never runs, and the active block stays unsealed. On next
    // start, `cleanup_incomplete_blocks` used to delete it → catastrophic
    // data loss. We now install a signal listener, drive both servers under
    // `with_graceful_shutdown`, and call `Engine::shutdown()` explicitly
    // **before** returning so the block is sealed and the WAL is fsynced.
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGTERM handler; Ctrl-C only");
                    tokio::signal::ctrl_c().await.ok();
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
                _ = term.recv() => tracing::info!("received SIGTERM"),
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %e, "failed to wait for Ctrl-C");
            } else {
                tracing::info!("received Ctrl-C");
            }
        }
    };

    // Two separate oneshot halves so each server can be driven by its own
    // graceful-shutdown adapter and we can await them concurrently.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut http_rx = shutdown_tx.subscribe();
    let mut grpc_rx = shutdown_tx.subscribe();
    let shutdown_tx_signal = shutdown_tx.clone();

    let http_handle = tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(http_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "HTTP bind failed");
                return;
            }
        };
        if let Err(e) = axum::serve(listener, http_router)
            .with_graceful_shutdown(async move {
                let _ = http_rx.recv().await;
            })
            .await
        {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = grpc_server
            .serve_with_shutdown(grpc_addr, async move {
                let _ = grpc_rx.recv().await;
            })
            .await
        {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // Block on signal, then fan out shutdown to both servers.
    shutdown_signal.await;
    tracing::info!("Graceful shutdown initiated");
    let _ = shutdown_tx_signal.send(());

    // Wait for both servers to drain. The timeout guards against a stuck
    // long-poll request — after 30 s we proceed with engine shutdown
    // regardless so we never leave the block unsealed.
    let drain = async {
        let _ = tokio::join!(http_handle, grpc_handle);
    };
    if tokio::time::timeout(std::time::Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tracing::warn!("Server drain timed out after 30s; proceeding with engine shutdown");
    }

    // CRITICAL: seal the active block and fsync the WAL. Skipping this is
    // what caused the historical data-loss-on-restart bug.
    engine.shutdown();
    tracing::info!("Shutdown complete");

    Ok(())
}
