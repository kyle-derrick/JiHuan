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
mod tls;

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

    // v0.5.0-iam bootstrap: when auth is enabled, ensure a root user exists.
    // Policy:
    //   • empty metadata → create root user (password from JIHUAN_ROOT_PASSWORD
    //     or auto-generated)
    //   • root user already exists → skip silently
    //   • apikeys table non-empty but users table empty → hard-fail with a
    //     migration message (dev-only: wipe data dir and restart)
    //
    // When auth is disabled every request is accepted with a synthetic
    // full-scope principal — we log a prominent warning.
    if config.auth.enabled {
        let engine_for_bootstrap = engine.clone();
        let data_dir_for_bootstrap = config.storage.meta_dir.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            http::iam::root_bootstrap(&engine_for_bootstrap, &data_dir_for_bootstrap)
        })
        .await
        .map_err(|e| anyhow::anyhow!("root bootstrap task: {e}"))??;
        match outcome {
            http::iam::BootstrapOutcome::Created {
                username,
                raw_password,
            } => {
                let banner = "═".repeat(72);
                eprintln!("\n{banner}");
                eprintln!("  JiHuan bootstrap: no users found — created the root account");
                eprintln!("  username = {username}");
                if !raw_password.is_empty() {
                    eprintln!("  PASSWORD (save now — it will NEVER be shown again):");
                    eprintln!("      {raw_password}");
                } else {
                    eprintln!("  password = (as supplied via JIHUAN_ROOT_PASSWORD)");
                }
                eprintln!(
                    "  Log in at  http://{}/ui/login  with username+password.",
                    config.server.http_addr
                );
                eprintln!("  Change your password later at  /ui/settings  → 修改登录密码.");
                eprintln!("{banner}\n");
                tracing::info!(%username, "Bootstrap root user created");
            }
            http::iam::BootstrapOutcome::Existing => {
                tracing::info!(
                    "Auth enabled; skipping bootstrap (users already present)"
                );
            }
            http::iam::BootstrapOutcome::NeedsMigration(msg) => {
                let banner = "═".repeat(72);
                eprintln!("\n{banner}");
                eprintln!("  JiHuan startup aborted:\n");
                for line in msg.lines() {
                    eprintln!("  {line}");
                }
                eprintln!("{banner}\n");
                return Err(anyhow::anyhow!(
                    "identity schema migration required (v0.4.x → v0.5.0-iam); see stderr"
                ));
            }
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

    // Phase 3 — resolve TLS material once and share it between HTTP and
    // gRPC so operators only manage a single certificate pair. Returns
    // `None` when `tls.enabled = false`, in which case both listeners
    // fall back to plaintext.
    let tls_material = tls::load(&config.tls)
        .map_err(|e| anyhow::anyhow!("TLS setup failed: {:#}", e))?;
    if let Some(ref m) = tls_material {
        let source = if !config.tls.cert_path.trim().is_empty() {
            "static PEM files"
        } else {
            "auto-generated self-signed (dev-only, ephemeral)"
        };
        tracing::info!(
            fingerprint = %m.fingerprint_sha256,
            source = %source,
            "TLS enabled for HTTP + gRPC"
        );
    }

    let scheme = if tls_material.is_some() { "https" } else { "http" };
    tracing::info!("HTTP server listening on {}://{}", scheme, http_addr);
    tracing::info!(
        "gRPC server listening on {}://{}",
        if tls_material.is_some() { "grpcs" } else { "grpc" },
        grpc_addr
    );

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

    // HTTP listener: plaintext via `axum::serve`, or TLS via
    // `axum_server::bind_rustls`. The TLS branch can't reuse our
    // tokio `TcpListener` because axum-server expects to own the
    // accept loop; instead it binds the socket itself from `http_addr`.
    let http_handle = {
        let tls_for_http = tls_material.clone();
        tokio::spawn(async move {
            if let Some(m) = tls_for_http {
                let rustls_cfg = axum_server::tls_rustls::RustlsConfig::from_config(
                    m.server_config.clone(),
                );
                // axum-server's Handle drives graceful shutdown — we
                // call `graceful_shutdown(None)` when the broadcast
                // signal fires, then the serve future completes.
                let handle = axum_server::Handle::new();
                let handle_for_shutdown = handle.clone();
                tokio::spawn(async move {
                    let _ = http_rx.recv().await;
                    handle_for_shutdown.graceful_shutdown(Some(
                        std::time::Duration::from_secs(30),
                    ));
                });
                if let Err(e) = axum_server::bind_rustls(http_addr, rustls_cfg)
                    .handle(handle)
                    .serve(http_router.into_make_service())
                    .await
                {
                    tracing::error!(error = %e, "HTTPS server error");
                }
            } else {
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
            }
        })
    };

    // gRPC listener. tonic 0.12 requires `tls_config` to be applied to
    // the `Server` *before* `add_service`, so we assemble the Router
    // here rather than earlier. Under auto-selfsigned the gRPC cert is
    // regenerated independently from the HTTP one — harmless because
    // neither is persisted and clients don't pin ephemeral certs.
    let grpc_handle = {
        let tls_for_grpc = tls_material.clone();
        let grpc_tls_cfg = config.tls.clone();
        tokio::spawn(async move {
            let mut builder = tonic::transport::Server::builder();
            if tls_for_grpc.is_some() {
                match tls::grpc_identity(&grpc_tls_cfg) {
                    Ok(identity) => {
                        match builder.tls_config(
                            tonic::transport::ServerTlsConfig::new().identity(identity),
                        ) {
                            Ok(b) => builder = b,
                            Err(e) => {
                                tracing::error!(error = %e, "gRPC TLS config failed");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %format!("{e:#}"), "gRPC TLS setup failed");
                        return;
                    }
                }
            }
            let router = builder.add_service(file_svc).add_service(admin_svc);
            if let Err(e) = router
                .serve_with_shutdown(grpc_addr, async move {
                    let _ = grpc_rx.recv().await;
                })
                .await
            {
                tracing::error!(error = %e, "gRPC server error");
            }
        })
    };

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
