//! gRPC authentication interceptor (Phase 2.3).
//!
//! Mirrors the HTTP `auth_middleware`: validates a Bearer / `x-api-key` token
//! against the persistent API-key store and attaches the resolved
//! [`AuthedKey`] to the request extensions. Scope enforcement is done per
//! RPC method via [`require_scope_grpc`] — the interceptor only performs
//! authentication. Session cookies are **not** accepted over gRPC — gRPC
//! callers use raw API keys just like HTTP server-to-server consumers.
//!
//! When `auth.enabled = false`, every request is let through with a synthetic
//! full-scope principal so that downstream scope checks behave identically to
//! the disabled-HTTP path.

use std::sync::Arc;

use tonic::{Request, Status};

use jihuan_core::{config::AuthConfig, Engine};

use crate::http::auth::{disabled_auth_principal, hash_key, AuthedKey};

/// Build a cloneable sync interceptor closure capturing the engine and auth
/// config. The closure is invoked once per inbound gRPC request *before* the
/// service method runs.
///
/// The lookup is a redb read on the tokio worker thread — acceptable because
/// redb is memory-mapped and a single key-hash lookup is microseconds.
pub fn make_interceptor(
    engine: Arc<Engine>,
    config: AuthConfig,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut req: Request<()>| {
        // Auth disabled → inject full-scope synthetic principal and pass through.
        if !config.enabled {
            req.extensions_mut().insert(AuthedKey(disabled_auth_principal()));
            return Ok(req);
        }

        // Extract raw key from metadata. tonic stores HTTP/2 headers as gRPC
        // metadata with lowercased names.
        let raw_key = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer ").map(str::to_string))
            .or_else(|| {
                req.metadata()
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            });

        let raw = match raw_key {
            Some(r) if !r.is_empty() => r,
            _ => {
                return Err(Status::unauthenticated(
                    "missing credentials: provide 'authorization: Bearer <key>' or 'x-api-key: <key>'",
                ));
            }
        };

        let hash = hash_key(&raw);
        let lookup = engine.metadata().get_api_key_by_hash(&hash);
        match lookup {
            Ok(Some(k)) if k.enabled => {
                req.extensions_mut().insert(AuthedKey(k));
                Ok(req)
            }
            Ok(Some(_)) => Err(Status::permission_denied("API key is disabled")),
            Ok(None) => Err(Status::unauthenticated("invalid API key")),
            Err(e) => {
                tracing::warn!(error = %e, "gRPC auth: metadata store error");
                Err(Status::internal("auth backend error"))
            }
        }
    }
}

/// Per-RPC scope check. Call at the top of every service method that requires
/// non-`read` capability. Returns `PermissionDenied` when the key lacks the
/// scope, or `Internal` if the interceptor did not attach a principal (a
/// programming error — should not happen in practice).
pub fn require_scope_grpc<T>(req: &Request<T>, required: &str) -> Result<(), Status> {
    let authed = req
        .extensions()
        .get::<AuthedKey>()
        .ok_or_else(|| Status::internal("auth extension missing; interceptor mis-wired"))?;
    if authed.has_scope(required) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "insufficient scope: need '{}'",
            required
        )))
    }
}
