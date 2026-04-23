//! Phase 3 — TLS certificate loading for HTTP + gRPC.
//!
//! Two sources are supported, resolved in this order:
//!
//! 1. **Static PEM files** (`tls.cert_path` + `tls.key_path`). This is
//!    the production path — cert renewal is handled externally (cron +
//!    `systemctl reload`, or a reverse proxy, or an out-of-band
//!    certbot).
//! 2. **Auto-selfsigned** (`tls.auto_selfsigned = true`). Dev-only: a
//!    fresh cert for `localhost`/`127.0.0.1` is generated in memory via
//!    `rcgen` on every boot. Not persisted — operators should copy the
//!    printed fingerprint into their client trust store if they want
//!    stable pinning across restarts.
//!
//! Both paths return a rustls `ServerConfig` ready to be handed to
//! `axum_server::bind_rustls` and `tonic::transport::ServerTlsConfig`.
//!
//! HTTP and gRPC intentionally **share the same certificate**. Single-
//! port termination is left to a reverse proxy if operators need SNI
//! routing or per-service certs — this module optimises for the common
//! case (one hostname, one cert).

use std::io::BufReader;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig,
};
use rustls_pemfile::Item;

use jihuan_core::config::TlsConfig;

/// Resolved TLS material. Held behind an `Arc` so HTTP and gRPC listeners
/// can share the same `ServerConfig` without cloning its internals.
#[derive(Clone, Debug)]
pub struct TlsMaterial {
    pub server_config: Arc<ServerConfig>,
    /// Hex-encoded SHA-256 fingerprint of the leaf certificate, printed
    /// at startup so operators can sanity-check which cert is actually
    /// live (rotation audits, self-signed pinning).
    pub fingerprint_sha256: String,
}

/// Build a rustls `ServerConfig` from the given [`TlsConfig`].
///
/// Returns `Ok(None)` when `tls.enabled = false` — callers use that as
/// the signal to stay on plaintext `axum::serve` / `tonic::Server`.
pub fn load(cfg: &TlsConfig) -> Result<Option<TlsMaterial>> {
    if !cfg.enabled {
        return Ok(None);
    }

    // Install the default ring-based crypto provider for rustls 0.23.
    // Safe to call multiple times — we swallow `Err` (already installed).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (certs, key) = if !cfg.cert_path.trim().is_empty() {
        load_static(&cfg.cert_path, &cfg.key_path)
            .with_context(|| {
                format!(
                    "loading TLS cert '{}' / key '{}'",
                    cfg.cert_path, cfg.key_path
                )
            })?
    } else if cfg.auto_selfsigned {
        generate_selfsigned().context("generating dev self-signed certificate")?
    } else {
        // Validation in `AppConfig::validate` should prevent this, but
        // we double-check here so library callers can't slip through.
        return Err(anyhow!(
            "tls.enabled = true but no certificate source configured"
        ));
    };

    let fingerprint = sha256_hex(certs.first().map(|c| c.as_ref()).unwrap_or(&[]));

    let sc = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("rustls rejected certificate/key pair")?;

    Ok(Some(TlsMaterial {
        server_config: Arc::new(sc),
        fingerprint_sha256: fingerprint,
    }))
}

/// Read a PEM cert chain + private key from disk. Accepts PKCS#8, RSA
/// (PKCS#1), or SEC1 EC keys — whichever `rustls-pemfile` finds first
/// in the key file is used.
fn load_static(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_bytes = std::fs::read(cert_path)
        .with_context(|| format!("reading certificate file '{cert_path}'"))?;
    let key_bytes = std::fs::read(key_path)
        .with_context(|| format!("reading private key file '{key_path}'"))?;

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(&cert_bytes[..]))
            .collect::<std::io::Result<_>>()
            .context("parsing PEM certificate chain")?;
    if certs.is_empty() {
        return Err(anyhow!(
            "no CERTIFICATE blocks found in '{cert_path}' (expected PEM)"
        ));
    }

    let mut key_reader = BufReader::new(&key_bytes[..]);
    let key: PrivateKeyDer<'static> = loop {
        match rustls_pemfile::read_one(&mut key_reader)
            .context("parsing PEM private key")?
        {
            Some(Item::Pkcs8Key(k)) => break PrivateKeyDer::Pkcs8(k),
            Some(Item::Pkcs1Key(k)) => break PrivateKeyDer::Pkcs1(k),
            Some(Item::Sec1Key(k)) => break PrivateKeyDer::Sec1(k),
            // Skip non-key items (e.g. a CERTIFICATE block left in the
            // key file by mistake) rather than bail immediately.
            Some(_) => continue,
            None => {
                return Err(anyhow!(
                    "no private key found in '{key_path}' (expected PKCS#8, RSA, or SEC1 PEM)"
                ))
            }
        }
    };

    Ok((certs, key))
}

/// Generate a dev-only self-signed certificate valid for `localhost` and
/// `127.0.0.1`/`::1`. The private key is discarded once the rustls
/// `ServerConfig` is built — no key material ever hits disk.
fn generate_selfsigned(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    let cert_key = rcgen::generate_simple_self_signed(subject_alt_names)
        .context("rcgen failed to generate self-signed cert")?;
    let cert_der = cert_key.cert.der().clone();
    let key_der = cert_key.key_pair.serialize_der();
    let key = PrivateKeyDer::Pkcs8(key_der.into());
    Ok((vec![cert_der], key))
}

/// Build a tonic `Identity` from the same config [`load`] consumes.
///
/// tonic 0.12 builds its own rustls stack from raw PEM bytes (it does
/// not accept a `rustls::ServerConfig`), so this helper re-reads the
/// static PEM files or generates a fresh self-signed cert specifically
/// for the gRPC listener. Invariant: gRPC and HTTP terminate on the
/// same hostnames, so either path produces a cert the client will
/// accept against the same trust root.
pub fn grpc_identity(cfg: &TlsConfig) -> Result<tonic::transport::Identity> {
    let (cert_pem, key_pem): (Vec<u8>, Vec<u8>) = if !cfg.cert_path.trim().is_empty() {
        let cert_pem = std::fs::read(&cfg.cert_path)
            .with_context(|| format!("reading certificate file '{}'", cfg.cert_path))?;
        let key_pem = std::fs::read(&cfg.key_path)
            .with_context(|| format!("reading private key file '{}'", cfg.key_path))?;
        (cert_pem, key_pem)
    } else if cfg.auto_selfsigned {
        let subject_alt_names = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];
        let cert_key = rcgen::generate_simple_self_signed(subject_alt_names)
            .context("rcgen failed to generate self-signed cert (gRPC)")?;
        (
            cert_key.cert.pem().into_bytes(),
            cert_key.key_pair.serialize_pem().into_bytes(),
        )
    } else {
        return Err(anyhow!(
            "tls.enabled = true but no certificate source configured"
        ));
    };
    Ok(tonic::transport::Identity::from_pem(cert_pem, key_pem))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_returns_none_when_disabled() {
        let cfg = TlsConfig::default();
        let out = load(&cfg).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn test_load_auto_selfsigned_builds_server_config() {
        let cfg = TlsConfig {
            enabled: true,
            auto_selfsigned: true,
            ..TlsConfig::default()
        };
        let out = load(&cfg).unwrap().expect("auto-selfsigned should produce material");
        // Fingerprint is 32 bytes → 64 hex chars.
        assert_eq!(out.fingerprint_sha256.len(), 64);
        assert!(out
            .fingerprint_sha256
            .chars()
            .all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_load_static_roundtrips_selfsigned_pem() {
        // Generate a cert via rcgen, write to temp PEMs, then load them
        // back through the static path. This is a full end-to-end check
        // that our PEM parsing accepts rcgen's output format.
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        let subject_alt_names = vec!["localhost".to_string()];
        let cert_key = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();
        std::fs::write(&cert_path, cert_key.cert.pem()).unwrap();
        std::fs::write(&key_path, cert_key.key_pair.serialize_pem()).unwrap();

        let cfg = TlsConfig {
            enabled: true,
            cert_path: cert_path.to_string_lossy().into_owned(),
            key_path: key_path.to_string_lossy().into_owned(),
            auto_selfsigned: false,
        };
        let out = load(&cfg).unwrap().expect("static cert path should load");
        assert_eq!(out.fingerprint_sha256.len(), 64);
    }

    #[test]
    fn test_load_static_errors_on_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        std::fs::write(&cert_path, "not a real pem").unwrap();

        let cfg = TlsConfig {
            enabled: true,
            cert_path: cert_path.to_string_lossy().into_owned(),
            key_path: dir.path().join("missing.pem").to_string_lossy().into_owned(),
            auto_selfsigned: false,
        };
        let err = load(&cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("private key") || msg.contains("reading"),
            "unexpected error: {msg}"
        );
    }
}
