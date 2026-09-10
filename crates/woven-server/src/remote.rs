//! Opt-in, static scoped-credential composition; not hosted tenant authentication.

use crate::{ServerError, router_with_transports, scoped_core};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use woven_core::TransportIndependentWorker;
use woven_transport::spawn_worker;
use woven_transport_quic::{QuicConfig, serve_endpoint, server_config, server_endpoint};

/// Native QUIC with operator-supplied TLS and one static scoped credential.
/// No default: every remote data-plane setting must be explicit.
#[derive(Clone, Debug)]
pub struct RemoteServerConfig {
    pub quic_bind_address: SocketAddr,
    /// Unauthenticated HTTP health/metrics/capabilities. Must be loopback.
    pub management_bind_address: SocketAddr,
    /// PEM certificate chain, leaf first (at most 1 MiB).
    pub certificate_file: PathBuf,
    /// PEM private key matching the leaf certificate (at most 1 MiB).
    pub private_key_file: PathBuf,
    /// UTF-8 token file; 32–4096 non-whitespace ASCII bytes, optional trailing newline.
    /// Grants principal 1 access to namespace/session 1, spaces 1/2, channels 1/2 only.
    pub auth_token_file: PathBuf,
}

impl RemoteServerConfig {
    /// Explicit opt-in via `WOVEN_REMOTE_QUIC=1`. All five other variables are required:
    /// `WOVEN_QUIC_BIND`, `WOVEN_MANAGEMENT_BIND`, `WOVEN_TLS_CERT_FILE`,
    /// `WOVEN_TLS_KEY_FILE`, `WOVEN_AUTH_TOKEN_FILE`. Partial configuration fails closed.
    pub fn from_env() -> Result<Option<Self>, ServerError> {
        for name in [
            "WOVEN_REMOTE_QUIC",
            "WOVEN_QUIC_BIND",
            "WOVEN_MANAGEMENT_BIND",
            "WOVEN_TLS_CERT_FILE",
            "WOVEN_TLS_KEY_FILE",
            "WOVEN_AUTH_TOKEN_FILE",
        ] {
            if std::env::var_os(name).is_some_and(|value| value.into_string().is_err()) {
                return Err(invalid("remote configuration variables must be UTF-8"));
            }
        }
        Self::from_lookup(|name| {
            std::env::var(name)
                .map_err(|_| invalid("missing or non-UTF-8 remote configuration variable"))
        })
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Result<String, ServerError>,
    ) -> Result<Option<Self>, ServerError> {
        const SETTINGS: [&str; 5] = [
            "WOVEN_QUIC_BIND",
            "WOVEN_MANAGEMENT_BIND",
            "WOVEN_TLS_CERT_FILE",
            "WOVEN_TLS_KEY_FILE",
            "WOVEN_AUTH_TOKEN_FILE",
        ];
        match lookup("WOVEN_REMOTE_QUIC") {
            Ok(value) if value == "1" => {}
            Ok(_) => return Err(invalid("WOVEN_REMOTE_QUIC must be exactly 1 when set")),
            Err(_) => {
                if SETTINGS.iter().any(|name| lookup(name).is_ok()) {
                    return Err(invalid("remote settings require WOVEN_REMOTE_QUIC=1"));
                }
                return Ok(None);
            }
        }
        let config = Self {
            quic_bind_address: lookup(SETTINGS[0])?
                .parse()
                .map_err(|_| invalid("invalid WOVEN_QUIC_BIND socket address"))?,
            management_bind_address: lookup(SETTINGS[1])?
                .parse()
                .map_err(|_| invalid("invalid WOVEN_MANAGEMENT_BIND socket address"))?,
            certificate_file: lookup(SETTINGS[2])?.into(),
            private_key_file: lookup(SETTINGS[3])?.into(),
            auth_token_file: lookup(SETTINGS[4])?.into(),
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), ServerError> {
        if !self.management_bind_address.ip().is_loopback() {
            return Err(invalid("management HTTP must bind a loopback address"));
        }
        if [
            &self.certificate_file,
            &self.private_key_file,
            &self.auth_token_file,
        ]
        .iter()
        .any(|p| p.as_os_str().is_empty())
        {
            return Err(invalid("TLS and credential file paths must not be empty"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> ServerError {
    ServerError::QuicConfiguration(message.to_owned())
}

fn read_bounded(path: &Path, limit: usize, secret: bool) -> Result<Vec<u8>, ServerError> {
    if !std::fs::metadata(path)?.is_file() {
        return Err(invalid("TLS and credential inputs must be regular files"));
    }
    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid("TLS and credential inputs must be regular files"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if secret && metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "private key and credential files must deny group/other permissions (use mode 0600 or 0400)",
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = secret;
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("TLS or credential file exceeds size limit"));
    }
    Ok(bytes)
}

fn validate_token(bytes: &[u8]) -> Result<&str, ServerError> {
    let token = std::str::from_utf8(bytes)
        .map_err(|_| invalid("credential file must be UTF-8"))?
        .trim_end_matches(['\r', '\n']);
    if !(32..=4096).contains(&token.len()) || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(invalid(
            "credential must contain 32–4096 non-whitespace ASCII bytes",
        ));
    }
    Ok(token)
}

/// Running remote composition. Dropping it closes QUIC and aborts listener tasks.
/// Keep this handle alive for as long as clients should be served.
pub struct RemoteServer {
    pub quic_address: SocketAddr,
    pub management_address: SocketAddr,
    endpoint: quinn::Endpoint,
    quic_task: tokio::task::JoinHandle<()>,
    http_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for RemoteServer {
    fn drop(&mut self) {
        self.endpoint.close(0_u32.into(), b"server shutdown");
        self.quic_task.abort();
        self.http_task.abort();
    }
}

/// Validate and load all credentials before binding; provision the fixed session explicitly.
/// WebTransport and inference are disabled, with no development/AI token installed.
pub async fn start_remote(config: RemoteServerConfig) -> Result<RemoteServer, ServerError> {
    config.validate()?;
    let cert_bytes = read_bounded(&config.certificate_file, 1024 * 1024, false)?;
    let key_bytes = read_bounded(&config.private_key_file, 1024 * 1024, true)?;
    let token_bytes = read_bounded(&config.auth_token_file, 4098, true)?;
    let token = validate_token(&token_bytes)?;
    let certificates = CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid("invalid TLS certificate PEM"))?;
    let key = PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(|_| invalid("invalid TLS private key PEM"))?;
    let tls = server_config(certificates, key)
        .map_err(|_| invalid("invalid TLS certificate chain or mismatched private key"))?;
    let core = scoped_core(token, false)?;
    let endpoint = server_endpoint(config.quic_bind_address, tls)?;
    let listener = tokio::net::TcpListener::bind(config.management_bind_address).await?;
    let quic_address = endpoint.local_addr()?;
    let management_address = listener.local_addr()?;
    let worker = spawn_worker(TransportIndependentWorker::new(core));
    let quic_task = tokio::spawn(serve_endpoint(
        endpoint.clone(),
        QuicConfig::new(worker.clone()),
    ));
    let http_task = tokio::spawn(async move {
        axum::serve(
            listener,
            router_with_transports(true, false, None, false, worker),
        )
        .await
    });
    Ok(RemoteServer {
        quic_address,
        management_address,
        endpoint,
        quic_task,
        http_task,
    })
}

/// Run until Ctrl-C or a listener fails. Does not print credentials or certificate contents.
pub async fn serve_remote(config: RemoteServerConfig) -> Result<(), ServerError> {
    let mut server = start_remote(config).await?;
    println!(
        "Woven static scoped QUIC ready at {}; private management HTTP at {}; WebTransport/inference disabled",
        server.quic_address, server.management_address
    );
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        result = &mut server.http_task => {
            result.map_err(|_| invalid("management task failed"))??;
            return Err(invalid("management listener stopped"));
        },
        _ = &mut server.quic_task => return Err(invalid("QUIC listener stopped")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_partial_or_unapproved_configuration() {
        assert!(
            RemoteServerConfig::from_lookup(|_| Err(invalid("absent")))
                .unwrap()
                .is_none()
        );
        assert!(
            RemoteServerConfig::from_lookup(|name| if name == "WOVEN_QUIC_BIND" {
                Ok("0.0.0.0:8081".into())
            } else {
                Err(invalid("absent"))
            })
            .is_err()
        );
        assert!(RemoteServerConfig::from_lookup(|_| Ok("0".into())).is_err());
        assert!(
            RemoteServerConfig::from_lookup(|name| match name {
                "WOVEN_REMOTE_QUIC" => Ok("1".into()),
                "WOVEN_QUIC_BIND" | "WOVEN_MANAGEMENT_BIND" => Ok("0.0.0.0:8081".into()),
                _ => Ok("file".into()),
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_weak_or_malformed_credentials() {
        for token in [
            b"".as_slice(),
            b"dev-token",
            b"ai-companion-dev-token",
            b"a token with embedded whitespace and length",
        ] {
            assert!(validate_token(token).is_err());
        }
        assert!(validate_token(&vec![b'a'; 4099]).is_err());
        assert_eq!(
            validate_token(b"01234567890123456789012345678901\n")
                .unwrap()
                .len(),
            32
        );
    }
}
