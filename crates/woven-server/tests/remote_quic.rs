//! Only ephemeral loopback traffic and throwaway local TLS fixtures; no external targets.
use std::{path::PathBuf, time::Duration};
use woven_client::{Client, ClientConfig, ClientError, ClientTlsConfig};
use woven_protocol::{ControlPayload, MessagePayload, ProtocolErrorCode};
use woven_server::{RemoteServerConfig, start_remote};

const TOKEN: &str = "test-only-static-scoped-credential-0123456789";

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("woven-remote-quic-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> RemoteServerConfig {
        RemoteServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: self.0.join("cert.pem"),
            private_key_file: self.0.join("key.pem"),
            auth_token_file: self.0.join("token"),
        }
    }
}

fn client_config(url: &str, token: &str) -> ClientConfig {
    ClientConfig {
        url: url.to_owned(),
        token: token.to_owned(),
        ..ClientConfig::default()
    }
}

async fn assigned(client: &mut Client) -> u64 {
    assert!(matches!(
        client.recv().await.unwrap().message,
        MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
    ));
    let entered = client.recv().await.unwrap();
    assert!(matches!(
        entered.message,
        MessagePayload::Control(ControlPayload::EntityEntered(_))
    ));
    entered.entity_id.unwrap()
}

#[tokio::test]
async fn verified_remote_composition_on_loopback() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let fixture = Fixture::new();
        let config = fixture.config();
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        std::fs::write(&config.certificate_file, certificate.cert.pem()).unwrap();
        std::fs::write(&config.private_key_file, certificate.key_pair.serialize_pem()).unwrap();
        std::fs::write(&config.auth_token_file, TOKEN).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&config.private_key_file, &config.auth_token_file] {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        let tls = ClientTlsConfig::from_ca_pem(certificate.cert.pem().as_bytes()).unwrap();
        let server = start_remote(config.clone()).await.unwrap();
        let url = format!("quic://{}", server.quic_address);

        let other = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let untrusted = ClientTlsConfig::from_ca_pem(other.cert.pem().as_bytes()).unwrap();
        assert!(matches!(Client::connect_with_tls(client_config(&url, TOKEN), untrusted).await, Err(ClientError::Transport(_))));
        // Same trusted certificate, different TLS name: localhost is not in its SANs.

        // Use a second IPv6 listener when localhost resolves to ::1 so the failure cannot
        // merely be an unreachable first DNS address.
        let first_localhost = tokio::net::lookup_host(("localhost", 0)).await.unwrap().next().unwrap();
        let mut name_config = config.clone();
        name_config.quic_bind_address = first_localhost;
        let name_server = start_remote(name_config).await.unwrap();
        let wrong_name = format!("quic://localhost:{}", name_server.quic_address.port());
        let mismatch = Client::connect_with_tls(client_config(&wrong_name, TOKEN), tls.clone()).await;
        assert!(matches!(mismatch, Err(ClientError::Transport(ref message)) if message.contains("certificate")), "expected certificate rejection");

        for token in ["wrong-token", "dev-token", "ai-companion-dev-token"] {
            assert!(matches!(Client::connect_with_tls(client_config(&url, token), tls.clone()).await,
                Err(ClientError::ServerError(ref error)) if error.code == ProtocolErrorCode::Unauthorized));
        }
        let mut alice = Client::connect_with_tls(client_config(&url, TOKEN), tls.clone()).await.unwrap();
        alice.join_session(2, 1).await.unwrap();
        assert!(matches!(alice.recv().await.unwrap().message,
            MessagePayload::Control(ControlPayload::ProtocolError(ref error)) if error.code == ProtocolErrorCode::Unauthorized));
        // An unauthorized control operation terminates the connection by protocol policy.
        alice.close().unwrap();
        let mut alice = Client::connect_with_tls(client_config(&url, TOKEN), tls.clone()).await.unwrap();
        let mut bob = Client::connect_with_tls(client_config(&url, TOKEN), tls.clone()).await.unwrap();
        for client in [&mut alice, &mut bob] {
            client.join_session(1, 1).await.unwrap();
            client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
        }
        let entity = assigned(&mut alice).await;
        let _ = assigned(&mut bob).await;
        alice.publish_event(1, 1, 1, 1, 1, entity, 1, 1, b"verified fanout".to_vec()).await.unwrap();
        assert!(matches!(bob.recv().await.unwrap().message, MessagePayload::ReliableEvent(ref payload) if payload.bytes == b"verified fanout"));
        alice.close_gracefully(Duration::from_secs(2)).await.unwrap();
        let left = bob.recv_timeout(Duration::from_millis(500)).await.unwrap().expect("prompt EntityLeft after graceful close");
        assert!(matches!(left.message, MessagePayload::Control(ControlPayload::EntityLeft(_))) && left.entity_id == Some(entity));
        bob.close_gracefully(Duration::from_secs(2)).await.unwrap();
        let no_budget = Client::connect_with_tls(client_config(&url, TOKEN), tls).await.unwrap();
        assert!(matches!(no_budget.close_gracefully(Duration::ZERO).await,
            Err(ClientError::Transport(ref message)) if message == "graceful close timed out"));
        drop(server);
        drop(name_server);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.auth_token_file, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(start_remote(config.clone()).await.is_err());
            std::fs::set_permissions(&config.auth_token_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut insecure = config.clone();
        insecure.management_bind_address = "0.0.0.0:0".parse().unwrap();
        assert!(start_remote(insecure).await.is_err());
        std::fs::write(&config.private_key_file, other.key_pair.serialize_pem()).unwrap();
        assert!(start_remote(config.clone()).await.is_err());
        std::fs::write(&config.private_key_file, certificate.key_pair.serialize_pem()).unwrap();
        std::fs::write(&config.auth_token_file, "dev-token").unwrap();
        assert!(start_remote(config.clone()).await.is_err());
        std::fs::write(&config.auth_token_file, TOKEN).unwrap();

        let mut expired_params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        expired_params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        expired_params.not_after = rcgen::date_time_ymd(2021, 1, 1);
        let expired = expired_params.self_signed(&certificate.key_pair).unwrap();
        std::fs::write(&config.certificate_file, expired.pem()).unwrap();
        let expired_server = start_remote(config.clone()).await.unwrap();
        let expired_tls = ClientTlsConfig::from_ca_pem(expired.pem().as_bytes()).unwrap();
        let expired_url = format!("quic://{}", expired_server.quic_address);
        let rejected = Client::connect_with_tls(client_config(&expired_url, TOKEN), expired_tls).await;
        assert!(matches!(rejected, Err(ClientError::Transport(ref message)) if message.contains("certificate")));
        drop(expired_server);

        std::fs::write(&config.certificate_file, "malformed PEM").unwrap();
        assert!(start_remote(config).await.is_err());
    }).await.expect("bounded local QUIC scenario timed out");
}

#[tokio::test]
async fn insecure_remote_client_is_rejected_without_network_traffic() {
    assert!(
        Client::connect(client_config("quic://192.0.2.1:8081", TOKEN))
            .await
            .is_err()
    );
    assert!(
        Client::connect(client_config(
            "wtransport://192.0.2.1:8082/webtransport",
            TOKEN
        ))
        .await
        .is_err()
    );
    assert!(ClientTlsConfig::from_ca_pem(b"").is_err());
    assert!(ClientTlsConfig::from_ca_pem(b"not a certificate").is_err());
    assert!(!format!("{:?}", client_config("quic://127.0.0.1:1", TOKEN)).contains(TOKEN));
}
