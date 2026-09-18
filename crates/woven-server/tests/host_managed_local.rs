//! Opt-in cross-repository test; run via woven-host/scripts/managed-local-e2e.mjs.
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use woven_client::{Client, ClientConfig, ClientTlsConfig};
use woven_protocol::{AdmissionStatus, AuthenticationScheme, ProtocolErrorCode, QueueState};
use woven_server::{ManagedServerConfig, start_managed};

async fn connect(descriptor: &Value) -> Result<Client, woven_client::ClientError> {
    Client::connect_with_tls_and_auth(
        ClientConfig {
            url: format!("quic://{}", descriptor["endpoint"].as_str().unwrap()),
            token: descriptor["token"].as_str().unwrap().into(),
            ..ClientConfig::default()
        },
        ClientTlsConfig::from_ca_pem(descriptor["caPem"].as_str().unwrap().as_bytes()).unwrap(),
        AuthenticationScheme::Bearer,
    )
    .await
}

async fn tls_rejected(descriptor: &Value) {
    match connect(descriptor).await {
        Err(error) => assert!(
            error.to_string().contains("certificate"),
            "expected TLS certificate failure: {error}"
        ),
        Ok(_) => panic!("invalid TLS trust unexpectedly accepted"),
    }
}

fn scope(value: &Value) -> (u64, u64) {
    (
        value["namespaceId"].as_str().unwrap().parse().unwrap(),
        value["sessionId"].as_str().unwrap().parse().unwrap(),
    )
}

async fn command(
    input: &mut tokio::process::ChildStdin,
    output: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    name: &str,
) -> Value {
    eprintln!("Host phase: {name}");
    input
        .write_all(format!("\"{name}\"\n").as_bytes())
        .await
        .unwrap();
    let line = tokio::time::timeout(Duration::from_secs(60), output.next_line())
        .await
        .expect("Host phase deadline")
        .unwrap()
        .expect("Host peer exited; see stderr (credentials are never printed)");
    serde_json::from_str(&line).expect("Host peer JSON response")
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential lifecycle across Host HTTP and managed QUIC"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Firebase emulators; use the Host local E2E launcher"]
async fn host_api_to_managed_node() {
    assert_eq!(std::env::var("WOVEN_NO_ENV_FILES").unwrap(), "1");
    let fixture = PathBuf::from(std::env::var("WOVEN_LOCAL_E2E_DIR").unwrap());
    let host = PathBuf::from(std::env::var("WOVEN_LOCAL_E2E_HOST").unwrap());
    tokio::time::timeout(Duration::from_secs(180), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        std::fs::write(fixture.join("cert.pem"), certificate.cert.pem()).unwrap();
        std::fs::write(
            fixture.join("key.pem"),
            certificate.key_pair.serialize_pem(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                fixture.join("key.pem"),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        let server = start_managed(ManagedServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            admin_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: fixture.join("cert.pem"),
            private_key_file: fixture.join("key.pem"),
            admin_token_file: fixture.join("admin"),
            webtransport: None,
        })
        .await
        .unwrap();
        let mut child = tokio::process::Command::new("node")
            .args(["--import", "tsx", "scripts/managed-local-peer.mjs"])
            .current_dir(host)
            .env("AUTH_MODE", "firebase")
            .env("APP_CHECK_MODE", "disabled")
            .env("LOG_LEVEL", "silent")
            .env(
                "WOVEN_MANAGEMENT_URL",
                format!("http://{}", server.admin_address),
            )
            .env("WOVEN_MANAGEMENT_TOKEN_FILE", fixture.join("admin"))
            .env("WOVEN_CREDENTIAL_KEY_FILE", fixture.join("credential-key"))
            .env(
                "WOVEN_CLIENT_QUIC_ENDPOINT",
                server.quic_address.to_string(),
            )
            .env("WOVEN_CLIENT_CA_FILE", fixture.join("cert.pem"))
            .env("WOVEN_CLIENT_SELF_SIGNED", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
        let descriptors = command(&mut input, &mut output, "setup").await;
        let a = &descriptors[0];
        let b = &descriptors[1];
        let (ns, session) = scope(a);
        let (other_ns, other_session) = scope(b);
        assert_ne!((ns, session), (other_ns, other_session));
        assert_eq!(
            a["spaces"],
            json!([
                {"spaceId": "1", "epoch": "1", "channelIds": ["1"]},
                {"spaceId": "2", "epoch": "1", "channelIds": ["1"]}
            ])
        );
        assert_eq!(a["spaces"], b["spaces"]);
        assert!(a["token"] != b["token"], "server-specific credentials");
        eprintln!("QUIC: TLS rejection checks");
        let mut wrong_ca = a.clone();
        wrong_ca["caPem"] = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])
            .unwrap()
            .cert
            .pem()
            .into();
        tls_rejected(&wrong_ca).await;
        let mut wrong_host = a.clone();
        wrong_host["endpoint"] = format!("localhost:{}", server.quic_address.port()).into();
        tls_rejected(&wrong_host).await;
        eprintln!("QUIC: foreign scope rejection");
        let mut foreign = connect(a).await.unwrap();
        assert!(matches!(
            foreign.request_admission(other_ns, other_session, 1, "foreign".into()).await,
            Err(woven_client::ClientError::ServerError(error)) if error.code == ProtocolErrorCode::Unauthorized
        ));
        foreign
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        eprintln!("QUIC: filling Lite 10 slots");
        let mut admitted = Vec::with_capacity(10);
        for index in 0..10 {
            let mut client = connect(a).await.unwrap();
            assert_eq!(
                client
                    .request_admission(ns, session, 1, format!("slot-{index}"))
                    .await
                    .unwrap()
                    .status,
                AdmissionStatus::Admitted
            );
            admitted.push(client);
        }
        let mut waiter = connect(a).await.unwrap();
        let queued = waiter
            .request_admission(ns, session, 1, "waiter".into())
            .await
            .unwrap();
        assert_eq!(queued.status, AdmissionStatus::Queued);
        let ticket = queued.ticket_id.unwrap();
        command(&mut input, &mut output, "full").await;
        admitted
            .pop()
            .unwrap()
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            waiter
                .queue_status(ns, session, 2, ticket)
                .await
                .unwrap()
                .state,
            QueueState::Offered
        );
        assert_eq!(
            waiter
                .queue_claim(ns, session, 3, ticket)
                .await
                .unwrap()
                .state,
            QueueState::Admitted
        );
        command(&mut input, &mut output, "claimed").await;
        admitted.push(waiter);
        let mut waiting = connect(a).await.unwrap();
        assert_eq!(
            waiting
                .request_admission(ns, session, 1, "delete-waiter".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Queued
        );
        admitted.push(waiting);
        admitted.push(connect(a).await.unwrap());
        let mut other = connect(b).await.unwrap();
        command(&mut input, &mut output, "delete").await;
        for client in &mut admitted {
            assert!(
                tokio::time::timeout(Duration::from_secs(3), client.recv())
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        assert!(connect(a).await.is_err(), "deleted credential revoked");
        assert_eq!(
            other
                .request_admission(other_ns, other_session, 1, "unaffected".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        command(&mut input, &mut output, "teardown").await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), other.recv())
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            connect(b).await.is_err(),
            "account teardown revoked credential"
        );
        drop(input);
        eprintln!("Host peer: awaiting shutdown");
        assert!(
            tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .expect("Host helper cleanup deadline")
                .unwrap()
                .success()
        );
    })
    .await
    .expect("bounded Host/managed Woven E2E");
}
