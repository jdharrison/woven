//! Opt-in real local logging E2E: run `npm run test:logs:local` in woven-host.
use serde_json::Value;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use woven_client::{Client, ClientConfig, ClientTlsConfig};
use woven_protocol::{
    AdmissionStatus, AuthenticationScheme, ControlPayload, Envelope, MessagePayload,
};
use woven_server::{ManagedServerConfig, start_managed};

fn scope(descriptor: &Value) -> (u64, u64) {
    (
        descriptor["namespaceId"].as_str().unwrap().parse().unwrap(),
        descriptor["sessionId"].as_str().unwrap().parse().unwrap(),
    )
}

async fn admitted(descriptor: &Value, name: &str) -> Client {
    let mut client = Client::connect_with_tls_and_auth(
        ClientConfig {
            url: format!("quic://{}", descriptor["endpoint"].as_str().unwrap()),
            token: descriptor["token"].as_str().unwrap().into(),
            ..ClientConfig::default()
        },
        ClientTlsConfig::from_ca_pem(descriptor["caPem"].as_str().unwrap().as_bytes()).unwrap(),
        AuthenticationScheme::Bearer,
    )
    .await
    .unwrap();
    let (namespace, session) = scope(descriptor);
    assert_eq!(
        client
            .request_admission(namespace, session, 1, name.into())
            .await
            .unwrap()
            .status,
        AdmissionStatus::Admitted
    );
    client
}

async fn receive(client: &mut Client, matches: impl Fn(&Envelope) -> bool) -> Envelope {
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..16 {
            let envelope = client.recv().await.unwrap();
            if let MessagePayload::Control(ControlPayload::ProtocolError(error)) = &envelope.message
            {
                panic!("unexpected QUIC protocol error: {:?}", error.code);
            }
            assert!(
                !matches!(
                    envelope.message,
                    MessagePayload::Control(ControlPayload::ClientLog(_))
                ),
                "logs must not fan out as application traffic"
            );
            if matches(&envelope) {
                return envelope;
            }
        }
        panic!("QUIC receive frame cap exceeded");
    })
    .await
    .expect("bounded QUIC receive")
}

async fn subscribe(client: &mut Client, namespace: u64, session: u64, space: u64) -> u64 {
    client
        .subscribe_space(namespace, session, space, 1, 1)
        .await
        .unwrap();
    receive(client, |envelope| {
        envelope.space_id == space
            && matches!(
                envelope.message,
                MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
            )
    })
    .await;
    receive(client, |envelope| {
        envelope.space_id == space
            && matches!(
                envelope.message,
                MessagePayload::Control(ControlPayload::EntityEntered(_))
            )
    })
    .await
    .entity_id
    .unwrap()
}

async fn command(
    input: &mut tokio::process::ChildStdin,
    output: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    name: &str,
) -> Value {
    eprintln!("Logging Host phase: {name}");
    tokio::time::timeout(Duration::from_secs(30), async {
        input
            .write_all(format!("\"{name}\"\n").as_bytes())
            .await
            .unwrap();
        let line = output
            .next_line()
            .await
            .unwrap()
            .expect("logging Host peer exited; see credential-free stderr");
        serde_json::from_str(&line).expect("logging Host peer JSON response")
    })
    .await
    .expect("bounded logging Host phase")
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential logging lifecycle across Host HTTP, Firestore, and managed QUIC"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Firebase emulators; run npm run test:logs:local in woven-host"]
async fn host_logging_over_real_local_quic() {
    assert_eq!(std::env::var("WOVEN_NO_ENV_FILES").unwrap(), "1");
    assert_eq!(
        std::env::var("FIREBASE_PROJECT_ID").unwrap(),
        "demo-woven-logging-e2e"
    );
    let fixture = PathBuf::from(std::env::var("WOVEN_LOCAL_E2E_DIR").unwrap());
    let host = PathBuf::from(std::env::var("WOVEN_LOCAL_E2E_HOST").unwrap());
    tokio::time::timeout(Duration::from_secs(120), async {
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
            .args(["--import", "tsx", "scripts/logging-local-peer.mjs"])
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
            .env("WOVEN_CLIENT_QUIC_ENDPOINT", server.quic_address.to_string())
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
        let (namespace, session) = scope(&descriptors[0]);
        assert_ne!((namespace, session), scope(&descriptors[1]));
        let mut publisher = admitted(&descriptors[0], "logging-publisher").await;
        let mut observer = admitted(&descriptors[0], "logging-observer").await;
        let mut other = admitted(&descriptors[1], "logging-other-owner").await;
        for (client, owner) in [(&mut publisher, "a"), (&mut other, "b")] {
            client
                .logger()
                .info(format!("logging-local-{owner}-info"))
                .await
                .unwrap();
            client
                .logger()
                .warn(format!("logging-local-{owner}-warn"))
                .await
                .unwrap();
            client
                .logger()
                .error(format!("logging-local-{owner}-error"))
                .await
                .unwrap();
        }
        let entity = subscribe(&mut publisher, namespace, session, 1).await;
        subscribe(&mut observer, namespace, session, 1).await;
        for sequence in 1..=3 {
            let payload = format!("logging-local-publish-not-a-log-{sequence}").into_bytes();
            publisher
                .publish_event(namespace, session, 1, 1, 1, entity, sequence, 42, payload.clone())
                .await
                .unwrap();
            let delivered = receive(&mut observer, |envelope| {
                matches!(&envelope.message, MessagePayload::ReliableEvent(event) if event.bytes == payload)
            })
            .await;
            assert_eq!(
                (delivered.namespace_id, delivered.session_id, delivered.entity_id),
                (namespace, session, Some(entity))
            );
        }
        assert_eq!(command(&mut input, &mut output, "automatic").await, true);
        publisher.log("logging-local-a-first-get-sync").await.unwrap();
        // A later ordered subscription response fences receipt of the unacknowledged log send.
        subscribe(&mut publisher, namespace, session, 2).await;
        assert_eq!(command(&mut input, &mut output, "sync").await, true);
        assert_eq!(command(&mut input, &mut output, "isolation").await, true);
        publisher.close_gracefully(Duration::from_secs(2)).await.unwrap();
        observer.close_gracefully(Duration::from_secs(2)).await.unwrap();
        other.close_gracefully(Duration::from_secs(2)).await.unwrap();
        assert_eq!(command(&mut input, &mut output, "disconnected").await, true);
        assert_eq!(command(&mut input, &mut output, "teardown").await, true);
        drop(input);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .expect("logging Host peer shutdown deadline")
                .unwrap()
                .success()
        );
    })
    .await
    .expect("bounded real local logging E2E");
}
