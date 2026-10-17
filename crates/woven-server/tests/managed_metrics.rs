//! Bounded loopback metrics scrapes and throwaway TLS fixtures; no external targets.

use std::{path::PathBuf, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use woven_core::{Command, CommandResult};
use woven_server::{ManagedServer, ManagedServerConfig, start_managed};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("woven-managed-metrics-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let fixture = Self(path);
        let config = fixture.config();
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        std::fs::write(&config.certificate_file, certificate.cert.pem()).unwrap();
        std::fs::write(
            &config.private_key_file,
            certificate.key_pair.serialize_pem(),
        )
        .unwrap();
        std::fs::write(
            &config.admin_token_file,
            "test-only-metrics-admin-credential-0123456789",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&config.private_key_file, &config.admin_token_file] {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        fixture
    }

    fn config(&self) -> ManagedServerConfig {
        ManagedServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            admin_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: self.0.join("cert.pem"),
            private_key_file: self.0.join("key.pem"),
            admin_token_file: self.0.join("admin"),
            webtransport: None,
            state_directory: None,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn scrape(server: &ManagedServer) -> (String, String, String) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = tokio::net::TcpStream::connect(server.management_address)
            .await
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        stream
            .take(16 * 1024)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        let response = String::from_utf8(bytes).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert_eq!(head.lines().next().unwrap(), "HTTP/1.1 200 OK");
        assert!(head.lines().any(|line| {
            line.eq_ignore_ascii_case("content-type: text/plain; version=0.0.4; charset=utf-8")
        }));
        let identity = |header: &str| {
            let mut values = head.lines().filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case(header).then(|| value.trim())
            });
            let value = values.next().expect("managed metrics carry identity");
            assert!(values.next().is_none(), "one value per identity header");
            assert_eq!(value.len(), 48);
            assert!(
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            value.to_owned()
        };
        (
            identity("Woven-Node-Incarnation"),
            identity("Woven-Boot-Id"),
            body.to_owned(),
        )
    })
    .await
    .expect("bounded metrics scrape")
}

#[tokio::test]
async fn ephemeral_managed_metrics_keep_identity_within_boot_and_reset_on_restart() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let fixture = Fixture::new();
        let first = start_managed(fixture.config()).await.unwrap();
        let (incarnation, boot, initial) = scrape(&first).await;
        assert_eq!(incarnation, first.node_incarnation);
        assert_eq!(boot, first.boot_id);
        assert!(
            initial
                .lines()
                .any(|line| line == "woven_connections_total 0")
        );
        assert!(
            initial
                .lines()
                .any(|line| line == "woven_publishes_total 0")
        );
        assert!(initial.contains("# TYPE woven_publish_bytes_delivered_total counter\n"));
        assert!(!initial.contains(&incarnation));
        assert!(!initial.contains(&boot));
        assert_eq!(
            scrape(&first).await,
            (incarnation.clone(), boot.clone(), initial.clone())
        );

        assert!(matches!(
            first
                .worker
                .execute(Command::TransportConnected)
                .await
                .unwrap(),
            CommandResult::Connected(_)
        ));
        let (updated_incarnation, updated_boot, updated) = scrape(&first).await;
        assert_eq!(updated_incarnation, incarnation);
        assert_eq!(updated_boot, boot);
        assert!(
            updated
                .lines()
                .any(|line| line == "woven_connections_total 1")
        );
        assert!(
            updated
                .lines()
                .any(|line| line == "woven_connections_active 1")
        );
        assert_eq!(
            scrape(&first).await,
            (incarnation.clone(), boot.clone(), updated)
        );
        drop(first);

        let second = start_managed(fixture.config()).await.unwrap();
        let (new_incarnation, new_boot, restarted) = scrape(&second).await;
        assert_eq!(new_boot, second.boot_id);
        assert_ne!(new_boot, boot);
        assert_eq!(new_incarnation, second.node_incarnation);
        assert_ne!(new_incarnation, incarnation);
        assert_eq!(restarted, initial);
        assert_eq!(
            scrape(&second).await,
            (new_incarnation, new_boot, restarted)
        );
    })
    .await
    .expect("bounded managed metrics lifecycle");
}
