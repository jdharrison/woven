use super::*;
use std::collections::BTreeMap;

fn settings() -> BTreeMap<&'static str, String> {
    [
        ("WOVEN_MANAGED_QUIC", "1"),
        ("WOVEN_QUIC_BIND", "127.0.0.1:0"),
        ("WOVEN_MANAGEMENT_BIND", "127.0.0.1:0"),
        ("WOVEN_ADMIN_BIND", "127.0.0.1:0"),
        ("WOVEN_TLS_CERT_FILE", "cert.pem"),
        ("WOVEN_TLS_KEY_FILE", "key.pem"),
        ("WOVEN_ADMIN_TOKEN_FILE", "admin"),
    ]
    .into_iter()
    .map(|(key, value)| (key, value.to_owned()))
    .collect()
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one table-driven matrix covers all managed environment invariants"
)]
fn environment_requires_complete_explicit_nonmixed_configuration() {
    let values = settings();
    assert!(
        ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned()))
            .unwrap()
            .is_some()
    );
    for missing in values.keys() {
        let mut incomplete = values.clone();
        incomplete.remove(missing);
        assert!(ManagedServerConfig::from_lookup(|key| Ok(incomplete.get(key).cloned())).is_err());
    }
    for (key, value) in [
        ("WOVEN_MANAGED_QUIC", "0"),
        ("WOVEN_REMOTE_QUIC", "1"),
        ("WOVEN_AUTH_TOKEN_FILE", "static-token"),
        ("WOVEN_ADMIN_BIND", "0.0.0.0:9000"),
        ("WOVEN_MANAGEMENT_BIND", "0.0.0.0:9001"),
        ("WOVEN_QUIC_BIND", "not a socket"),
        ("WOVEN_TLS_KEY_FILE", ""),
    ] {
        let mut invalid_values = values.clone();
        invalid_values.insert(key, value.into());
        assert!(
            ManagedServerConfig::from_lookup(|key| Ok(invalid_values.get(key).cloned())).is_err()
        );
    }
    assert!(
        ManagedServerConfig::from_lookup(|_| Ok(None))
            .unwrap()
            .is_none()
    );
    assert!(ManagedServerConfig::from_lookup(|_| Err(invalid())).is_err());

    for (key, value) in [
        ("WOVEN_MANAGED_WEBTRANSPORT", "1"),
        ("WOVEN_WEBTRANSPORT_BIND", "127.0.0.1:0"),
        ("WOVEN_WEBTRANSPORT_PATH", "/managed-webtransport"),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test",
        ),
    ] {
        let mut incomplete = values.clone();
        incomplete.insert(key, value.to_owned());
        assert!(ManagedServerConfig::from_lookup(|key| Ok(incomplete.get(key).cloned())).is_err());
    }

    let mut with_webtransport = values.clone();
    with_webtransport.extend([
        ("WOVEN_MANAGED_WEBTRANSPORT", "1".to_owned()),
        ("WOVEN_WEBTRANSPORT_BIND", "127.0.0.1:0".to_owned()),
        (
            "WOVEN_WEBTRANSPORT_PATH",
            "/managed-webtransport".to_owned(),
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test,https://operator.example.test:8443".to_owned(),
        ),
    ]);
    let config = ManagedServerConfig::from_lookup(|key| Ok(with_webtransport.get(key).cloned()))
        .unwrap()
        .unwrap();
    let webtransport = config.webtransport.unwrap();
    assert_eq!(webtransport.path, "/managed-webtransport");
    assert_eq!(
        webtransport.origin_policy,
        ManagedWebTransportOriginPolicy::Allowlist(vec![
            "https://console.example.test".to_owned(),
            "https://operator.example.test:8443".to_owned()
        ])
    );

    for (key, value) in [
        ("WOVEN_MANAGED_WEBTRANSPORT", "0"),
        ("WOVEN_WEBTRANSPORT_BIND", "invalid"),
        ("WOVEN_WEBTRANSPORT_PATH", "managed-webtransport"),
        ("WOVEN_WEBTRANSPORT_PATH", "/managed?query=true"),
        ("WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS", ""),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test/path",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test/",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test?query=true",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test#fragment",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://user@console.example.test",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "HTTPS://console.example.test",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://Console.Example.Test",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test:443",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "http://console.example.test:80",
        ),
        (
            "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
            "https://console.example.test,https://console.example.test",
        ),
    ] {
        let mut invalid_values = with_webtransport.clone();
        invalid_values.insert(key, value.to_owned());
        assert!(
            ManagedServerConfig::from_lookup(|key| Ok(invalid_values.get(key).cloned())).is_err()
        );
    }

    let mut oversized_origins = with_webtransport.clone();
    oversized_origins.insert(
        "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
        "a".repeat(MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES + 1),
    );
    assert!(
        ManagedServerConfig::from_lookup(|key| Ok(oversized_origins.get(key).cloned())).is_err()
    );

    let mut too_many_origins = with_webtransport;
    too_many_origins.insert(
        "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
        (0..=MAX_WEBTRANSPORT_ORIGINS)
            .map(|index| format!("https://origin-{index}.example.test"))
            .collect::<Vec<_>>()
            .join(","),
    );
    assert!(
        ManagedServerConfig::from_lookup(|key| Ok(too_many_origins.get(key).cloned())).is_err()
    );
}

#[tokio::test]
async fn metrics_returns_unavailable_instead_of_fabricated_zeros_after_worker_failure() {
    use tower::ServiceExt;
    use woven_transport::{ManagedPersistence, ManagedPersistenceError};
    struct FailingStore;
    impl ManagedPersistence for FailingStore {
        fn save(
            &self,
            _: woven_core::ManagedCheckpoint,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), ManagedPersistenceError>> + Send + '_>,
        > {
            Box::pin(std::future::ready(Err(ManagedPersistenceError)))
        }
    }
    let worker = spawn_worker_with_managed_persistence(
        TransportIndependentWorker::new(
            managed_core("synthetic-metrics-admin-0123456789").unwrap(),
        ),
        Arc::new(FailingStore),
    );
    let app = crate::router_with_transports(true, false, None, false, worker.clone());
    worker
        .execute(woven_core::Command::TransportConnected)
        .await
        .unwrap();
    let request = || {
        Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let response = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .any(|line| line == "woven_connections_active 1")
    );
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        worker.manage(ManagedRequest::Put {
            session: SessionKey::new(NamespaceId::new(1), SessionId::new(1)),
            revision: 1,
            allocated_ccu: 1,
            tick_rate_hz: None,
            credentials: Credentials::new("a".repeat(64)),
        }),
    )
    .await
    .unwrap();
    assert_eq!(result, Err(ManagedError::WorkerUnavailable));
    assert!(worker.is_closed());
    let response = app.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn stalled_persistence_times_out_permanently_and_readiness_rejects_closed_worker() {
    use tower::ServiceExt;
    use woven_transport::{
        MANAGED_PERSISTENCE_TIMEOUT, ManagedPersistence, ManagedPersistenceError,
    };
    struct StalledStore;
    impl ManagedPersistence for StalledStore {
        fn save(
            &self,
            _: woven_core::ManagedCheckpoint,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), ManagedPersistenceError>> + Send + '_>,
        > {
            Box::pin(std::future::pending())
        }
    }
    let worker = spawn_worker_with_managed_persistence(
        TransportIndependentWorker::new(
            managed_core("synthetic-timeout-admin-0123456789").unwrap(),
        ),
        Arc::new(StalledStore),
    );
    let app = crate::router_with_transports(true, false, None, false, worker.clone());
    let session = SessionKey::new(NamespaceId::new(1), SessionId::new(1));
    let outcome = tokio::time::timeout(
        MANAGED_PERSISTENCE_TIMEOUT + Duration::from_secs(3),
        worker.manage(ManagedRequest::Put {
            session,
            revision: 1,
            allocated_ccu: 1,
            tick_rate_hz: None,
            credentials: Credentials::new("a".repeat(64)),
        }),
    )
    .await
    .unwrap();
    assert_eq!(outcome, Err(ManagedError::WorkerUnavailable));
    assert!(worker.is_closed());
    assert!(
        worker
            .manage(ManagedRequest::Get { session })
            .await
            .is_err()
    );
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn durable_directory_is_explicit_and_never_enables_development_fallback() {
    let mut values = settings();
    values.insert("WOVEN_MANAGED_STATE_DIR", "/var/lib/woven-managed".into());
    let config = ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned()))
        .unwrap()
        .unwrap();
    assert_eq!(
        config.state_directory,
        Some(PathBuf::from("/var/lib/woven-managed"))
    );
    values.remove("WOVEN_MANAGED_QUIC");
    assert!(ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned())).is_err());
    values.insert("WOVEN_MANAGED_QUIC", "1".into());
    for path in ["", "relative/state"] {
        values.insert("WOVEN_MANAGED_STATE_DIR", path.into());
        assert!(ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned())).is_err());
    }
}

#[test]
fn webtransport_origin_policy_is_explicit_and_fails_closed() {
    let mut values = settings();
    values.extend([
        ("WOVEN_MANAGED_WEBTRANSPORT", "1".to_owned()),
        ("WOVEN_WEBTRANSPORT_BIND", "127.0.0.1:0".to_owned()),
        (
            "WOVEN_WEBTRANSPORT_PATH",
            "/managed-webtransport".to_owned(),
        ),
        ("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", "https".to_owned()),
    ]);
    let parse = |values: &BTreeMap<&str, String>| {
        ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned()))
    };
    assert_eq!(
        parse(&values)
            .unwrap()
            .unwrap()
            .webtransport
            .unwrap()
            .origin_policy,
        ManagedWebTransportOriginPolicy::Https
    );
    for missing in values.keys() {
        let mut incomplete = values.clone();
        incomplete.remove(missing);
        assert!(parse(&incomplete).is_err(), "missing {missing}");
    }
    for value in ["", "HTTPS", "https ", "unknown", "allowlist"] {
        let mut invalid_values = values.clone();
        invalid_values.insert("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", value.to_owned());
        assert!(parse(&invalid_values).is_err(), "policy {value:?}");
    }
    let mut empty_exceptions = values.clone();
    empty_exceptions.insert("WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS", String::new());
    assert!(parse(&empty_exceptions).is_err());
    for origins in [
        vec!["https://jdharrison.com"],
        vec!["http://localhost:8000", "http://127.0.0.1:8000"],
        vec![
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "https://jdharrison.com",
        ],
    ] {
        let mut combined = values.clone();
        combined.insert("WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS", origins.join(","));
        assert_eq!(
            parse(&combined)
                .unwrap()
                .unwrap()
                .webtransport
                .unwrap()
                .origin_policy,
            ManagedWebTransportOriginPolicy::HttpsWithAllowlist(
                origins.into_iter().map(str::to_owned).collect()
            )
        );
    }
    for policy in ["https", "allowlist", "unknown", ""] {
        let orphan = BTreeMap::from([("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", policy.to_owned())]);
        assert!(parse(&orphan).is_err());
        let mut orphan = settings();
        orphan.insert("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", policy.to_owned());
        assert!(parse(&orphan).is_err());
    }

    values.insert("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", "allowlist".to_owned());
    values.insert(
        "WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS",
        "http://localhost:5173,https://jdharrison.com".to_owned(),
    );
    let expected = ManagedWebTransportOriginPolicy::Allowlist(vec![
        "http://localhost:5173".to_owned(),
        "https://jdharrison.com".to_owned(),
    ]);
    assert_eq!(
        parse(&values)
            .unwrap()
            .unwrap()
            .webtransport
            .unwrap()
            .origin_policy,
        expected
    );
    values.remove("WOVEN_WEBTRANSPORT_ORIGIN_POLICY");
    assert_eq!(
        parse(&values)
            .unwrap()
            .unwrap()
            .webtransport
            .unwrap()
            .origin_policy,
        expected
    );
}

#[test]
fn combined_webtransport_origin_exceptions_are_bounded_and_canonical() {
    let mut values = settings();
    values.extend([
        ("WOVEN_MANAGED_WEBTRANSPORT", "1".to_owned()),
        ("WOVEN_WEBTRANSPORT_BIND", "127.0.0.1:0".to_owned()),
        (
            "WOVEN_WEBTRANSPORT_PATH",
            "/managed-webtransport".to_owned(),
        ),
        ("WOVEN_WEBTRANSPORT_ORIGIN_POLICY", "https".to_owned()),
    ]);
    let parse = |origins: &str| {
        let mut values = values.clone();
        values.insert("WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS", origins.to_owned());
        ManagedServerConfig::from_lookup(|key| Ok(values.get(key).cloned()))
    };
    for origin in [
        "",
        "null",
        "*",
        "http://*.example.test",
        "https://*.example.test",
        "not-an-origin",
        "http://localhost:8000,",
        ",http://localhost:8000",
        "http://localhost:8000,http://localhost:8000",
        "http://localhost:8000, http://127.0.0.1:8000",
        "http://localhost:8000 ",
        "HTTP://localhost:8000",
        "http://LOCALHOST:8000",
        "http://localhost:80",
        "https://example.test:443",
        "http://localhost:8000/",
        "http://localhost:8000/path",
        "http://localhost:8000?query",
        "http://localhost:8000#fragment",
        "http://user@localhost:8000",
        "file:///localhost",
        "wss://localhost:8000",
    ] {
        assert!(parse(origin).is_err(), "accepted {origin:?}");
        let origins = origin.split(',').map(str::to_owned).collect();
        assert!(
            ManagedWebTransportConfig {
                bind_address: "127.0.0.1:0".parse().unwrap(),
                path: "/managed-webtransport".to_owned(),
                origin_policy: ManagedWebTransportOriginPolicy::HttpsWithAllowlist(origins),
            }
            .validate()
            .is_err(),
            "embedding accepted {origin:?}"
        );
    }
    let config = |origins| ManagedWebTransportConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        path: "/managed-webtransport".to_owned(),
        origin_policy: ManagedWebTransportOriginPolicy::HttpsWithAllowlist(origins),
    };
    assert!(config(vec![]).validate().is_err());
    let mut origins: Vec<String> = (0..MAX_WEBTRANSPORT_ORIGINS)
        .map(|index| format!("http://o{index}.test"))
        .collect();
    assert!(parse(&origins.join(",")).is_ok());
    assert!(config(origins.clone()).validate().is_ok());
    origins.push("http://extra.test".to_owned());
    assert!(origins.join(",").len() < MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES);
    assert!(parse(&origins.join(",")).is_err());
    assert!(config(origins).validate().is_err());

    let origin = format!(
        "http://{}.test",
        "a".repeat(MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES - "http://.test".len())
    );
    assert!(parse(&origin).is_ok());
    assert!(config(vec![origin.clone()]).validate().is_ok());
    assert!(parse(&format!("{origin}a")).is_err());
    assert!(config(vec![format!("{origin}a")]).validate().is_err());
    assert!(parse(&format!("{origin},http://localhost:8000")).is_err());
    assert!(
        config(vec![origin, "http://localhost:8000".to_owned()])
            .validate()
            .is_err()
    );
}

#[tokio::test]
async fn admin_limits_and_secret_separation_are_enforced_before_dispatch() {
    let core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    let worker = spawn_worker(TransportIndependentWorker::new(core));
    let state = Arc::new(AdminState::new(
        worker,
        "node".into(),
        "b".repeat(48),
        json!({"mode": "ephemeral"}),
        "test-admin-credential-0123456789012345",
        None,
    ));
    for _ in 0..32 {
        assert!(state.rate_permitted());
    }
    assert!(!state.rate_permitted());
    state.rate.store(0, Ordering::Relaxed);
    let permits = state.concurrency.acquire_many(32).await.unwrap();
    let response = admin_request(
        State(state.clone()),
        Request::new(axum::body::Body::empty()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    drop(permits);
    let request = Request::builder()
        .uri("/v1/node")
        .header(
            header::AUTHORIZATION,
            "Bearer wrong-credential-0123456789012345",
        )
        .body(axum::body::Body::empty())
        .unwrap();
    assert_eq!(
        admin_request(State(state.clone()), request).await.status(),
        StatusCode::UNAUTHORIZED
    );
    for id in [
        "0",
        "01",
        "-1",
        "+1",
        "18446744073709551616",
        "1.0",
        "",
        " 1",
    ] {
        assert!(canonical_id(id).is_none());
    }
    assert_eq!(canonical_id("18446744073709551615"), Some(u64::MAX));
}
