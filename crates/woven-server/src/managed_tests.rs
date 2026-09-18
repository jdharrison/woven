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
}

#[tokio::test]
async fn admin_limits_and_secret_separation_are_enforced_before_dispatch() {
    let core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    let worker = spawn_worker(TransportIndependentWorker::new(core));
    let state = Arc::new(AdminState::new(
        worker,
        "node".into(),
        "test-admin-credential-0123456789012345",
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
