use super::super::{AdminState, admin_request};
use super::*;
use axum::{body::Body, extract::State};
use std::sync::Arc;
use woven_core::{
    ConnectionId, CoreConfig, DevAuthenticator, NamespaceId, SessionId, SessionKey,
    TransportIndependentWorker, WovenCore,
};
use woven_transport::spawn_worker;

const ADMIN: &str = "log-feed-admin-test-credential-0123456789";

fn request(query: &str, bearer: Option<&str>, incarnation: Option<&str>) -> Request {
    let mut request = Request::builder().uri(format!("/v1/logs{query}"));
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    if let Some(incarnation) = incarnation {
        request = request.header("woven-node-incarnation", incarnation);
    }
    request.body(Body::empty()).unwrap()
}

fn state() -> Arc<AdminState> {
    let core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    Arc::new(AdminState::new(
        spawn_worker(TransportIndependentWorker::new(core)),
        "node".into(),
        ADMIN,
        None,
    ))
}

#[tokio::test]
async fn feed_requires_admin_and_explicit_single_matching_incarnation_on_every_read() {
    let state = state();
    for token in [None, Some("client-token-is-not-admin-0123456789012")] {
        let response = admin_request(
            State(state.clone()),
            request("?after=0&limit=32", token, Some("node")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    for incarnation in [None, Some("old-node"), Some("")] {
        assert_eq!(
            admin_request(
                State(state.clone()),
                request("?after=0&limit=32", Some(ADMIN), incarnation)
            )
            .await
            .status(),
            StatusCode::CONFLICT
        );
    }
    let mut duplicate = request("?after=0&limit=32", Some(ADMIN), Some("node"));
    duplicate.headers_mut().append(
        "woven-node-incarnation",
        header::HeaderValue::from_static("node"),
    );
    assert_eq!(
        admin_request(State(state.clone()), duplicate)
            .await
            .status(),
        StatusCode::CONFLICT
    );
    let mut duplicate = request("?after=0&limit=32", Some(ADMIN), Some("node"));
    duplicate.headers_mut().append(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Bearer {ADMIN}")).unwrap(),
    );
    assert_eq!(
        admin_request(State(state.clone()), duplicate)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = admin_request(
        State(state),
        request(
            "?after=18446744073709551615&limit=1",
            Some(ADMIN),
            Some("node"),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        json,
        json!({"nodeIncarnation": "node", "entries": [], "nextSequence": "18446744073709551615", "droppedThrough": "0"})
    );
}

#[test]
fn cursors_and_limits_are_raw_canonical_decimals_and_unknown_parameters_fail_closed() {
    for query in [
        None,
        Some(""),
        Some("after=0"),
        Some("limit=32"),
        Some("after=00&limit=32"),
        Some("after=01&limit=32"),
        Some("after=+1&limit=32"),
        Some("after=-1&limit=32"),
        Some("after=1.0&limit=32"),
        Some("after=%30&limit=32"),
        Some("after=18446744073709551616&limit=32"),
        Some("after=0&limit=0"),
        Some("after=0&limit=33"),
        Some("after=0&limit=032"),
        Some("after=0&limit=+1"),
        Some("after=0&limit=1&after=0"),
        Some("after=0&limit=1&limit=1"),
        Some("after=0&limit=1&tenant=1"),
        Some("after=0&limit=1&"),
        Some("after=0&limit=1=2"),
        Some("after= 1&limit=1"),
    ] {
        assert!(parameters(query).is_none(), "{query:?}");
    }
    assert_eq!(parameters(Some("after=0&limit=32")), Some((0, 32)));
    assert_eq!(
        parameters(Some("limit=1&after=18446744073709551615")),
        Some((u64::MAX, 1))
    );
}

#[tokio::test]
async fn feed_rejects_bad_query_method_and_bodies_at_http_boundary() {
    let state = state();
    for query in [
        "",
        "?after=00&limit=32",
        "?after=0&limit=33",
        "?after=0&limit=1&unknown=1",
    ] {
        assert_eq!(
            admin_request(
                State(state.clone()),
                request(query, Some(ADMIN), Some("node"))
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
    for method in [Method::POST, Method::PUT, Method::DELETE] {
        let mut value = request("?after=0&limit=32", Some(ADMIN), Some("node"));
        *value.method_mut() = method;
        assert_eq!(
            admin_request(State(state.clone()), value).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut value = request("?after=0&limit=32", Some(ADMIN), Some("node"));
    *value.body_mut() = Body::from("unexpected body");
    assert_eq!(
        admin_request(State(state.clone()), value).await.status(),
        StatusCode::BAD_REQUEST
    );
    for (name, value) in [("content-length", "1"), ("transfer-encoding", "chunked")] {
        let mut request = request("?after=0&limit=32", Some(ADMIN), Some("node"));
        request
            .headers_mut()
            .insert(name, header::HeaderValue::from_static(value));
        assert_eq!(
            admin_request(State(state.clone()), request).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
}

#[test]
fn serialized_escaping_budget_preserves_contract_and_continuation_without_skips() {
    let entries: Vec<_> = (1..=32)
        .map(|sequence| LogEntry {
            sequence,
            occurred_at_ms: 1_800_000_000_000,
            session: SessionKey::new(NamespaceId::new(u64::MAX), SessionId::new(u64::MAX)),
            connection: ConnectionId::new(u64::MAX),
            source: "client",
            event: "client.log",
            level: LogLevel::Error,
            message: "\0".repeat(woven_protocol::MAX_LOG_MESSAGE_BYTES),
        })
        .collect();
    let page = LogPage {
        entries: entries.clone(),
        next_sequence: 32,
        dropped_through: 0,
    };
    let bytes = serialize_page("incarnation", 0, page).unwrap();
    assert!(bytes.len() <= MAX_LOG_RESPONSE_BYTES);
    assert!(bytes.len() < 65536);
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let returned = value["entries"].as_array().unwrap();
    assert!(!returned.is_empty() && returned.len() < 32);
    assert_eq!(value["nextSequence"], returned.last().unwrap()["sequence"]);
    let after = value["nextSequence"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(
        returned[0],
        json!({
            "sequence": "1", "occurredAtMs": 1_800_000_000_000_u64,
            "namespaceId": u64::MAX.to_string(), "sessionId": u64::MAX.to_string(),
            "connectionId": u64::MAX.to_string(), "source": "client", "event": "client.log",
            "level": "error", "message": "\0".repeat(1024),
        })
    );
    let remainder = entries
        .into_iter()
        .filter(|entry| entry.sequence > after)
        .collect();
    let bytes = serialize_page(
        "incarnation",
        after,
        LogPage {
            entries: remainder,
            next_sequence: 32,
            dropped_through: 17,
        },
    )
    .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["entries"][0]["sequence"], (after + 1).to_string());
    assert_eq!(value["droppedThrough"], "17");
    assert!(bytes.len() <= MAX_LOG_RESPONSE_BYTES);
}
