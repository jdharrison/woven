//! Integration coverage over native QUIC transport (reference client).
//!
//! Uses `serve_dev_ephemeral` to stand up HTTP + QUIC + WebTransport, then drives the
//! QUIC transport through the public reference client. The capabilities control plane is
//! checked over plain HTTP.

use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use woven_client::{Client, ClientConfig, ClientError};
use woven_protocol::{ControlPayload, MessagePayload, ProtocolErrorCode};
use woven_server::serve_dev_ephemeral;

async fn start_server() -> String {
    serve_dev_ephemeral(false).await.unwrap().quic
}

#[tokio::test]
async fn capabilities_reports_quic_and_webtransport() {
    let urls = serve_dev_ephemeral(false).await.unwrap();
    let address = urls.http.trim_start_matches("http://");
    let body = http_get(address, "/v1/capabilities").await;
    assert!(body.contains("\"protocol_version\":1"));
    assert!(body.contains("\"quic\""));
    assert!(body.contains("\"webtransport\""));
    assert!(!body.contains("\"websocket\""));
    // The WebTransport endpoint is advertised as a relative `port/path` that a
    // client resolves against the host it used for the control plane.
    let wt = urls.webtransport.trim_start_matches("wtransport://");
    let port = wt
        .split('/')
        .next()
        .unwrap_or("")
        .split(':')
        .next_back()
        .unwrap_or("");
    let expected = format!("\"webtransport\":\"{port}/webtransport\"");

    assert!(
        body.contains(&expected),
        "capabilities should advertise the relative webtransport endpoint at port {port}, got: {body}"
    );
}

async fn http_get(address: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test]
async fn reference_client_completes_development_handshake() {
    let url = start_server().await;
    let client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await;
    assert!(client.is_ok());
    client.unwrap().close().unwrap();
}

#[tokio::test]
async fn invalid_credentials_are_rejected() {
    let url = start_server().await;
    let result = Client::connect(ClientConfig {
        url,
        token: "wrong-token".to_owned(),
        ..ClientConfig::default()
    })
    .await;
    assert!(matches!(result, Err(ClientError::ServerError(_))));
}

#[tokio::test]
async fn webtransport_client_completes_handshake_and_fans_out() {
    let webtransport = serve_dev_ephemeral(false).await.unwrap().webtransport;
    let config = |url| ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    };
    let mut alice = Client::connect(config(webtransport.clone())).await.unwrap();
    let mut bob = Client::connect(config(webtransport)).await.unwrap();
    for client in [&mut alice, &mut bob] {
        client.join_session(1, 1).await.unwrap();
        client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    }
    let alice_entity = receive_assigned_entity(&mut alice).await;
    let _bob_entity = receive_assigned_entity(&mut bob).await;
    alice
        .publish_event(1, 1, 1, 1, 1, alice_entity, 1, 1, b"hello-wt".to_vec())
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), bob.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(received.message, MessagePayload::ReliableEvent(ref payload) if payload.bytes == b"hello-wt")
    );
    alice
        .close_gracefully(Duration::from_secs(2))
        .await
        .unwrap();
    let left = bob
        .recv_timeout(Duration::from_millis(500))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        left.message,
        MessagePayload::Control(ControlPayload::EntityLeft(_))
    ));
    assert_eq!(left.entity_id, Some(alice_entity));
    bob.close_gracefully(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn reliable_events_fan_out_to_subscribers() {
    let url = start_server().await;
    let config = |url| ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    };
    let mut alice = Client::connect(config(url.clone())).await.unwrap();
    let mut bob = Client::connect(config(url)).await.unwrap();
    for client in [&mut alice, &mut bob] {
        client.join_session(1, 1).await.unwrap();
        client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    }
    let alice_entity = receive_assigned_entity(&mut alice).await;
    let _bob_entity = receive_assigned_entity(&mut bob).await;
    alice
        .publish_event(1, 1, 1, 1, 1, alice_entity, 1, 1, b"hello".to_vec())
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), bob.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(received.message, MessagePayload::ReliableEvent(ref payload) if payload.bytes == b"hello")
    );
}

#[tokio::test]
async fn latest_value_replaces_pending_state_for_recipients() {
    let url = start_server().await;
    let config = |url| ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    };
    let mut alice = Client::connect(config(url.clone())).await.unwrap();
    let mut bob = Client::connect(config(url)).await.unwrap();
    for client in [&mut alice, &mut bob] {
        client.join_session(1, 1).await.unwrap();
        client.subscribe_space(1, 1, 1, 1, 2).await.unwrap();
    }
    let alice_entity = receive_assigned_entity(&mut alice).await;
    let _bob_entity = receive_assigned_entity(&mut bob).await;
    alice
        .publish_state(1, 1, 1, 1, 2, alice_entity, 1, 7, b"old".to_vec())
        .await
        .unwrap();
    alice
        .publish_state(1, 1, 1, 1, 2, alice_entity, 2, 7, b"new".to_vec())
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), bob.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(received.message, MessagePayload::EntityState(ref payload) if payload.bytes == b"new")
    );
    assert!(
        bob.recv_timeout(Duration::from_millis(50))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn snapshot_request_returns_an_opaque_scoped_snapshot() {
    let url = start_server().await;
    let mut client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.join_session(1, 1).await.unwrap();
    client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    let _entity = receive_assigned_entity(&mut client).await;
    client.request_snapshot(1, 1, 1, 1, 1).await.unwrap();
    let snapshot = client.recv().await.unwrap();
    assert!(
        matches!(snapshot.message, MessagePayload::Snapshot(ref payload) if !payload.bytes.is_empty())
    );
}

async fn receive_assigned_entity(client: &mut Client) -> u64 {
    let accepted = client.recv().await.unwrap();
    assert!(matches!(
        accepted.message,
        MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
    ));
    let entered = client.recv().await.unwrap();
    let MessagePayload::Control(ControlPayload::EntityEntered(_)) = entered.message else {
        panic!("expected EntityEntered");
    };
    entered.entity_id.unwrap()
}

#[tokio::test]
async fn wrong_namespace_cannot_be_joined() {
    let url = start_server().await;
    let mut client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.join_session(2, 1).await.unwrap();
    let response = client.recv().await.unwrap();
    assert!(
        matches!(response.message, MessagePayload::Control(ControlPayload::ProtocolError(ref error)) if error.code == ProtocolErrorCode::Unauthorized)
    );
}

#[tokio::test]
async fn nested_subscriptions_hold_distinct_owned_entities() {
    let url = start_server().await;
    let mut client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.join_session(1, 1).await.unwrap();
    client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    let first = receive_assigned_entity(&mut client).await;
    client.subscribe_space(1, 1, 2, 1, 1).await.unwrap();
    let second = receive_assigned_entity(&mut client).await;
    assert_ne!(first, second);
}

#[tokio::test]
async fn entity_owner_is_enforced_over_quic() {
    let url = start_server().await;
    let config = |url| ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    };
    let mut alice = Client::connect(config(url.clone())).await.unwrap();
    let mut bob = Client::connect(config(url)).await.unwrap();
    for client in [&mut alice, &mut bob] {
        client.join_session(1, 1).await.unwrap();
        client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    }
    let alice_entity = receive_assigned_entity(&mut alice).await;
    let _bob_entity = receive_assigned_entity(&mut bob).await;
    bob.publish_event(1, 1, 1, 1, 1, alice_entity, 1, 1, b"forged".to_vec())
        .await
        .unwrap();
    let response = bob.recv().await.unwrap();
    assert!(
        matches!(response.message, MessagePayload::Control(ControlPayload::ProtocolError(ref error)) if error.code == ProtocolErrorCode::Unauthorized)
    );
}

#[tokio::test]
async fn transition_emits_left_then_entered() {
    let url = start_server().await;
    let mut client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.join_session(1, 1).await.unwrap();
    client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    let entity = receive_assigned_entity(&mut client).await;
    client.subscribe_space(1, 1, 2, 1, 1).await.unwrap();
    let _other_entity = receive_assigned_entity(&mut client).await;
    client
        .transition_entity(1, 1, 1, 1, 2, 1, entity)
        .await
        .unwrap();
    let left = client.recv().await.unwrap();
    assert!(
        matches!(left.message, MessagePayload::Control(ControlPayload::EntityLeft(ref value)) if left.entity_id == Some(entity) && value.reason == woven_protocol::EntityLeaveReason::Transitioned)
    );
    let entered = client.recv().await.unwrap();
    assert!(
        matches!(entered.message, MessagePayload::Control(ControlPayload::EntityEntered(_)) if entered.entity_id == Some(entity) && entered.space_id == 2)
    );
}

#[tokio::test]
async fn disconnect_emits_entity_left_to_other_subscribers() {
    let url = start_server().await;
    let config = |url| ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    };
    let mut alice = Client::connect(config(url.clone())).await.unwrap();
    let mut bob = Client::connect(config(url)).await.unwrap();
    for client in [&mut alice, &mut bob] {
        client.join_session(1, 1).await.unwrap();
        client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    }
    let alice_entity = receive_assigned_entity(&mut alice).await;
    let _bob_entity = receive_assigned_entity(&mut bob).await;
    alice.close().unwrap();
    let left = tokio::time::timeout(Duration::from_secs(1), bob.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(left.message, MessagePayload::Control(ControlPayload::EntityLeft(ref value)) if left.entity_id == Some(alice_entity) && value.reason == woven_protocol::EntityLeaveReason::Disconnected)
    );
}
