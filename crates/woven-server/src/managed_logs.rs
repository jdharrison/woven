//! The independent bearer-authenticated admin listener is the only HTTP log reader.

use super::{AdminState, canonical_id, error, single_header};
use axum::{
    body::to_bytes,
    extract::Request,
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use woven_protocol::LogLevel;
use woven_transport::{LogEntry, LogPage, MAX_LOG_PAGE_ENTRIES};

const MAX_LOG_RESPONSE_BYTES: usize = 48 * 1024;

fn parameters(query: Option<&str>) -> Option<(u64, usize)> {
    let query = query?;
    if query.len() > 64 {
        return None;
    }
    let mut after = None;
    let mut limit = None;
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=')?;
        match name {
            "after" if after.is_none() => {
                after = Some(if value == "0" {
                    0
                } else {
                    canonical_id(value)?
                });
            }
            "limit" if limit.is_none() => {
                let value = usize::try_from(canonical_id(value)?).ok()?;
                if !(1..=MAX_LOG_PAGE_ENTRIES).contains(&value) {
                    return None;
                }
                limit = Some(value);
            }
            _ => return None,
        }
    }
    Some((after?, limit?))
}

pub(super) async fn dispatch(state: &AdminState, request: Request) -> Response {
    if single_header(request.headers(), "woven-node-incarnation")
        != Some(state.incarnation.as_str())
    {
        return error(StatusCode::CONFLICT, "incarnation_conflict");
    }
    let bad = || error(StatusCode::BAD_REQUEST, "invalid_request");
    if request.method() != Method::GET || request.headers().contains_key(header::TRANSFER_ENCODING)
    {
        return bad();
    }
    if request.headers().contains_key(header::CONTENT_LENGTH)
        && single_header(request.headers(), "content-length") != Some("0")
    {
        return bad();
    }
    let Some((after, limit)) = parameters(request.uri().query()) else {
        return bad();
    };
    // Check the actual body too: an embedding caller can omit Content-Length.
    if to_bytes(request.into_body(), 0).await.is_err() {
        return bad();
    }
    let Ok(page) = state.worker.read_logs(after, limit).await else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "worker_unavailable");
    };
    match serialize_page(&state.incarnation, after, page) {
        Ok(bytes) => ([(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "serialization_failed"),
    }
}

fn entry_json(entry: &LogEntry) -> Value {
    json!({
        "sequence": entry.sequence.to_string(),
        "occurredAtMs": entry.occurred_at_ms,
        "namespaceId": entry.session.namespace.to_string(),
        "sessionId": entry.session.session.to_string(),
        "connectionId": entry.connection.to_string(),
        "source": entry.source,
        "event": entry.event,
        "level": match entry.level {
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
            LogLevel::Unknown => unreachable!("only validated log levels are captured"),
        },
        "message": entry.message,
    })
}

fn serialize_page(
    incarnation: &str,
    after: u64,
    page: LogPage,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut value = json!({
        "nodeIncarnation": incarnation,
        "entries": [],
        "nextSequence": after.to_string(),
        "droppedThrough": page.dropped_through.to_string(),
    });
    // Reserve the maximum cursor growth. Count actual escaped JSON, not UTF-8 message size.
    let mut size = serde_json::to_vec(&value)?.len() + 20;
    let mut next = after;
    let mut entries = Vec::with_capacity(page.entries.len());
    for entry in page.entries {
        let json = entry_json(&entry);
        let bytes = serde_json::to_vec(&json)?.len() + usize::from(!entries.is_empty());
        if size + bytes > MAX_LOG_RESPONSE_BYTES {
            break;
        }
        size += bytes;
        next = entry.sequence;
        entries.push(json);
    }
    value["entries"] = Value::Array(entries);
    value["nextSequence"] = Value::String(next.to_string());
    serde_json::to_vec(&value)
}

#[cfg(test)]
#[path = "managed_logs_tests.rs"]
mod tests;
