//! Bounded, volatile capture owned exclusively by the core worker.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use woven_core::{Authenticator, ConnectionId, SessionKey, WovenCore};
use woven_protocol::{ClientLog, LogLevel, MAX_LOG_MESSAGE_BYTES, ProtocolErrorCode};

pub const LOG_CAPACITY: usize = 2048;
pub const MAX_LOG_PAGE_ENTRIES: usize = 32;
const CONNECTION_LOG_RATE: usize = 10;
const NODE_LOG_RATE: usize = 1024;
const LOG_WINDOW: Duration = Duration::from_secs(1);

/// IDs and time are attached by the worker; no client timestamp or connection ID is trusted.
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub sequence: u64,
    pub occurred_at_ms: u64,
    pub session: SessionKey,
    pub connection: ConnectionId,
    pub source: &'static str,
    pub event: &'static str,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug)]
pub struct LogPage {
    pub entries: Vec<LogEntry>,
    pub next_sequence: u64,
    pub dropped_through: u64,
}

#[derive(Default)]
struct ConnectionCapture {
    sessions: BTreeSet<SessionKey>,
    accepted: VecDeque<Instant>,
}

#[derive(Default)]
pub(super) struct LogCapture {
    entries: VecDeque<LogEntry>,
    last_sequence: u64,
    dropped_through: u64,
    connections: BTreeMap<ConnectionId, ConnectionCapture>,
    accepted: VecDeque<Instant>,
}

fn expire(window: &mut VecDeque<Instant>, now: Instant) {
    while window
        .front()
        .is_some_and(|time| now.saturating_duration_since(*time) >= LOG_WINDOW)
    {
        window.pop_front();
    }
}

impl LogCapture {
    pub(super) fn client<A: Authenticator>(
        &mut self,
        core: &WovenCore<A>,
        connection: ConnectionId,
        session: SessionKey,
        log: ClientLog,
        now: Instant,
    ) -> Result<(), ProtocolErrorCode> {
        let memberships = core
            .session_memberships(connection)
            .map_err(|error| crate::core_error_code(&error))?;
        if !memberships.contains(&session) {
            return Err(ProtocolErrorCode::InvalidScope);
        }
        validate_log(&log)?;
        self.observe_connection(core, connection, "session left");
        let capture = self
            .connections
            .get_mut(&connection)
            .expect("joined connection is tracked");
        expire(&mut capture.accepted, now);
        expire(&mut self.accepted, now);
        if capture.accepted.len() >= CONNECTION_LOG_RATE || self.accepted.len() >= NODE_LOG_RATE {
            return Err(ProtocolErrorCode::RateLimited);
        }
        capture.accepted.push_back(now);
        self.accepted.push_back(now);
        self.append(
            connection,
            session,
            "client",
            "client.log",
            log.level,
            log.message,
        );
        Ok(())
    }

    /// Compare real membership after an admission/join/leave operation, including failed retries.
    pub(super) fn observe_connection<A: Authenticator>(
        &mut self,
        core: &WovenCore<A>,
        connection: ConnectionId,
        reason: &'static str,
    ) {
        let current = core.session_memberships(connection).ok();
        let mut capture = self.connections.remove(&connection).unwrap_or_default();
        for session in &capture.sessions {
            if current.is_none_or(|sessions| !sessions.contains(session)) {
                self.append(
                    connection,
                    *session,
                    "node",
                    "client.disconnected",
                    LogLevel::Info,
                    reason.to_owned(),
                );
            }
        }
        if let Some(current) = current {
            for session in current.difference(&capture.sessions) {
                self.append(
                    connection,
                    *session,
                    "node",
                    "client.connected",
                    LogLevel::Info,
                    "session joined".to_owned(),
                );
            }
            capture.sessions.clone_from(current);
        }
        // Retain the rate window across leave/rejoin, but never beyond the core connection lifetime.
        if core.is_connected(connection)
            && (!capture.sessions.is_empty() || !capture.accepted.is_empty())
        {
            self.connections.insert(connection, capture);
        }
    }

    /// Core publication and lifecycle fan-out may disconnect other connections internally.
    pub(super) fn disconnected<A: Authenticator>(&self, core: &WovenCore<A>) -> Vec<ConnectionId> {
        self.connections
            .keys()
            .filter(|connection| !core.is_connected(**connection))
            .copied()
            .collect()
    }

    fn append(
        &mut self,
        connection: ConnectionId,
        session: SessionKey,
        source: &'static str,
        event: &'static str,
        level: LogLevel,
        message: String,
    ) {
        // Exhaustion stops capture rather than wrapping or reusing an incarnation's sequence.
        let Some(sequence) = self.last_sequence.checked_add(1) else {
            return;
        };
        self.last_sequence = sequence;
        if self.entries.len() == LOG_CAPACITY {
            self.dropped_through = self.entries.pop_front().expect("full ring").sequence;
        }
        let occurred_at_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        self.entries.push_back(LogEntry {
            sequence,
            occurred_at_ms,
            session,
            connection,
            source,
            event,
            level,
            message,
        });
    }

    pub(super) fn page(&self, after: u64, limit: usize) -> LogPage {
        let entries: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.sequence > after)
            .take(limit.min(MAX_LOG_PAGE_ENTRIES))
            .cloned()
            .collect();
        let next_sequence = entries.last().map_or(after, |entry| entry.sequence);
        LogPage {
            entries,
            next_sequence,
            dropped_through: self.dropped_through,
        }
    }
}

pub(super) fn validate_log(log: &ClientLog) -> Result<(), ProtocolErrorCode> {
    if log.message.len() > MAX_LOG_MESSAGE_BYTES {
        return Err(ProtocolErrorCode::PayloadTooLarge);
    }
    if log.message.is_empty() || log.level == LogLevel::Unknown {
        return Err(ProtocolErrorCode::MalformedFrame);
    }
    Ok(())
}

#[cfg(test)]
#[path = "logging_tests.rs"]
mod tests;
