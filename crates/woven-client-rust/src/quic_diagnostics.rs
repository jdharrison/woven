//! Reason-free snapshots of native QUIC connection state.

use std::fmt;

use crate::{Client, Transport};

/// Locally observed native QUIC state, without peer reason text or connection metadata.
///
/// The fields are sampled independently and may change immediately after the query.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct QuicDiagnostics {
    /// Observed closure, if any. `None` does not guarantee that the connection is live.
    pub close_reason: Option<QuicCloseReason>,
    /// Current outgoing QUIC datagram budget, or `None` if unsupported by the peer
    /// or disabled locally. The entire encoded Woven frame must fit this budget;
    /// it is not an application payload limit or a guarantee that a send will succeed.
    /// The budget can change with the path MTU and may remain available after closure.
    pub max_datagram_bytes: Option<usize>,
}

/// Classification of Quinn's native connection errors, retaining only numeric codes.
///
/// Neither `Debug` nor `Display` includes reason text, endpoints, tokens, or payloads.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QuicCloseReason {
    /// Communication lapsed beyond the negotiated idle timeout.
    TimedOut,
    /// The peer reset the connection.
    Reset,
    /// The local application closed the connection.
    LocallyClosed,
    /// The peer application closed the connection with an application-specific code.
    ApplicationClosed { code: u64 },
    /// The peer's QUIC stack closed the connection with a transport code.
    ConnectionClosed { code: u64 },
    /// The local QUIC stack detected a transport error.
    TransportError { code: u64 },
    /// The peer does not support a compatible QUIC version.
    VersionMismatch,
    /// There was insufficient connection ID space to create the connection.
    CidsExhausted,
}

impl QuicCloseReason {
    fn from_quinn(error: &quinn::ConnectionError) -> Self {
        match error {
            quinn::ConnectionError::TimedOut => Self::TimedOut,
            quinn::ConnectionError::Reset => Self::Reset,
            quinn::ConnectionError::LocallyClosed => Self::LocallyClosed,
            quinn::ConnectionError::ApplicationClosed(close) => Self::ApplicationClosed {
                code: close.error_code.into_inner(),
            },
            quinn::ConnectionError::ConnectionClosed(close) => Self::ConnectionClosed {
                code: u64::from(close.error_code),
            },
            quinn::ConnectionError::TransportError(error) => Self::TransportError {
                code: u64::from(error.code),
            },
            quinn::ConnectionError::VersionMismatch => Self::VersionMismatch,
            quinn::ConnectionError::CidsExhausted => Self::CidsExhausted,
        }
    }
}

impl fmt::Display for QuicCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => f.write_str("timed out"),
            Self::Reset => f.write_str("reset by peer"),
            Self::LocallyClosed => f.write_str("closed locally"),
            Self::ApplicationClosed { code } => {
                write!(f, "application closed by peer (code {code})")
            }
            Self::ConnectionClosed { code } => {
                write!(f, "connection closed by peer (transport code {code})")
            }
            Self::TransportError { code } => write!(f, "local transport error (code {code})"),
            Self::VersionMismatch => f.write_str("QUIC version mismatch"),
            Self::CidsExhausted => f.write_str("connection IDs exhausted"),
        }
    }
}

impl Client {
    /// Read a reason-free snapshot of native QUIC state, including observed closure.
    ///
    /// Returns `None` for WebTransport. This performs no I/O, probes, or retries and
    /// does not change connection timing or keepalives. A `None` close reason only
    /// means no closure has been observed yet; it does not guarantee liveness.
    #[must_use]
    pub fn quic_diagnostics(&self) -> Option<QuicDiagnostics> {
        match &self.transport {
            Transport::Quic { connection, .. } => Some(QuicDiagnostics {
                close_reason: connection
                    .close_reason()
                    .as_ref()
                    .map(QuicCloseReason::from_quinn),
                max_datagram_bytes: connection.max_datagram_size(),
            }),
            Transport::WebTransport { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNTRUSTED_REASON: &str = "token=ATTACKER_SECRET\nforged event\r\nendpoint=quic://private.invalid:4433 payload=PRIVATE";

    fn assert_classification(
        error: &quinn::ConnectionError,
        expected: QuicCloseReason,
        display: &str,
    ) {
        let actual = QuicCloseReason::from_quinn(error);
        assert_eq!(actual, expected);
        assert_eq!(actual.to_string(), display);
        let diagnostics = QuicDiagnostics {
            close_reason: Some(actual),
            max_datagram_bytes: Some(1200),
        };
        for rendered in [
            actual.to_string(),
            format!("{actual:?}"),
            format!("{diagnostics:?}"),
        ] {
            for forbidden in [
                "ATTACKER_SECRET",
                "forged event",
                "private.invalid",
                "PRIVATE",
                "\n",
                "\r",
            ] {
                assert!(!rendered.contains(forbidden));
            }
        }
    }

    #[test]
    fn classifies_native_variants_without_codes() {
        use quinn::ConnectionError;

        for (error, expected, display) in [
            (
                ConnectionError::TimedOut,
                QuicCloseReason::TimedOut,
                "timed out",
            ),
            (
                ConnectionError::Reset,
                QuicCloseReason::Reset,
                "reset by peer",
            ),
            (
                ConnectionError::LocallyClosed,
                QuicCloseReason::LocallyClosed,
                "closed locally",
            ),
            (
                ConnectionError::VersionMismatch,
                QuicCloseReason::VersionMismatch,
                "QUIC version mismatch",
            ),
            (
                ConnectionError::CidsExhausted,
                QuicCloseReason::CidsExhausted,
                "connection IDs exhausted",
            ),
        ] {
            assert_classification(&error, expected, display);
        }
    }

    #[test]
    fn application_codes_are_not_truncated_and_peer_text_is_discarded() {
        for code in [0, 0x1_0000_0001, quinn::VarInt::MAX.into_inner()] {
            let error = quinn::ConnectionError::ApplicationClosed(quinn::ApplicationClose {
                error_code: quinn::VarInt::from_u64(code).unwrap(),
                reason: UNTRUSTED_REASON.as_bytes().into(),
            });
            assert_classification(
                &error,
                QuicCloseReason::ApplicationClosed { code },
                &format!("application closed by peer (code {code})"),
            );
        }
    }

    #[test]
    fn peer_and_local_transport_errors_keep_only_codes() {
        for (transport_code, code) in [
            (quinn::TransportErrorCode::NO_ERROR, 0),
            (quinn::TransportErrorCode::PROTOCOL_VIOLATION, 10),
            (quinn::TransportErrorCode::crypto(42), 298),
        ] {
            let peer_error = quinn::ConnectionError::ConnectionClosed(quinn::ConnectionClose {
                error_code: transport_code,
                frame_type: None,
                reason: UNTRUSTED_REASON.as_bytes().into(),
            });
            assert_classification(
                &peer_error,
                QuicCloseReason::ConnectionClosed { code },
                &format!("connection closed by peer (transport code {code})"),
            );

            let mut local_error = quinn::ConnectionError::TransportError(transport_code.into());
            if let quinn::ConnectionError::TransportError(error) = &mut local_error {
                error.reason = UNTRUSTED_REASON.to_owned();
            }
            assert_classification(
                &local_error,
                QuicCloseReason::TransportError { code },
                &format!("local transport error (code {code})"),
            );
        }
    }
}
