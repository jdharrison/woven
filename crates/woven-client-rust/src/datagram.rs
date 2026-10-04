//! Pull-driven entity-state datagrams, independent of the control stream.

use std::time::Duration;

use woven_protocol::{
    Codec, CodecError, DeliveryClass, Envelope, MessagePayload, OpaquePayload, PROTOCOL_VERSION,
    RoutingPosition3D,
};

use crate::{Client, ClientError, Transport};

enum DatagramConnection {
    Quic(quinn::Connection),
    WebTransport(wtransport::Connection),
}

/// Single-owner receiver for `UnreliableSequenced` entity-state datagrams.
///
/// Obtain it once with [`Client::take_datagram_receiver`]. This handle retains a
/// connection clone, not the endpoint or a control-stream reader. Keep the client
/// and its runtime alive, and use the client's close APIs to shut down the connection.
/// There is no background reader or application inbox.
///
/// Datagrams may arrive out of order. Consumers must reject stale sequences and
/// epochs using their own bounded entity/route state; this receiver does not filter
/// or buffer them. Control-stream lifecycle messages have no cross-lane ordering
/// guarantee with datagrams.
pub struct DatagramReceiver {
    connection: DatagramConnection,
    codec: Codec,
}

impl DatagramReceiver {
    /// Receive and decode exactly one datagram as one size-prefixed protocol frame.
    ///
    /// Only valid `EntityState` envelopes with `UnreliableSequenced` delivery are
    /// accepted. Malformed, oversized, trailing-byte, or wrong-kind/class packets
    /// return [`ClientError::Protocol`] without closing the connection. The rejected
    /// packet is consumed; the next call can receive the next packet.
    ///
    /// Cancelling this future while it waits does not consume a partial frame or
    /// interfere with the control stream. No pending receive is retained by this
    /// handle. Already-buffered transport datagrams may be drained after closure.
    pub async fn recv(&mut self) -> Result<Envelope, ClientError> {
        match &self.connection {
            DatagramConnection::Quic(connection) => {
                let bytes = connection
                    .read_datagram()
                    .await
                    .map_err(|error| ClientError::Transport(error.to_string()))?;
                decode_state_datagram(&self.codec, &bytes)
            }
            DatagramConnection::WebTransport(connection) => {
                let datagram = connection
                    .receive_datagram()
                    .await
                    .map_err(|error| ClientError::Transport(error.to_string()))?;
                decode_state_datagram(&self.codec, datagram.payload().as_ref())
            }
        }
    }

    /// Receive one datagram within `duration`, returning `Ok(None)` on timeout.
    ///
    /// This timeout is cancellation-safe: it only interrupts a whole-datagram
    /// transport receive, never the framed control-stream reader. Packet errors
    /// remain errors rather than being reported as a timeout.
    pub async fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Option<Envelope>, ClientError> {
        match tokio::time::timeout(duration, self.recv()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }
}

impl Client {
    /// Submit an `UnreliableSequenced` entity-state envelope as one datagram.
    ///
    /// The server must provision the channel with matching delivery/persistence
    /// policy. `sequence` must be strictly monotone per connection × space × epoch
    /// × entity × channel; the method does not override policy or track sequences.
    /// Payloads use the negotiated outgoing codec, including the 64 KiB ceiling.
    /// The entire encoded frame must also fit the connection's current datagram
    /// budget, which can change with the path MTU.
    ///
    /// `Ok(())` means local transport submission only, not server acceptance or
    /// peer delivery. Congestion can discard this or older queued datagrams.
    /// Unsupported datagrams and oversized frames return transport errors. There
    /// is no fragmentation, truncation, stream fallback, retry, or application queue.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_unreliable_state(
        &self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
        entity_id: u64,
        sequence: u64,
        type_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.publish_unreliable_state_inner(
            namespace_id,
            session_id,
            space_id,
            space_epoch,
            channel_id,
            entity_id,
            sequence,
            type_id,
            None,
            payload,
        )
    }

    /// Queue positioned unreliable state. Position and state are applied atomically
    /// by the server; this fails locally unless the capability was negotiated.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_unreliable_positioned_state(
        &self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
        entity_id: u64,
        sequence: u64,
        type_id: u64,
        position: RoutingPosition3D,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.require_positioned_state()?;
        self.publish_unreliable_state_inner(
            namespace_id,
            session_id,
            space_id,
            space_epoch,
            channel_id,
            entity_id,
            sequence,
            type_id,
            Some(position),
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_unreliable_state_inner(
        &self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
        entity_id: u64,
        sequence: u64,
        type_id: u64,
        routing_position: Option<RoutingPosition3D>,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        let frame = self
            .outbound_codec
            .encode(&Envelope {
                protocol_version: PROTOCOL_VERSION,
                delivery_class: DeliveryClass::UnreliableSequenced,
                namespace_id,
                session_id,
                space_id,
                channel_id: Some(channel_id),
                entity_id: Some(entity_id),
                space_epoch,
                server_tick: 0,
                sender_sequence: sequence,
                correlation_id: None,
                routing_position,
                message: MessagePayload::EntityState(OpaquePayload {
                    type_id,
                    bytes: payload,
                }),
            })
            .map_err(ClientError::Protocol)?;
        match &self.transport {
            Transport::Quic { connection, .. } => {
                check_datagram_size(frame.len(), connection.max_datagram_size())?;
                connection.send_datagram(frame.into()).map_err(|error| {
                    ClientError::Transport(format!("datagram send failed: {error}"))
                })
            }
            Transport::WebTransport { connection, .. } => {
                check_datagram_size(frame.len(), connection.max_datagram_size())?;
                connection.send_datagram(&frame).map_err(|error| {
                    ClientError::Transport(format!("datagram send failed: {error}"))
                })
            }
        }
    }

    /// Take the connection's single entity-state datagram receiver.
    ///
    /// The receiver is not cloneable and does not borrow this client, so its receive
    /// can run alongside control-stream I/O. It uses the configured incoming codec,
    /// independently of negotiated outgoing limits. A second call fails even if
    /// the first receiver was dropped.
    ///
    /// Complete managed admission before calling this method. All managed admission
    /// and queue methods, including the owned cancellation runner, are permanently
    /// unavailable after handout because the receiver retains a connection clone.
    /// The client retains its endpoint and remains responsible for explicit shutdown.
    pub fn take_datagram_receiver(&mut self) -> Result<DatagramReceiver, ClientError> {
        if self.datagram_receiver_taken {
            return Err(ClientError::Transport(
                "datagram receiver has already been taken".to_owned(),
            ));
        }
        let connection = match &self.transport {
            Transport::Quic { connection, .. } => DatagramConnection::Quic(connection.clone()),
            Transport::WebTransport { connection, .. } => {
                DatagramConnection::WebTransport(connection.clone())
            }
        };
        self.datagram_receiver_taken = true;
        Ok(DatagramReceiver {
            connection,
            codec: self.codec.clone(),
        })
    }
}

fn check_datagram_size(actual: usize, maximum: Option<usize>) -> Result<(), ClientError> {
    let maximum = maximum.ok_or_else(|| {
        ClientError::Transport(
            "datagrams are unsupported by the peer or disabled locally".to_owned(),
        )
    })?;
    if actual > maximum {
        return Err(ClientError::Transport(format!(
            "datagram frame length {actual} exceeds current transport limit {maximum}"
        )));
    }
    Ok(())
}

fn decode_state_datagram(codec: &Codec, bytes: &[u8]) -> Result<Envelope, ClientError> {
    let envelope = codec.decode(bytes).map_err(ClientError::Protocol)?;
    if envelope.delivery_class != DeliveryClass::UnreliableSequenced
        || !matches!(envelope.message, MessagePayload::EntityState(_))
    {
        return Err(ClientError::Protocol(CodecError::InvalidSemantics {
            message_kind: envelope.message_kind(),
            reason: "state datagrams require UnreliableSequenced EntityState",
        }));
    }
    Ok(envelope)
}

#[cfg(test)]
mod tests;
