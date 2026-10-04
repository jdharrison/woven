//! One bounded control frame retained across receive cancellation.

use tokio::io::{AsyncRead, AsyncReadExt};
use woven_protocol::{Codec, Envelope};

use crate::ClientError;

#[derive(Default)]
pub(crate) struct StreamReadState {
    prefix: [u8; 4],
    prefix_read: usize,
    frame: Vec<u8>,
    frame_read: usize,
}

impl StreamReadState {
    pub(crate) async fn recv<S: AsyncRead + Unpin>(
        &mut self,
        stream: &mut S,
        codec: &Codec,
    ) -> Result<Envelope, ClientError> {
        // `read` is cancel-safe for both transport AsyncRead implementations. Each
        // cursor is committed before the next await, and lives on Client, not here.
        while self.prefix_read < self.prefix.len() {
            self.prefix_read += read_some(stream, &mut self.prefix[self.prefix_read..]).await?;
        }
        if self.frame.is_empty() {
            let frame_len = codec
                .expected_frame_len(&self.prefix)
                .map_err(ClientError::Protocol)?
                .ok_or_else(|| ClientError::Transport("incomplete size prefix".to_owned()))?;
            self.frame = vec![0; frame_len];
            self.frame[..self.prefix.len()].copy_from_slice(&self.prefix);
            self.frame_read = self.prefix.len();
        }
        while self.frame_read < self.frame.len() {
            self.frame_read += read_some(stream, &mut self.frame[self.frame_read..]).await?;
        }
        let result = codec.decode(&self.frame).map_err(ClientError::Protocol);
        *self = Self::default();
        result
    }
}

async fn read_some<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut [u8],
) -> Result<usize, ClientError> {
    let count = stream
        .read(buffer)
        .await
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    if count == 0 {
        return Err(ClientError::Transport(
            "control stream ended before a complete frame arrived".to_owned(),
        ));
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncWriteExt, DuplexStream};
    use woven_protocol::{
        CodecError, CodecLimits, ControlPayload, DeliveryClass, OpaquePayload, Pong,
    };

    use super::*;

    const POLL_TIMEOUT: Duration = Duration::from_millis(1);

    fn pong() -> Envelope {
        Envelope::control(
            DeliveryClass::ReliableUnordered,
            ControlPayload::Pong(Pong {
                nonce: 1,
                sender_time_micros: 2,
                responder_time_micros: 3,
            }),
        )
    }

    async fn poll_partial(reader: &mut StreamReadState, stream: &mut DuplexStream, codec: &Codec) {
        assert!(
            tokio::time::timeout(POLL_TIMEOUT, reader.recv(stream, codec))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn partial_prefix_and_body_remain_in_the_reader_after_cancellation() {
        let codec = Codec::default();
        let frame = codec.encode(&pong()).unwrap();
        let (mut input, mut output) = tokio::io::duplex(512);
        let mut reader = StreamReadState::default();
        output.write_all(&frame[..1]).await.unwrap();
        poll_partial(&mut reader, &mut input, &codec).await;
        assert_eq!(reader.prefix_read, 1);
        assert!(reader.frame.is_empty());
        output.write_all(&frame[1..3]).await.unwrap();
        poll_partial(&mut reader, &mut input, &codec).await;
        assert_eq!(reader.prefix_read, 3);
        output.write_all(&frame[3..4]).await.unwrap();
        poll_partial(&mut reader, &mut input, &codec).await;
        assert_eq!(reader.prefix_read, 4);
        assert_eq!(reader.frame_read, 4);
        assert_eq!(reader.frame.len(), frame.len());
        output.write_all(&frame[4..11]).await.unwrap();
        poll_partial(&mut reader, &mut input, &codec).await;
        assert_eq!(reader.frame_read, 11);
        output.write_all(&frame[11..]).await.unwrap();
        assert_eq!(reader.recv(&mut input, &codec).await.unwrap(), pong());
        assert_eq!(reader.prefix_read, 0);
        assert!(reader.frame.is_empty());
        output.write_all(&frame).await.unwrap();
        assert_eq!(reader.recv(&mut input, &codec).await.unwrap(), pong());
    }

    #[tokio::test]
    async fn oversized_prefix_is_rejected_before_body_allocation_and_remains_rejected() {
        let codec = Codec::new(CodecLimits::new(512, 128).unwrap()).unwrap();
        let (mut input, mut output) = tokio::io::duplex(16);
        let mut reader = StreamReadState::default();
        output.write_all(&1_024_u32.to_le_bytes()).await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                reader.recv(&mut input, &codec).await,
                Err(ClientError::Protocol(CodecError::FrameTooLarge {
                    actual: 1_028,
                    maximum: 512
                }))
            ));
            assert_eq!(reader.prefix_read, 4);
            assert_eq!(reader.frame.capacity(), 0);
        }
    }

    #[tokio::test]
    async fn complete_frames_still_use_the_configured_payload_limit() {
        let codec = Codec::new(CodecLimits::new(512, 128).unwrap()).unwrap();
        let mut event = Envelope::reliable_event(
            DeliveryClass::ReliableOrdered,
            OpaquePayload {
                type_id: 1,
                bytes: vec![0; 129],
            },
        );
        event.namespace_id = 1;
        event.session_id = 1;
        event.space_id = 1;
        event.space_epoch = 1;
        event.channel_id = Some(1);
        let frame = Codec::default().encode(&event).unwrap();
        let (mut input, mut output) = tokio::io::duplex(1_024);
        let mut reader = StreamReadState::default();
        output.write_all(&frame).await.unwrap();
        assert!(matches!(
            reader.recv(&mut input, &codec).await,
            Err(ClientError::Protocol(CodecError::PayloadTooLarge {
                actual: 129,
                maximum: 128
            }))
        ));
        assert!(reader.frame.is_empty());
        output
            .write_all(&codec.encode(&pong()).unwrap())
            .await
            .unwrap();
        assert_eq!(reader.recv(&mut input, &codec).await.unwrap(), pong());
    }
}
