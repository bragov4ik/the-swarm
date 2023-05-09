//! Message passing protocol. Updating interface (format of messages sent
//! across stream) should also increase protocol version number (see [`protocol_info`]).
//!
//! Used as protocol for libp2p [ConnectionHandler](https://docs.rs/libp2p/latest/libp2p/swarm/trait.ConnectionHandler.html)
//!
//! Also note that type `R` in corresponding `send_message` and `receive_message`
//! pairs should be the same. Otherwise, deserialization on receiving side
//! will fail.

use futures::{future, io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{core::UpgradeInfo, swarm::NegotiatedSubstream, InboundUpgrade, OutboundUpgrade};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Display, iter};
use tracing::{debug, instrument, trace};
use void::Void;

use crate::types::{GraphSync, Shard, Sid, Vid};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Hash)]
pub enum Request {
    /// "Give me a shard `Sid` for data `Vid` pls".
    /// For purposes of rebuilding data.
    GetShard((Vid, Sid)),
    /// "I want to store this shard that you distribute, please serve it to me".
    ServeShard((Vid, Sid)),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Response {
    /// Shard that was requested (or its absence)
    GetShard(Option<Shard>),
    /// Shard that was requested (or `None` if shard `Vid, Sid` is not currently in distribution)
    ServeShard(Option<Shard>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Simple {
    /// Graph state update according to consensus
    GossipGraph(GraphSync),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Primary {
    Request(Request),
    Simple(Simple),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Message {
    // Each request in primary should have corresponding response
    Primary(Primary),
    Secondary(Response),
}

pub struct SwarmComputerProtocol;

impl UpgradeInfo for SwarmComputerProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/p2p/the_swarm_computer/0.0.1")
    }
}

impl InboundUpgrade<NegotiatedSubstream> for SwarmComputerProtocol {
    type Output = NegotiatedSubstream;
    type Error = Void;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        future::ok(stream)
    }
}

impl OutboundUpgrade<NegotiatedSubstream> for SwarmComputerProtocol {
    type Output = NegotiatedSubstream;
    type Error = Void;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        future::ok(stream)
    }
}

#[derive(Debug)]
pub enum Error {
    /// Could not parse the message as payload is too big for this machine
    LengthTooBig,
    SerializationFailed(Box<bincode::ErrorKind>),
    Io(io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::LengthTooBig => {
                write!(f, "Reported payload length is too large for this machine")
            }
            Error::SerializationFailed(e) => write!(f, "Serialization error: {}", e),
            Error::Io(e) => write!(f, "IO failure: {}", e),
        }
    }
}

impl std::error::Error for Error {}

impl SwarmComputerProtocol {
    #[instrument(skip_all, fields(payload_size))]
    pub async fn receive_message<S, R>(mut stream: S) -> Result<(S, R), Error>
    where
        S: AsyncRead + Unpin,
        R: DeserializeOwned + std::fmt::Debug,
    {
        debug!("Receiving next message from the stream");
        trace!("Getting length of the next message");
        let mut payload_size: [u8; 8] = [0; 8];
        stream
            .read_exact(&mut payload_size)
            .await
            .map_err(Error::Io)?;
        trace!("Received bytes successfully");
        let payload_size: usize = u64::from_be_bytes(payload_size)
            .try_into()
            .map_err(|_| Error::LengthTooBig)?;
        tracing::Span::current().record("payload_size", &payload_size);
        trace!("Receiving {} bytes of payload", payload_size);
        let mut buf = vec![0u8; payload_size];
        stream.read_exact(&mut buf).await.map_err(Error::Io)?;
        trace!("Deserializing the payload");
        let msg = bincode::deserialize(&buf).map_err(Error::SerializationFailed)?;
        trace!("Message: {:?}", msg);
        debug!("Received the message successfully!");
        Ok((stream, msg))
    }

    #[instrument(skip_all, fields(payload_size))]
    pub async fn send_message<S, R>(mut stream: S, msg: R) -> Result<S, Error>
    where
        S: AsyncWrite + Unpin,
        R: Serialize + std::fmt::Debug,
    {
        debug!("Sending next message to the stream");
        trace!("Message: {:?}", msg);
        trace!("Serializing");
        let serialized = bincode::serialize(&msg).map_err(Error::SerializationFailed)?;
        let payload_size = serialized
            .len()
            .try_into()
            .map_err(|_| Error::LengthTooBig)?;
        tracing::Span::current().record("payload_size", &payload_size);
        trace!("Encoded length: {} bytes", payload_size);
        trace!("Sending length");
        stream
            .write_all(&u64::to_be_bytes(payload_size))
            .await
            .map_err(Error::Io)?;
        trace!("Sending payload");
        stream.write_all(&serialized).await.map_err(Error::Io)?;
        debug!("Sent the message successfully");
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum TestMessage {
        Variant,
        VariantWithData(String),
        NestedVariant(SomeEnum),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum SomeEnum {
        A,
        B(i32),
    }

    fn mock_one_shot_stream(msg: &TestMessage) -> Vec<u8> {
        let mut serialized = bincode::serialize(&msg).expect("serialization failed");
        let payload_size = serialized
            .len()
            .try_into()
            .expect("payload doesn't fit in u64");
        let mut stream_to_send = u64::to_be_bytes(payload_size).to_vec();
        stream_to_send.append(&mut serialized);
        stream_to_send
    }

    fn sample_messages() -> Vec<TestMessage> {
        vec![
            TestMessage::Variant,
            TestMessage::VariantWithData("abobus sus among us".to_owned()),
            TestMessage::NestedVariant(SomeEnum::A),
            TestMessage::NestedVariant(SomeEnum::B(1337)),
        ]
    }

    #[tokio::test]
    async fn reads_requests_correctly() {
        let messages = sample_messages();
        for m in messages {
            let stream = mock_one_shot_stream(&m);
            let (slice, accepted_msg): (&[u8], TestMessage) =
                SwarmComputerProtocol::receive_message(&stream[..])
                    .await
                    .expect("read failed");
            assert!(slice.is_empty());
            assert!(accepted_msg == m);
        }
    }

    #[tokio::test]
    async fn writes_then_reads_correctly() {
        let messages = sample_messages();

        // Send messages to stream (vector of bytes)
        let mut stream = vec![];
        for m in &messages {
            stream = SwarmComputerProtocol::send_message(stream, m)
                .await
                .expect("write failed");
        }

        // Read messages from the vector
        let mut stream_left = &stream[..];
        for m in &messages {
            let (updated_stream, accepted_msg): (&[u8], TestMessage) =
                SwarmComputerProtocol::receive_message(stream_left)
                    .await
                    .expect("write failed");
            stream_left = updated_stream;
            assert!(accepted_msg == *m);
        }

        // We should have read everything
        assert!(stream_left.is_empty());
    }
}
