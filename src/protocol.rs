use futures::{future, io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{core::UpgradeInfo, swarm::NegotiatedSubstream, InboundUpgrade, OutboundUpgrade};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{trace, debug};
use std::{fmt::Display, iter};
use void::Void;

use crate::{types::{Shard, Vid, Graph}};

// TODO: do not forget to make static size of values small
// (Clippy should report though)
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Request {
    Shard(Vid),
}

// TODO: do not forget to make static size of values small
// (Clippy should report though)
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Response {
    Shard(Option<Shard>)
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Simple {
    GossipGraph(Graph),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Primary {
    Request(Request),
    Simple(Simple),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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
    pub async fn receive_message<S, R>(mut stream: S) -> Result<(S, R), Error>
    where
        S: AsyncRead + Unpin,
        R: DeserializeOwned,
    {
        debug!("Receiving next message from the stream");
        trace!("Getting length of the next message");
        let mut message_length: [u8; 8] = [0; 8];
        stream
            .read_exact(&mut message_length)
            .await
            .map_err(Error::Io)?;
        trace!("Received bytes successfully");
        let message_length: usize = u64::from_be_bytes(message_length)
            .try_into()
            .map_err(|_| Error::LengthTooBig)?;
        trace!("Receiving {} bytes of payload", message_length);
        let mut buf = vec![0u8; message_length];
        stream.read_exact(&mut buf).await.map_err(Error::Io)?;
        trace!("Deserializing the payload");
        let msg = bincode::deserialize(&buf).map_err(Error::SerializationFailed)?;
        debug!("Received the message successfully!");
        Ok((stream, msg))
    }

    pub async fn send_message<S, R>(mut stream: S, msg: R) -> Result<S, Error>
    where
        S: AsyncWrite + Unpin,
        R: Serialize,
    {
        debug!("Sending next message to the stream");
        trace!("Serializing");
        let serialized = bincode::serialize(&msg).map_err(Error::SerializationFailed)?;
        let payload_size = serialized.len().try_into().map_err(|_| Error::LengthTooBig)?;
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

    #[derive(Serialize, Deserialize, PartialEq)]
    enum TestMessage {
        Variant,
        VariantWithData(String),
        NestedVariant(SomeEnum)
    }

    #[derive(Serialize, Deserialize, PartialEq)]
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
            assert!(slice.len() == 0);
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
        assert!(stream_left.len() == 0);
    }
}
