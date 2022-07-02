use futures::{future, io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{core::UpgradeInfo, swarm::NegotiatedSubstream, InboundUpgrade, OutboundUpgrade};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Display, iter};
use void::Void;

use crate::types::{Graph, Shard, Vid};

// TODO: do not forget to make static size of values small
// (Clippy should report though)
/// Payloads transferred through the protocol
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Message {
    Pair(RequestResponse),
    Single(SimpleMessage),
}

// Messages that are expected to work in a request-response manner
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum RequestResponse {
    Shard(RequestResponsePayload<Vid, Option<Shard>>),
}

// Messages that do not expect any response back
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum SimpleMessage {
    GossipGraph(Graph),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum RequestResponsePayload<TRequest, TResponse> {
    Request(TRequest),
    Response(TResponse),
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
    DeserializationFailed(Box<bincode::ErrorKind>),
    Io(io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::LengthTooBig => {
                write!(f, "Reported payload length is too large for this machine")
            }
            Error::DeserializationFailed(e) => write!(f, "Could not deserialize: {}", e),
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
        let mut message_length: [u8; 8] = [0; 8];
        stream
            .read_exact(&mut message_length)
            .await
            .map_err(Error::Io)?;
        let message_length: usize = u64::from_be_bytes(message_length)
            .try_into()
            .map_err(|_| Error::LengthTooBig)?;
        let mut buf = vec![0u8; message_length];
        stream.read_exact(&mut buf).await.map_err(Error::Io)?;
        let msg = bincode::deserialize(&buf).map_err(Error::DeserializationFailed)?;
        Ok((stream, msg))
    }

    pub async fn send_message<S, R>(mut stream: S, msg: &R) -> Result<S, Error>
    where
        S: AsyncWrite + Unpin,
        R: Serialize,
    {
        let serialized = bincode::serialize(&msg).unwrap();
        let payload_size = serialized.len().try_into().unwrap();
        stream
            .write_all(&u64::to_be_bytes(payload_size))
            .await
            .map_err(Error::Io)?;
        stream.write_all(&serialized).await.map_err(Error::Io)?;
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_one_shot_stream(msg: &Message) -> Vec<u8> {
        let mut serialized = bincode::serialize(&msg).expect("serialization failed");
        let payload_size = serialized
            .len()
            .try_into()
            .expect("payload doesn't fit in u64");
        let mut stream_to_send = u64::to_be_bytes(payload_size).to_vec();
        stream_to_send.append(&mut serialized);
        stream_to_send
    }

    fn sample_messages() -> Vec<Message> {
        vec![
            Message::Pair(RequestResponse::Shard(RequestResponsePayload::Request(
                Vid(1234),
            ))),
            Message::Pair(RequestResponse::Shard(RequestResponsePayload::Response(
                Some(Shard(1337)),
            ))),
            Message::Single(SimpleMessage::GossipGraph(Graph {
                some_data: "abobus sus among us".to_owned(),
            })),
        ]
    }

    #[tokio::test]
    async fn reads_requests_correctly() {
        let messages = sample_messages();
        for m in messages {
            let stream = mock_one_shot_stream(&m);
            let (slice, accepted_msg): (&[u8], Message) =
                SwarmComputerProtocol::receive_message(&stream[..])
                    .await
                    .expect("read failed");
            assert!(slice.len() == 0);
            assert!(accepted_msg == m);
        }
    }

    #[tokio::test]
    async fn writes_reads_correctly() {
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
            let (updated_stream, accepted_msg): (&[u8], Message) =
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
