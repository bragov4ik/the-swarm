use std::{iter, pin::Pin};

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, Future};
use libp2p::{
    core::{upgrade, UpgradeInfo},
    swarm::OneShotHandler,
    InboundUpgrade, OutboundUpgrade,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{versions, Simple};

pub type SwarmOneShot = OneShotHandler<SimpleMessageReceiver, SimpleMessage, InnerMessage>;

#[derive(Debug, Clone)]
pub struct SimpleMessageReceiver;

impl Default for SimpleMessageReceiver {
    fn default() -> Self {
        Self
    }
}

#[derive(Error, Debug)]
pub enum SimpleMessageReceiverError {
    #[error("Error writing or reading data")]
    Io(#[from] std::io::Error),
    #[error("Error writing or reading data")]
    Serialization(#[from] bincode::Error),
}

impl UpgradeInfo for SimpleMessageReceiver {
    type Info = versions::SimpleVersion;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(versions::SimpleVersion::V1)
    }
}

impl<TSocket> InboundUpgrade<TSocket> for SimpleMessageReceiver
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = SimpleMessage;
    type Error = SimpleMessageReceiverError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = upgrade::read_length_prefixed(&mut socket, 1024 * 1024).await?;
            let response = bincode::deserialize(&bytes)?;
            Ok(response)
        })
    }
}

/// Acts as a received message and a "handle" for
/// sending the message
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SimpleMessage(pub Simple);

impl From<Simple> for SimpleMessage {
    fn from(value: Simple) -> Self {
        Self(value)
    }
}

/// for `OneShotHandler`
#[derive(Debug)]
pub enum InnerMessage {
    /// We received an message from a remote.
    Rx(SimpleMessage),
    /// We successfully sent an message.
    Sent,
}

impl From<SimpleMessage> for InnerMessage {
    fn from(value: SimpleMessage) -> Self {
        InnerMessage::Rx(value)
    }
}

impl From<()> for InnerMessage {
    fn from(_: ()) -> Self {
        InnerMessage::Sent
    }
}

#[derive(Error, Debug)]
pub enum SimpleMessageSendError {
    #[error("Error writing or reading data")]
    Io(#[from] std::io::Error),
    #[error("Error writing or reading data")]
    Serialization(#[from] bincode::Error),
}

impl UpgradeInfo for SimpleMessage {
    type Info = versions::SimpleVersion;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(versions::SimpleVersion::V1)
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for SimpleMessage
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = SimpleMessageSendError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = bincode::serialize(&self)?;
            upgrade::write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await?;
            Ok(())
        })
    }
}
