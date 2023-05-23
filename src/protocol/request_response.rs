use async_trait::async_trait;
use futures::{io, AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::upgrade;
use thiserror::Error;

use super::{versions::RequestResponseVersion, Request, Response};

#[derive(Debug, Clone)]
pub struct SwarmRequestResponse;

impl SwarmRequestResponse {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error writing or reading data")]
    Io(#[from] io::Error),
    #[error("Error writing or reading data")]
    Serialization(#[from] bincode::Error),
}

// from https://github.com/libp2p/rust-libp2p/blob/fd53245889abcec2cd421a9193abab3344483210/protocols/autonat/src/protocol.rs#L38
#[async_trait]
impl libp2p_request_response::Codec for SwarmRequestResponse {
    type Protocol = RequestResponseVersion;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let bytes = upgrade::read_length_prefixed(io, 1024 * 1024).await?;
        let request = bincode::deserialize(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        let bytes = upgrade::read_length_prefixed(io, 1024 * 1024).await?;
        let response = bincode::deserialize(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        data: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes =
            bincode::serialize(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        data: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes =
            bincode::serialize(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        io.close().await
    }
}
