use libp2p::PeerId;
use libp2p_request_response::{RequestId, ResponseChannel};
use tokio::sync::mpsc::error::SendError;
use tracing::{error, trace, warn};

use crate::{module::ModuleChannelServer, protocol};

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

#[derive(Debug)]
pub enum OutEvent {
    AssignedRequestId {
        request_id: RequestId,
        request: protocol::Request,
    },
    Response {
        request_id: RequestId,
        response: protocol::Response,
    },
    IncomingRequest {
        request_id: RequestId,
        request: protocol::Request,
        channel: ResponseChannel<protocol::Response>,
    },
}

#[derive(Debug)]
pub enum InEvent {
    MakeRequest {
        request: protocol::Request,
        to: PeerId,
    },
    Respond {
        request_id: RequestId,
        channel: libp2p_request_response::ResponseChannel<protocol::Response>,
        response: protocol::Response,
    },
}

pub type Event = libp2p::request_response::Event<protocol::Request, protocol::Response>;

pub async fn handle_request_response_event(
    request_response_bus: &mut ModuleChannelServer<Module>,
    event: Event,
) -> Result<(), SendError<()>> {
    match event {
        libp2p_request_response::Event::Message { peer: _, message } => match message {
            libp2p_request_response::Message::Request {
                request_id,
                request,
                channel,
            } => {
                if (request_response_bus
                    .output
                    .send(OutEvent::IncomingRequest {
                        request_id,
                        request,
                        channel,
                    })
                    .await)
                    .is_err()
                {
                    error!("other half of `request_response_bus.output` was closed. no reason to operate without main behaviour.");
                    return Err(SendError(()));
                }
            }
            libp2p_request_response::Message::Response {
                request_id,
                response,
            } => {
                if (request_response_bus
                    .output
                    .send(OutEvent::Response {
                        request_id,
                        response,
                    })
                    .await)
                    .is_err()
                {
                    error!("other half of `request_response_bus.output` was closed. no reason to operate without main behaviour.");
                    return Err(SendError(()));
                }
            }
        },
        libp2p_request_response::Event::OutboundFailure {
            peer: _,
            request_id: _,
            error: _,
        }
        | libp2p_request_response::Event::InboundFailure {
            peer: _,
            request_id: _,
            error: _,
        } => warn!("{:?}", event),
        libp2p_request_response::Event::ResponseSent { peer, request_id } => {
            trace!("Sent request {} to {} successfully", request_id, peer)
        }
    }
    Ok(())
}
