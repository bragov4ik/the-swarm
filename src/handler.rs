use libp2p::swarm::{ConnectionHandler, ConnectionHandlerSelect, NetworkBehaviour};

use crate::protocol::{one_shot::SwarmOneShot, request_response::SwarmRequestResponse};

pub type SwarmComputerProtocol = ConnectionHandlerSelect<SwarmOneShot, RequestResponseHandler>;
type RequestResponseHandler = <libp2p_request_response::Behaviour<SwarmRequestResponse> as NetworkBehaviour>::ConnectionHandler;

pub type ToSwarmEvent = <SwarmComputerProtocol as ConnectionHandler>::OutEvent;
pub type ToSwarmError = <SwarmComputerProtocol as ConnectionHandler>::Error;

pub fn new(request_response: RequestResponseHandler) -> SwarmComputerProtocol {
    let one_shot = SwarmOneShot::default();
    ConnectionHandler::select(one_shot, request_response)
}
