use libp2p_request_response::ProtocolName;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum SwarmProtocolName {
    // old version with dummy consensus, had single protocol for simple and request-response messages
    Original,
    Split(SwarmProtocolNameType),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum SwarmProtocolNameType {
    Simple(SimpleVersion),
    RequestResponse(RequestResponseVersion),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum SimpleVersion {
    V1,
}

impl ProtocolName for SimpleVersion {
    fn protocol_name(&self) -> &[u8] {
        match self {
            SimpleVersion::V1 => b"/p2p/the_swarm_computer/simple/0.0.1",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum RequestResponseVersion {
    V1,
}

impl ProtocolName for RequestResponseVersion {
    fn protocol_name(&self) -> &[u8] {
        match self {
            RequestResponseVersion::V1 => b"/p2p/the_swarm_computer/request_response/0.0.1",
        }
    }
}

impl ProtocolName for SwarmProtocolName {
    fn protocol_name(&self) -> &[u8] {
        match self {
            SwarmProtocolName::Original => b"/p2p/the_swarm_computer/0.0.1",
            SwarmProtocolName::Split(SwarmProtocolNameType::Simple(simple)) => {
                simple.protocol_name()
            }
            SwarmProtocolName::Split(SwarmProtocolNameType::RequestResponse(request_response)) => {
                request_response.protocol_name()
            }
        }
    }
}
