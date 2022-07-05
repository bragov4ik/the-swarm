use std::{collections::VecDeque, task::Poll};

use crate::{
    handler::{Connection, ConnectionError, ConnectionSuccess},
    protocol::{Message, RequestResponse, RequestResponsePayload, SimpleMessage},
};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction};

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::core::connection::ConnectionId,
    event: Result<ConnectionSuccess, ConnectionError>,
}

pub struct Node {
    connection_events: VecDeque<ConnectionEvent>,
}

impl NetworkBehaviour for Node {
    type ConnectionHandler = Connection;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        Connection::new(10)
    }

    fn inject_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: Result<ConnectionSuccess, ConnectionError>,
    ) {
        self.connection_events.push_front(ConnectionEvent {
            peer_id,
            connection,
            event,
        });
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>,
    > {
        match self.connection_events.pop_back() {
            Some(ConnectionEvent {
                peer_id,
                connection,
                event,
            }) => {
                match event {
                    Ok(ConnectionSuccess::MessageReceived(msg)) => match msg {
                        Message::Pair(RequestResponse::Shard(payload)) => match payload {
                            RequestResponsePayload::Request(id) => todo!(),
                            RequestResponsePayload::Response(shard) => todo!(),
                        },
                        Message::Single(SimpleMessage::GossipGraph(graph)) => todo!(),
                    },
                    Err(ConnectionError::PeerUnsupported) => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection: libp2p::swarm::CloseConnection::One(connection),
                        })
                    }
                    // do something idk, save stats mb
                    Err(ConnectionError::Timeout) => todo!(),
                    Err(ConnectionError::Other(err)) => {
                        // log error
                    }
                }
            }
            None => {}
        }
        Poll::Pending
    }
}
