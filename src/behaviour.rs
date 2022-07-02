use crate::handler::Connection;
use libp2p::swarm::NetworkBehaviour;

pub struct Node {}

impl NetworkBehaviour for Node {
    type ConnectionHandler = Connection;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        Connection::new()
    }

    fn inject_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler as libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        todo!()
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        libp2p::swarm::NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>,
    > {
        todo!()
    }
}
