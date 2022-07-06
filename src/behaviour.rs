use std::{collections::{VecDeque, HashMap}, task::Poll};

use crate::{
    consensus::{DataDiscoverer, GraphConsensus},
    data_memory::DataMemory,
    handler::{Connection, ConnectionError, ConnectionSuccess, IncomingEvent},
    processor::Processor,
    protocol::{Message, RequestResponse, RequestResponsePayload, SimpleMessage},
    types::{Shard, Vid}, utils::create_shard_response,
};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, NotifyHandler};

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::core::connection::ConnectionId,
    event: Result<ConnectionSuccess, ConnectionError>,
}

pub struct Node<TConsensus, TDataMemory, TProcessor> {
    consensus: TConsensus,
    data_memory: TDataMemory,
    processor: TProcessor,

    connection_events: VecDeque<ConnectionEvent>,
    // TODO: timeout, ensure uniqueness/validiry?
    incoming_shards_buffer: HashMap<Vid, VecDeque<Shard>>
}

impl<C, D, P> Node<C, D, P> {
    pub fn new(consensus: C, data_memory: D, processor: P) -> Self {
        Self {
            consensus,
            data_memory,
            processor,
            connection_events: VecDeque::new(),
            incoming_shards_buffer: HashMap::new(),
        }
    }

    async fn retrieve_shards(id: Vid) {}
}

impl<TConsensus, TDataMemory, TProcessor> NetworkBehaviour
    for Node<TConsensus, TDataMemory, TProcessor>
where
    TConsensus: GraphConsensus + DataDiscoverer + 'static,
    TDataMemory: DataMemory<Identifier = Vid, Data = Shard> + 'static,
    TProcessor: Processor + 'static,
{
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
    ) -> std::task::Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        match self.connection_events.pop_back() {
            Some(ConnectionEvent {
                peer_id,
                connection,
                event,
            }) => {
                match event {
                    Ok(ConnectionSuccess::MessageReceived(msg)) => match msg {
                        Message::Pair(RequestResponse::Shard(payload)) => match payload {
                            RequestResponsePayload::Request(id) => {
                                let result = self.data_memory.get(&id);
                                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                    peer_id,
                                    handler: NotifyHandler::One(connection),
                                    event: IncomingEvent::SendMessage(create_shard_response(result)),
                                });
                            }
                            RequestResponsePayload::Response(shard) => {
                                self.incoming_shards_buffer[]
                            },
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
