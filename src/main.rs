use futures::prelude::*;
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::swarm::{NetworkBehaviourEventProcess, Swarm, SwarmEvent};
use libp2p::{identity, Multiaddr, NetworkBehaviour, PeerId};
use tracing::{info, debug};
use std::error::Error;
use std::time::Duration;
use types::Shard;

use crate::consensus::mock::MockConsensus;
use crate::data_memory::MemoryStorage;
use crate::processor::mock::MockProcessor;
use crate::types::Vid;

mod consensus;
mod data_memory;
mod handler;
mod instruction_memory;
mod node;
mod processor;
mod protocol;
mod types;
mod utils;

pub type Data = Shard;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let format = tracing_subscriber::fmt::format();
    tracing_subscriber::fmt().event_format(format).init();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    info!("Local peer id: {:?}", local_peer_id);

    let transport = libp2p::development_transport(local_key).await?;

    let consensus = MockConsensus::<Vid>::new();
    let data_memory = MemoryStorage::<Vid, Data>::new();
    let processor = MockProcessor {};

    #[derive(NetworkBehaviour)]
    #[behaviour(event_process = true)]
    struct CombinedBehaviour {
        main: node::Behaviour<MockConsensus<Vid>, MemoryStorage<Vid, i32>, MockProcessor>,
        mdns: Mdns,
    }

    impl NetworkBehaviourEventProcess<()> for CombinedBehaviour {
        // Called when `node` produces an event.
        fn inject_event(&mut self, _: ()) {}
    }

    impl NetworkBehaviourEventProcess<MdnsEvent> for CombinedBehaviour {
        // Called when `mdns` produces an event.
        fn inject_event(&mut self, event: MdnsEvent) {
            match event {
                MdnsEvent::Discovered(list) => {
                    for (peer, _) in list {
                        self.main.inject_peer_connected(peer);
                    }
                }
                MdnsEvent::Expired(list) => {
                    for (peer, _) in list {
                        if !self.mdns.has_node(&peer) {
                            self.main.inject_peer_disconnected(&peer);
                        }
                    }
                }
            }
        }
    }

    let main_behaviour =
        node::Behaviour::new(consensus, data_memory, processor, Duration::from_secs(5));
    let mdns = Mdns::new(Default::default()).await?;

    let behaviour = CombinedBehaviour {
        main: main_behaviour,
        mdns,
    };

    let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

    // Tell the swarm to listen on all interfaces and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = std::env::args().nth(1) {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        info!("Dialed {}", addr)
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => info!("Listening on {:?}", address),
            SwarmEvent::Behaviour(event) => info!("{:?}", event),
            other => debug!("{:?}", other),
        }
    }
}
