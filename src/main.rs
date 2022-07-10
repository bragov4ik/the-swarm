use clap::Parser;
use futures::StreamExt;
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::swarm::{NetworkBehaviourEventProcess, SwarmEvent, SwarmBuilder};
use libp2p::{identity, Multiaddr, NetworkBehaviour, PeerId};
use std::error::Error;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{debug, error, info};
use types::Shard;

use crate::consensus::mock::MockConsensus;
use crate::data_memory::MemoryStorage;
use crate::processor::mock::MockProcessor;
use crate::types::Vid;

mod consensus;
mod data_memory;
mod demo_input;
mod handler;
mod instruction_memory;
mod node;
mod processor;
mod protocol;
mod types;
mod utils;

pub type Data = Shard;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Is this node a main one (that does all the stuff for demo)
    #[clap(short, long)]
    is_main: bool,

    #[clap(long)]
    generate_input: bool,

    #[clap(short, long)]
    dial_address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let format = tracing_subscriber::fmt::format();
    tracing_subscriber::fmt::init();

    let args = Args::parse();

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
                        self.main.inject_peer_discovered(peer);
                    }
                }
                MdnsEvent::Expired(list) => {
                    for (peer, _) in list {
                        if !self.mdns.has_node(&peer) {
                            self.main.inject_peer_expired(&peer);
                        }
                    }
                }
            }
        }
    }

    let main_behaviour = node::Behaviour::new(
        consensus,
        data_memory,
        processor,
        Duration::from_secs(5),
        args.is_main,
    );
    let mdns = Mdns::new(Default::default()).await?;

    let behaviour = CombinedBehaviour {
        main: main_behaviour,
        mdns,
    };

    let mut swarm = {
        SwarmBuilder::new(transport, behaviour, local_peer_id)
            // We want the connection background tasks to be spawned
            // onto the tokio runtime.
            .executor(Box::new(|fut| {
                tokio::spawn(fut);
            }))
            .build()    
    };

    // Tell the swarm to listen on all interfaces and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = args.dial_address {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        info!("Dialed {}", addr)
    }

    // TODO: remove, for demo only
    if args.is_main {
        info!("This is the main node");
        if args.generate_input {
            info!("Writing test input");
            demo_input::test_write_input("src/demo_input/input.json")?;
            info!("Wrote successfully");
        } else {
            info!("Reading test input");
            let input = demo_input::read_input("src/demo_input/input.json").map_err(|e| {
                error!("Failed to read input data: {:?}", e);
                e
            })?;
            for (id, data) in input.data_layout {
                swarm
                    .behaviour_mut()
                    .main
                    .add_data_to_distribute(id, data)
                    .expect("Just checked that node is main");
            }
            for instruction in input.instructions {
                swarm
                    .behaviour_mut()
                    .main
                    .add_instruction(instruction)
                    .expect("Just checked that node is main");
            }
            info!("Read input and added it successfully!");
        }
    }

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    loop {
        tokio::select! {
            line = stdin.next_line() => {
                let line = line?.expect("stdin closed");
                match &line[..] {
                    "read all" => {
                        let data = swarm.behaviour().main.read_all_local();
                        info!("All local state:\n{:?}", data);
                    },
                    "distribute" => {
                        info!("Starting distributing the vectors");
                        swarm.behaviour_mut().main.allow_distribution();
                    },
                    "execute" => {
                        info!("Starting executing instructions");
                        swarm.behaviour_mut().main.allow_execution();
                    },
                    other => info!("Can't recognize command '{}'", other),
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => info!("Listening on {:?}", address),
                    SwarmEvent::Behaviour(event) => info!("{:?}", event),
                    other => debug!("{:?}", other),
                }
            }
        }
    }
}
