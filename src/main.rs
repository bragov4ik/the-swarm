use clap::Parser;
use consensus::graph::GenesisPayload;
use futures::StreamExt;
use libp2p::mdns;
use libp2p::swarm::{NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{identity, Multiaddr, PeerId};
use rand::RngCore;
use rust_hashgraph::algorithm::datastructure::Graph;
use std::error::Error;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{debug, error, info};
use types::Shard;

use crate::consensus::graph::{EventPayload, GraphWrapper};
use crate::data_memory::DataMemory;
use crate::processor::single_threaded::ShardProcessor;
use crate::types::{Sid, Vid};

mod behaviour;
mod consensus;
mod data_memory;
mod demo_input;
mod encoding;
mod handler;
mod instruction_storage;
mod processor;
mod protocol;
mod signatures;
mod types;

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

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CombinedBehaviourEvent")]
struct CombinedBehaviour {
    // Main logic
    // main: behaviour::Behaviour<MockConsensus<Vid>, MemoryStorage<Vid, Shard>, MockProcessor>,
    // MDNS performs LAN node discovery, allows not to manually write peer addresses
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug)]
// TODO: add Main event and check if applies
#[allow(clippy::large_enum_variant)]
enum CombinedBehaviourEvent {
    Main(()),
    Mdns(mdns::Event),
}

impl From<()> for CombinedBehaviourEvent {
    fn from(event: ()) -> Self {
        CombinedBehaviourEvent::Main(event)
    }
}

impl From<mdns::Event> for CombinedBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        CombinedBehaviourEvent::Mdns(event)
    }
}

pub trait Module {
    type InEvent;
    type OutEvent;
    type SharedState: State;
}

pub trait State {
    fn accepts_input(&self) -> bool;
}

impl State for () {
    fn accepts_input(&self) -> bool {
        true
    }
}

struct Ed25519Signer {
    inner: libp2p::identity::ed25519::Keypair,
}

impl Ed25519Signer {
    pub fn new(keypair: libp2p::identity::ed25519::Keypair) -> Self {
        Self { inner: keypair }
    }
}

impl rust_hashgraph::algorithm::Signer<GenesisPayload> for Ed25519Signer {
    type SignerIdentity = PeerId;

    fn sign(
        &self,
        hash: &rust_hashgraph::algorithm::event::Hash,
    ) -> rust_hashgraph::algorithm::event::Signature {
        let signature_bytes: [u8; 64] = self
            .inner
            .sign(hash.as_ref())
            .try_into()
            .expect("signature failure");
        rust_hashgraph::algorithm::event::Signature(
            rust_hashgraph::algorithm::event::Hash::from_array(signature_bytes),
        )
    }

    fn verify(
        &self,
        hash: &rust_hashgraph::algorithm::event::Hash,
        signature: &rust_hashgraph::algorithm::event::Signature,
        identity: &Self::SignerIdentity,
        genesis_payload: &GenesisPayload,
    ) -> bool {
        let public_key = genesis_payload.pubkey.clone().into();
        identity.is_public_key(&public_key).unwrap_or(false);
        public_key.verify(hash.as_ref(), signature.0.as_ref())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let format = tracing_subscriber::fmt::format();
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let local_keypair = libp2p::identity::Keypair::generate_ed25519();
    let local_ed25519_keypair = local_keypair
        .clone()
        .into_ed25519()
        .expect("Just created this variant");
    let local_peer_id = PeerId::from(local_keypair.public());
    info!("Local peer id: {:?}", local_peer_id);

    let transport = libp2p::development_transport(local_keypair).await?;

    let signer = Ed25519Signer::new(local_ed25519_keypair.clone());
    let consensus_graph = Graph::new(
        local_peer_id,
        EventPayload::<Vid, Sid>::new(vec![]),
        GenesisPayload {
            pubkey: local_ed25519_keypair.public().into(),
        },
        30,
        signer,
        (),
    );

    // let main_behaviour = behaviour::Behaviour::new(
    //     local_peer_id,
    //     Duration::from_secs(5),
    //     args.is_main,
    // );
    let mdns = mdns::async_io::Behaviour::new(Default::default(), local_peer_id)?;

    let behaviour = CombinedBehaviour {
        // main: main_behaviour,
        mdns,
    };

    // We want the connection background tasks to be spawned
    // onto the tokio runtime.
    let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build();

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
            // for (id, data) in input.data_layout {
            //     swarm
            //         .behaviour_mut()
            //         .main
            //         .add_data_to_distribute(id, data)
            //         .expect("Just checked that node is main");
            // }
            // for instruction in input.instructions {
            //     swarm
            //         .behaviour_mut()
            //         .main
            //         .add_instruction(instruction)
            //         .expect("Just checked that node is main");
            // }
            info!("Read input and added it successfully!");
        }
    }

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    //     loop {
    //         futures::select! {
    //             line = stdin.next_line() => {
    //                 let line = line?.expect("stdin closed");
    //                 match &line[..] {
    //                     "read all" => {
    //                         let data = swarm.behaviour().main.read_all_local();
    //                         info!("All local state:\n{:?}", data);
    //                     },
    //                     "distribute" => {
    //                         info!("Starting distributing the vectors");
    //                         swarm.behaviour_mut().main.allow_distribution();
    //                     },
    //                     "execute" => {
    //                         info!("Starting executing instructions");
    //                         swarm.behaviour_mut().main.allow_execution();
    //                     },
    //                     "help" => {
    //                         info!("Available commands:
    // read all - Print all data stored locally in the node
    // distribute - Distribute initial data across nodes randomly
    // execute - Add initial instructions to the execution schedule");
    //                     }
    //                     other => info!("Can't recognize command '{}'", other),
    //                 }
    //             }
    //             event = swarm.select_next_some() => {
    //                 match event {
    //                     SwarmEvent::NewListenAddr { address, .. } => info!("Listening on {:?}", address),
    //                     SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(
    //                         mdns::Event::Discovered(list)
    //                     )) => {
    //                         for (peer, _) in list {
    //                             swarm.behaviour_mut().main.inject_peer_discovered(peer);
    //                         }
    //                     }
    //                     SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(
    //                         mdns::Event::Expired(list)
    //                     )) => {
    //                         for (peer, _) in list {
    //                             if !swarm.behaviour_mut().mdns.has_node(&peer) {
    //                                 swarm.behaviour_mut().main.inject_peer_expired(&peer);
    //                             }
    //                         }
    //                     }
    //                     SwarmEvent::Behaviour(event) => info!("{:?}", event),
    //                     other => debug!("{:?}", other),
    //                 }
    //             }
    //         }
    //    }
    Ok(())
}
