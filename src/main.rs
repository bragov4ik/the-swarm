use clap::Parser;
use consensus::graph::GenesisPayload;
use futures::StreamExt;
use libp2p::mdns;
use libp2p::swarm::{NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{identity, Multiaddr, PeerId};
use rand::RngCore;
use rust_hashgraph::algorithm::datastructure::Graph;
use signatures::Ed25519Signer;
use std::error::Error;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use types::Shard;

use crate::consensus::graph::{EventPayload, GraphWrapper};
use crate::data_memory::distributed_simple::{DistributedDataMemory, MemoryBus};
use crate::data_memory::DataMemory;
use crate::instruction_storage::InstructionMemory;
use crate::module::ModuleChannelServer;
use crate::processor::single_threaded::ShardProcessor;
use crate::types::{Sid, Vid};

mod behaviour;
mod consensus;
mod data_memory;
mod demo_input;
mod encoding;
mod handler;
mod instruction_storage;
mod module;
mod processor;
mod protocol;
mod signatures;
mod types;
mod ui;

const CHANNEL_BUFFER_LIMIT: usize = 100;
//todo move to spec file
const ENCODING_SETTINGS: encoding::reed_solomon::Settings = encoding::reed_solomon::Settings {
    data_shards_total: 3,
    data_shards_sufficient: 2,
};

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
    main: behaviour::Behaviour,
    // MDNS performs LAN node discovery, allows not to manually write peer addresses
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug)]
// TODO: add Main event and check if applies
#[allow(clippy::large_enum_variant)]
enum CombinedBehaviourEvent {
    Main(behaviour::ToSwarmEvent),
    Mdns(mdns::Event),
}

impl From<behaviour::ToSwarmEvent> for CombinedBehaviourEvent {
    fn from(event: behaviour::ToSwarmEvent) -> Self {
        CombinedBehaviourEvent::Main(event)
    }
}

impl From<mdns::Event> for CombinedBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        CombinedBehaviourEvent::Mdns(event)
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

    let mut join_handles = vec![];
    let shutdown_token = CancellationToken::new();

    // consensus
    let signer = Ed25519Signer::new(local_ed25519_keypair.clone());
    let graph = Graph::new(
        local_peer_id,
        EventPayload::<Vid, Sid>::new(vec![]),
        GenesisPayload {
            pubkey: local_ed25519_keypair.public().into(),
        },
        30,
        signer,
        (),
    );
    let consensus = GraphWrapper::from_graph(graph);
    let (consensus_server, consensus_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(consensus.run(consensus_server)));

    // data memory
    let (memory_bus_data_memory, memory_bus_processor) = MemoryBus::channel(CHANNEL_BUFFER_LIMIT);
    let data_memory =
        DistributedDataMemory::new(local_peer_id, memory_bus_data_memory, ENCODING_SETTINGS);
    let (data_memory_server, data_memory_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(data_memory.run(data_memory_server)));

    // instruction memory
    let instruction_memory =
        InstructionMemory::new(ENCODING_SETTINGS.data_shards_sufficient.try_into().unwrap());
    let (instruction_memory_server, instruction_memory_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(
        instruction_memory.run(instruction_memory_server),
    ));

    // processor
    let processor = ShardProcessor::new(memory_bus_processor);
    let (processor_server, processor_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(processor.run(processor_server)));

    let (behaviour_server, behaviour_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());

    let main_behaviour = behaviour::Behaviour::new(
        local_peer_id,
        Duration::from_secs(5),
        behaviour_server,
        consensus_client,
        instruction_memory_client,
        data_memory_client,
        processor_client,
    );
    let mdns = mdns::async_io::Behaviour::new(Default::default(), local_peer_id)?;

    let behaviour = CombinedBehaviour {
        main: main_behaviour,
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

    // repl is sync, so run it in a separate thread
    let cloned_shutdown = shutdown_token.clone();
    std::thread::spawn(|| ui::run_repl(behaviour_client, cloned_shutdown));

    // todo: send sigterm
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => info!("Listening on {:?}", address),
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(
                        mdns::Event::Discovered(list)
                    )) => {
                        for (peer, _) in list {
                            swarm.behaviour_mut().main.inject_peer_discovered(peer);
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(
                        mdns::Event::Expired(list)
                    )) => {
                        for (peer, _) in list {
                            if !swarm.behaviour_mut().mdns.has_node(&peer) {
                                swarm.behaviour_mut().main.inject_peer_expired(&peer);
                            }
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Main(Err(behaviour::Error::CancelSignal))) => {
                        info!("{}", behaviour::Error::CancelSignal);
                        shutdown_token.cancel();
                        break;
                    },
                    SwarmEvent::Behaviour(event) => info!("{:?}", event),
                    other => debug!("{:?}", other),
                }
            }
        }
    }
    for handle in join_handles {
        handle.await.unwrap()
    }
    Ok(())
}
