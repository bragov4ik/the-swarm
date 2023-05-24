use clap::Parser;
use consensus::graph::GenesisPayload;
use futures::StreamExt;
use libp2p::mdns;
use libp2p::swarm::{NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{Multiaddr, PeerId};
use protocol::request_response::SwarmRequestResponse;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use rust_hashgraph::algorithm::datastructure::Graph;
use signatures::Ed25519Signer;
use std::error::Error;
use std::time::Duration;

use crate::consensus::graph::{EventPayload, GraphWrapper};
use crate::data_memory::{DistributedDataMemory, MemoryBus};
use crate::instruction_storage::InstructionMemory;
use crate::module::ModuleChannelServer;
use crate::processor::single_threaded::ShardProcessor;
use crate::types::{Sid, Vid};

// "modules" as in `module::Module`
mod behaviour;
mod consensus;
mod data_memory;
mod instruction_storage;
mod request_response;

mod encoding;
mod io;
mod logging_helpers;
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
    request_response: libp2p::request_response::Behaviour<SwarmRequestResponse>,
    // MDNS performs LAN node discovery, allows not to manually write peer addresses
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug)]
// TODO: add Main event and check if applies
#[allow(clippy::large_enum_variant)]
enum CombinedBehaviourEvent {
    Main(behaviour::ToSwarmEvent),
    RequestResponse(request_response::Event),
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

impl From<request_response::Event> for CombinedBehaviourEvent {
    fn from(value: request_response::Event) -> Self {
        CombinedBehaviourEvent::RequestResponse(value)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "gen-input")]
    {
        crate::io::test_write_input("data.json", "program.json")
            .await
            .unwrap();
        return Ok(());
    }

    // let format = tracing_subscriber::fmt::format();
    #[cfg(not(feature = "console"))]
    tracing_subscriber::fmt::init();
    #[cfg(feature = "console")]
    console_subscriber::init();

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

    let (mut request_response_server, request_response_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());

    let request_response = libp2p::request_response::Behaviour::new(
        SwarmRequestResponse,
        std::iter::once((
            protocol::versions::RequestResponseVersion::V1,
            libp2p_request_response::ProtocolSupport::Full,
        )),
        Default::default(),
    );
    let main_behaviour = behaviour::Behaviour::new(
        local_peer_id,
        Duration::from_millis(2000),
        behaviour_server,
        consensus_client,
        instruction_memory_client,
        data_memory_client,
        processor_client,
        request_response_client,
    );
    let mdns = mdns::async_io::Behaviour::new(Default::default(), local_peer_id)?;

    let behaviour = CombinedBehaviour {
        main: main_behaviour,
        request_response,
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
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Main(Err(behaviour::Error::UnableToOperate))) => {
                        error!("Shutting down...");
                        shutdown_token.cancel();
                        break;
                    },
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::RequestResponse(r)) => {
                        let handle_result = request_response::handle_request_response_event(&mut request_response_server, r).await;
                        if let Err(_) = handle_result {
                            error!("Shutting down...");
                            shutdown_token.cancel();
                            break;
                        }
                    }
                    SwarmEvent::Behaviour(event) => info!("{:?}", event),
                    other => debug!("{:?}", other),
                }
            }
            action = request_response_server.input.recv() => {
                let Some(action) = action else {
                    error!("other half of `request_response_server.input` was closed. no reason to operate without main behaviour.");
                    shutdown_token.cancel();
                    break;
                };
                match action {
                    request_response::InEvent::MakeRequest { request, to } => {
                        let request_id = swarm.behaviour_mut().request_response.send_request(&to, request.clone());
                        let send_result = request_response_server.output.send(request_response::OutEvent::AssignedRequestId { request_id, request }).await;
                        if let Err(_) = send_result {
                            error!("other half of `request_response_server.output` was closed. no reason to operate without main behaviour.");
                            shutdown_token.cancel();
                            break;
                        }
                    },
                    request_response::InEvent::Respond { request_id, channel, response } => {
                        let send_result = swarm.behaviour_mut().request_response.send_response(channel, response);
                        if let Err(_) = &send_result {
                            warn!("Could not send response to {:?}: {:?}", request_id, send_result);
                        }
                    },
                }
            }
        }
    }
    for handle in join_handles {
        handle.await.unwrap()
    }
    Ok(())
}
