use libp2p::mdns;
use libp2p::{
    identity,
    swarm::{NetworkBehaviour, Swarm, SwarmBuilder},
    PeerId,
};
use rust_hashgraph::algorithm::datastructure::Graph;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::error::Error;
use std::time::Duration;

use crate::consensus::graph::{EventPayload, GenesisPayload, GraphWrapper};
use crate::data_memory::{DistributedDataMemory, MemoryBus};
use crate::encoding::reed_solomon;
use crate::instruction_storage::InstructionMemory;
use crate::module::ModuleChannelServer;
use crate::processor::single_threaded::ShardProcessor;
use crate::protocol::request_response::SwarmRequestResponse;
use crate::protocol::versions::RequestResponseVersion;
use crate::signatures::Ed25519Signer;
use crate::types::{Sid, Vid};
use crate::{behaviour, ui, CHANNEL_BUFFER_LIMIT};

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CombinedBehaviourEvent")]
pub struct CombinedBehaviour {
    // Main logic
    pub main: behaviour::Behaviour,
    pub request_response: libp2p::request_response::Behaviour<SwarmRequestResponse>,
    // MDNS performs LAN node discovery, allows not to manually write peer addresses
    pub mdns: mdns::async_io::Behaviour,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CombinedBehaviourEvent {
    Main(behaviour::ToSwarmEvent),
    RequestResponse(crate::request_response::Event),
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

impl From<crate::request_response::Event> for CombinedBehaviourEvent {
    fn from(value: crate::request_response::Event) -> Self {
        CombinedBehaviourEvent::RequestResponse(value)
    }
}

pub async fn new(
    key_seed: Option<u8>,
    encoding_settings: reed_solomon::Settings,
) -> Result<
    (
        Swarm<CombinedBehaviour>,
        ModuleChannelServer<crate::request_response::Module>,
        Vec<tokio::task::JoinHandle<()>>,
        CancellationToken,
    ),
    Box<dyn Error>,
> {
    // Create a public/private key pair, either random or based on a seed.
    let local_keypair = match key_seed {
        Some(s) => {
            let mut bytes = [0u8; 32];
            bytes[0] = s;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        }
        None => identity::Keypair::generate_ed25519(),
    };
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
    let (consensus_server, consensus_client) = ModuleChannelServer::new(
        Some(crate::consensus::graph::ModuleState::Ready),
        CHANNEL_BUFFER_LIMIT,
        shutdown_token.clone(),
    );
    join_handles.push(tokio::spawn(consensus.run(consensus_server)));

    // data memory
    let (memory_bus_data_memory, memory_bus_processor) = MemoryBus::channel(CHANNEL_BUFFER_LIMIT);
    let data_memory = DistributedDataMemory::new(
        local_peer_id,
        memory_bus_data_memory,
        encoding_settings.clone(),
    );
    let (data_memory_server, data_memory_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(data_memory.run(data_memory_server)));

    // instruction memory
    let instruction_memory =
        InstructionMemory::new(encoding_settings.data_shards_sufficient.try_into().unwrap());
    let (instruction_memory_server, instruction_memory_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());
    join_handles.push(tokio::spawn(
        instruction_memory.run(instruction_memory_server),
    ));

    // processor
    let processor = ShardProcessor::new(memory_bus_processor);
    let (processor_server, processor_client) = ModuleChannelServer::new(
        Some(crate::processor::single_threaded::ModuleState::Ready),
        CHANNEL_BUFFER_LIMIT,
        shutdown_token.clone(),
    );
    join_handles.push(tokio::spawn(processor.run(processor_server)));

    let (behaviour_server, behaviour_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());

    let (request_response_server, request_response_client) =
        ModuleChannelServer::new(None, CHANNEL_BUFFER_LIMIT, shutdown_token.clone());

    let mut request_response_cfg: libp2p::request_response::Config = Default::default();
    request_response_cfg.set_connection_keep_alive(Duration::from_secs(60));
    let request_response = libp2p::request_response::Behaviour::new(
        SwarmRequestResponse,
        std::iter::once((
            RequestResponseVersion::V1,
            libp2p_request_response::ProtocolSupport::Full,
        )),
        request_response_cfg,
    );
    let main_behaviour = behaviour::Behaviour::new(
        local_peer_id,
        Duration::from_secs(2),
        Duration::from_secs(12),
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

    // repl is sync, so run it in a separate thread
    let cloned_shutdown = shutdown_token.clone();
    std::thread::spawn(|| ui::run_repl(behaviour_client, cloned_shutdown));

    Ok((swarm, request_response_server, join_handles, shutdown_token))
}
