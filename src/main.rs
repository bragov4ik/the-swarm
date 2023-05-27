use clap::Parser;
use console_subscriber::ConsoleLayer;
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;
use libp2p::{mdns, PeerId};
use tracing::{debug, error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

use std::error::Error;
use std::net::SocketAddr;

use crate::network::CombinedBehaviourEvent;

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
mod network;
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

    #[cfg(feature = "console-log")]
    #[clap(short, long)]
    console_subscriber_addr: Option<String>,

    /// Seed to generate key
    #[clap(long)]
    key_seed: Option<u8>,
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

    let args = Args::parse();

    let (mut swarm, mut request_response_server, join_handles, shutdown_token) =
        network::new(None).await.unwrap();

    let _guard = configure_logs(*swarm.local_peer_id(), args.console_subscriber_addr);

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = args.dial_address {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        info!("Dialed {}", addr)
    }

    // todo: send sigterm
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer, address) in list {
                            swarm.behaviour_mut().main.inject_peer_discovered(peer);
                            swarm.behaviour_mut().request_response.add_address(&peer, address);
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer, address) in list {
                            if !swarm.behaviour_mut().mdns.has_node(&peer) {
                                swarm.behaviour_mut().main.inject_peer_expired(&peer);
                                swarm.behaviour_mut().request_response.remove_address(&peer, &address);
                            }
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::RequestResponse(e)) => {
                        let handle_result = request_response::handle_request_response_event(
                            &mut request_response_server, e
                        ).await;
                        if let Err(_) = handle_result {
                            error!("Shutting down...");
                            shutdown_token.cancel();
                            break;
                        }
                    },
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
                    SwarmEvent::Behaviour(event) => info!("{:?}", event),
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let local_peer_id = *swarm.local_peer_id();
                        eprintln!(
                            "Local node is listening on {:?}",
                            address.with(libp2p::multiaddr::Protocol::P2p(local_peer_id.into()))
                        );
                    }
                    SwarmEvent::IncomingConnection { .. } => {}
                    SwarmEvent::ConnectionEstablished { .. } => {}
                    SwarmEvent::ConnectionClosed { .. } => {}
                    SwarmEvent::OutgoingConnectionError { .. } => {}
                    SwarmEvent::IncomingConnectionError { .. } => {}
                    SwarmEvent::Dialing(peer_id) => eprintln!("Dialing {peer_id}"),
                    other => debug!("{:?}", other),
                }
            }
            action = request_response_server.input.recv() => {
                let Some(action) = action else {
                    error!("other half of `request_response_server.input` was closed. no reason to operate without main behaviour.");
                    shutdown_token.cancel();
                    break;
                };
                info!("{:?}", action);
                match action {
                    request_response::InEvent::MakeRequest { request, to } => {
                        // todo: check if `to` is local id, reroute manually if needed
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

/// Returned guard should be dropped at the end of program execution
/// (see docs for details)
fn configure_logs(
    local_id: PeerId,
    console_subscriber_addr: Option<String>,
) -> Option<WorkerGuard> {
    #[allow(unused_assignments)]
    let mut guard = None;
    #[cfg(feature = "file-log")]
    let file_layer = {
        let filename =
            format!("./logs/{:?}-{}.log", chrono::offset::Utc::now(), local_id).to_string();
        let path = std::path::Path::new(&filename);
        let prefix = path.parent().unwrap();
        std::fs::create_dir_all(prefix).unwrap();
        let (non_blocking, _guard) =
            tracing_appender::non_blocking(std::fs::File::create(path).unwrap());
        guard = Some(_guard);

        let file_layer = tracing_subscriber::fmt::Layer::new()
            .with_ansi(false)
            .with_writer(non_blocking);

        file_layer
    };
    #[cfg(feature = "console-log")]
    let console_layer = {
        let mut layer = ConsoleLayer::builder().with_default_env();
        match console_subscriber_addr {
            Some(addr) => {
                let addr: SocketAddr = addr.parse().unwrap();
                layer = layer.server_addr(addr);
            }
            None => (),
        }
        layer.spawn()
    };

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    let registry = tracing_subscriber::registry();

    #[cfg(feature = "file-log")]
    let registry = registry.with(file_layer);
    #[cfg(feature = "console-log")]
    let registry = registry.with(console_layer);
    registry.with(stdout_layer).init();
    guard
}
