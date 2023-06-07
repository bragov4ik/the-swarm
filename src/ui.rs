use std::collections::HashMap;

use easy_repl::{validator, CommandStatus, Repl};
use libp2p::PeerId;
use textplots::{Chart, Plot, Shape};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    behaviour::{self, metrics::Metrics, InEvent},
    io::{read_input, InputData, InputProgram},
    module::ModuleChannelClient,
    processor::{mock::MockProcessor, Program},
    types::{Data, Hash, Sid, Vid},
};

fn print_all_stored(list: Vec<(Vid, HashMap<Sid, PeerId>)>) {
    println!("\nAll data known to be stored in the network:");
    for (id, shards) in list {
        println!("{:?}", id);
        for (shard_id, peer_id) in shards {
            println!("\t{:?} - {:?}", shard_id, peer_id);
        }
        println!();
    }
}

fn print_metrics_field(name: String, data_points: Vec<(f32, f32)>) {
    println!("{}", name);
    let Some((t_last, _)) = data_points.last() else {
        return
    };
    Chart::new(120, 60, (t_last - 60f32).max(0f32), *t_last)
        .lineplot(&Shape::Steps(&data_points))
        .display();
    println!();
}

fn print_metrics(metrics: Metrics) {
    println!("\nMetrics:");
    print_metrics_field(
        "Sync generation".to_string(),
        metrics.sync.generate_data_for_step(),
    );
    let data = metrics
        .consensus_queue_size
        .generate_data_for_step()
        .into_iter()
        .map(|(t, size)| (t, size as f32))
        .collect();
    print_metrics_field("Consensus input queue".to_string(), data);
}

async fn handle_responses(mut output: Receiver<behaviour::OutEvent>) {
    while let Some(next) = output.recv().await {
        match next {
            behaviour::OutEvent::ScheduleOk => println!("Program scheduled successfully"),
            behaviour::OutEvent::ProgramExecuted(id) => {
                println!("Program {:?} finished execution", id)
            }
            behaviour::OutEvent::GetResponse(Ok(data)) => {
                println!("Retrieved data with id {:?}: {:?}", data.0, data.1)
            }
            behaviour::OutEvent::GetResponse(Err(e)) => println!("Could not recollect data: {}", e),
            behaviour::OutEvent::PutConfirmed(id) => println!(
                "Data with id {:?} was successfully distributed across the network",
                id
            ),
            behaviour::OutEvent::ListStoredResponse(list) => print_all_stored(list),
            behaviour::OutEvent::StorageInitialized => println!("Storage initialized"),
            behaviour::OutEvent::GetMetricsResponse(metrics) => print_metrics(metrics),
        }
    }
}

async fn handle_put(filename: &str, input: &Sender<InEvent>) -> anyhow::Result<()> {
    let data_list = read_input::<_, InputData>(filename).await?;
    for (data_id, data) in data_list.data {
        input.send(InEvent::Put(data_id, data)).await?;
    }
    Ok(())
}

async fn handle_get(data_id: Vid, input: &Sender<InEvent>) -> anyhow::Result<()> {
    input.send(InEvent::Get(data_id)).await?;
    Ok(())
}

async fn handle_schedule_exec(filename: &str, input: &Sender<InEvent>) -> anyhow::Result<()> {
    let program = read_input::<_, InputProgram>(filename).await?;
    input
        .send(InEvent::ScheduleProgram(program.instructions))
        .await?;
    Ok(())
}

async fn handle_expected_output(data_filename: &str, program_filename: &str) -> anyhow::Result<()> {
    println!("Reading program...");
    let program = read_input::<_, InputProgram>(program_filename).await?;
    println!("Reading data...");
    let data = read_input::<_, InputData>(data_filename).await?;
    let program = Program::new(program.instructions, Hash::from_array([0; 64]))?;
    let mut data_storage: HashMap<Vid, Data> = data.data.into_iter().collect();
    println!("Starting mock program execution...");
    MockProcessor::execute_on(program, &mut data_storage)?;
    println!(
        "Finished mock execution, storage state after:\n{:?}",
        data_storage
    );
    Ok(())
}

async fn handle_metrics_print(input: &Sender<InEvent>) -> anyhow::Result<()> {
    input.send(InEvent::GetMetrics).await?;
    Ok(())
}

pub fn run_repl(
    behaviour_channel: ModuleChannelClient<behaviour::Module>,
    shutdown_token: CancellationToken,
) {
    let ModuleChannelClient { input, output, .. } = behaviour_channel;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("couldn't create an async runtime for repl");

    let mut repl = Repl::builder()
        .description("Example REPL")
        .prompt("=> ")
        .text_width(60 as usize)
        .with_filename_completion(true)
        .add(
            "init",
            easy_repl::Command {
                description: "Initialize storage with known peers".into(),
                args_info: vec![],
                handler: Box::new(|_| {
                    if let Err(e) = rt.block_on(input.send(InEvent::InitializeStorage)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    info!("Initializing storage... This may take some time");
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "readall",
            easy_repl::Command {
                description: "List all stored data in the system".into(),
                args_info: vec![],
                handler: Box::new(|_| {
                    if let Err(e) = rt.block_on(input.send(InEvent::ListStored)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "put",
            easy_repl::Command {
                description: "Load data from the file into the system".into(),
                args_info: vec!["filename".into()],
                handler: Box::new(|args| {
                    let validator = validator!(String);
                    validator(args)?;
                    let filename = args[0];
                    if let Err(e) = rt.block_on(handle_put(filename, &input)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "get",
            easy_repl::Command {
                description: "Get data stored in the distributed system".into(),
                args_info: vec!["data id".into()],
                handler: Box::new(|args| {
                    let validator = validator!(u64);
                    validator(args)?;
                    let data_id = Vid(args[0].parse::<u64>()?);
                    if let Err(e) = rt.block_on(handle_get(data_id, &input)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "schedule",
            easy_repl::Command {
                description: "Schedule the program from the file".into(),
                args_info: vec!["filename".into()],
                handler: Box::new(|args| {
                    let validator = validator!(String);
                    validator(args)?;
                    let filename = args[0];
                    if let Err(e) = rt.block_on(handle_schedule_exec(filename, &input)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "metrics_print",
            easy_repl::Command {
                description: "Print metrics to the terminal".into(),
                args_info: vec![],
                handler: Box::new(|_| {
                    if let Err(e) = rt.block_on(handle_metrics_print(&input)) {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .add(
            "mock_calc",
            easy_repl::Command {
                description: "Execute the program on given data \
                    completely locally and return the result"
                    .into(),
                args_info: vec!["datafile".into(), "programfile".into()],
                handler: Box::new(|args| {
                    let validator = validator!(String, String);
                    validator(args)?;
                    let data_filename = args[0];
                    let program_filename = args[1];
                    if let Err(e) =
                        rt.block_on(handle_expected_output(data_filename, program_filename))
                    {
                        warn!("could not proceed with request: {}", e)
                    }
                    Ok(CommandStatus::Done)
                }),
            },
        )
        .build()
        .expect("Failed to create repl");

    // print responses in a separate thread because repl.run() is blocking
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("couldn't create an async runtime for repl");
        rt.block_on(handle_responses(output))
    });

    repl.run().expect("failed to run repl");
    shutdown_token.cancel()
}
