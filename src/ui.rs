use easy_repl::{command, validator, CommandStatus, Repl};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    behaviour::{self, InEvent},
    io::{read_input, InputData, InputProgram},
    module::ModuleChannelClient,
    types::{Data, Vid},
};

async fn print_responses(mut output: mpsc::Receiver<behaviour::OutEvent>) {
    while let Some(next) = output.recv().await {
        println!("{:?}", next)
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
        .build()
        .expect("Failed to create repl");

    // print responses in a separate thread because repl.run() is blocking
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("couldn't create an async runtime for repl");
        rt.block_on(print_responses(output))
    });

    repl.run().expect("failed to run repl");
    shutdown_token.cancel()
}
