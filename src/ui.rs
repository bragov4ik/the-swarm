use easy_repl::{command, validator, CommandStatus, Repl};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    behaviour::{self, InEvent},
    module::ModuleChannelClient,
};

async fn print_responses(mut output: mpsc::Receiver<behaviour::OutEvent>) {
    while let Some(next) = output.recv().await {
        println!("{:?}", next)
    }
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
        .add("count", command! {
            "Count from X to Y",
            (X:i32, Y:i32) => |x, y| {
                for i in x..=y {
                    print!(" {}", i);
                }
                println!();
                Ok(CommandStatus::Done)
            }
        })
        .add("say", command! {
            "Say X",
            (:f32) => |x| {
                println!("x is equal to {}", x);
                Ok(CommandStatus::Done)
            },
        })
        .add("outx", command! {
            "Use mutably outside var x. This command has a really long description so we need to wrap it somehow, it is interesting how actually the wrapping will be performed.",
            () => || {
                // outside_x += "x";
                // println!("{}", outside_x);
                Ok(CommandStatus::Done)
            },
        })
        // this shows how to create Command manually with the help of the validator! macro
        // one could also implement arguments validation manually
        .add("outy", easy_repl::Command {
            description: "Use mutably outside var y".into(),
            args_info: vec!["appended".into()],
            handler: Box::new(|args| {
                // let validator = validator!(i32);
                // validator(args)?;
                // outside_y += args[0];
                // println!("{}", outside_y);
                Ok(CommandStatus::Done)
            }),
        })
        .add("test", easy_repl::Command {
            description: "Use mutably outside var y".into(),
            args_info: vec!["appended".into()],
            handler: Box::new(|args| {
                // let validator = validator!(i32);
                // validator(args)?;

                if let Err(e) = rt.block_on(
                    input.send(InEvent::ListStored)
                ) {
                    warn!("could not proceed with request: {}", e)
                }
                Ok(CommandStatus::Done)
            }),
        })
        .add("readall", easy_repl::Command {
            description: "List all stored data in the system".into(),
            args_info: vec![],
            handler: Box::new(|_| {
                if let Err(e) = rt.block_on(
                    input.send(InEvent::ListStored)
                ) {
                    warn!("could not proceed with request: {}", e)
                }
                Ok(CommandStatus::Done)
            }),
        })
        .add("init", easy_repl::Command {
            description: "Initialize storage with known peers".into(),
            args_info: vec![],
            handler: Box::new(|_| {
                if let Err(e) = rt.block_on(
                    input.send(InEvent::InitializeStorage)
                ) {
                    warn!("could not proceed with request: {}", e)
                }
                Ok(CommandStatus::Done)
            }),
        })
        .build().expect("Failed to create repl");

    // print responses in a separate thread because repl.run() is blocking
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("couldn't create an async runtime for repl");
        rt.block_on(print_responses(output))
    });

    repl.run().expect("failed to run repl");
    // ScheduleProgram(Instructions),
    // Get(Vid),
    // Put(Vid, Data),
    // ,

    shutdown_token.cancel()
}
