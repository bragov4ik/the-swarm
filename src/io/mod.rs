//! Reading & parsing initial demo input.

use rand::{thread_rng, Rng};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{iter::repeat, path::Path};

use crate::{
    processor::{Instruction, Instructions},
    types::{Data, Vid},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct InputData {
    pub data: Vec<(Vid, Data)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InputProgram {
    pub instructions: Instructions,
}

pub async fn read_input<P, T>(path: P) -> anyhow::Result<T>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let raw = tokio::fs::read_to_string(&path).await?;
    let data = serde_json::from_str::<T>(&raw)?;
    Ok(data)
}

#[allow(dead_code)]
pub async fn write_input<P, T>(path: P, data: T) -> anyhow::Result<()>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let raw = serde_json::to_string(&data)?;
    tokio::fs::write(path, raw).await?;
    Ok(())
}

#[allow(dead_code)]
/// Write some basic layout to path to see the format
/// for generating other inputs
pub async fn test_write_input<P>(path_data: P, path_program: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let mut first =
        [0u8; (crate::types::SHARD_BYTES_NUMBER * crate::types::DATA_SHARDS_COUNT) as usize];
    for byte in &mut first {
        *byte = thread_rng().gen_range(u8::MIN..=u8::MAX);
    }
    let mut second =
        [0u8; (crate::types::SHARD_BYTES_NUMBER * crate::types::DATA_SHARDS_COUNT) as usize];
    for byte in &mut second {
        *byte = thread_rng().gen_range(u8::MIN..=u8::MAX);
    }
    let test_data = InputData {
        data: vec![(Vid(1), Data(first)), (Vid(2), Data(second))],
    };
    write_input(path_data, test_data).await?;

    // very simple program
    #[allow(unused)]
    let test_program = InputProgram {
        instructions: vec![
            Instruction::plus(Vid(1), Vid(2), Vid(3)),
            Instruction::sub(Vid(1), Vid(2), Vid(4)),
            Instruction::inv(Vid(4), Vid(5)),
        ],
    };

    // large program for testing consistency
    let instructions = repeat([
        Instruction::plus(Vid(1), Vid(2), Vid(1)),
        Instruction::plus(Vid(1), Vid(2), Vid(2)),
    ])
    .take(10000)
    .flatten()
    .collect();
    #[allow(unused)]
    let test_program = InputProgram { instructions };

    // large program for testing performance
    let instructions = repeat(Instruction::plus(Vid(1), Vid(2), Vid(1)))
        .take(25000)
        .collect();
    let test_program = InputProgram { instructions };

    write_input(path_program, test_program).await?;
    Ok(())
}
