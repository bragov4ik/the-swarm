use std::path::Path;
use serde::{Serialize, Deserialize};

use crate::{processor::Instruction, types::{Shard, Vid}};

#[derive(Serialize, Deserialize, Debug)]
pub struct InputData {
    data_layout: Vec<DataInit>,
    instructions: Vec<Instruction<Vid, Vid>>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataInit {
    id: Vid,
    data: Shard,
}

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Serde(serde_json::Error),
}

pub fn read_input<P>(path: P) -> Result<InputData, Error>
where
    P: AsRef<Path>
{
    let raw = std::fs::read_to_string(&path)
        .map_err(Error::IO)?;
    let data = serde_json::from_str::<InputData>(&raw)
        .map_err(Error::Serde)?;
    Ok(data)
}

pub fn test_write_input<P>(path: P) -> Result<(), Error>
where
    P: AsRef<Path>
{
    let test_data = InputData {
        data_layout: vec![
            DataInit{ id: Vid(1), data: -123 },
            DataInit{ id: Vid(2), data: 1337 },
        ],
        instructions: vec![
            Instruction::Or(Vid(1), Vid(2), Vid(3)),
            Instruction::And(Vid(1), Vid(2), Vid(4)),
            Instruction::Not(Vid(4), Vid(5)),
        ],
    };
    let raw = serde_json::to_string_pretty(&test_data)
        .map_err(Error::Serde)?;
    std::fs::write(path, raw)
        .map_err(Error::IO)?;
    Ok(())
}