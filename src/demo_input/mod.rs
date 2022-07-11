//! Reading & parsing initial demo input.

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display, path::Path};

use crate::{
    processor::Instruction,
    types::{Shard, Vid},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct InputData {
    pub data_layout: Vec<(Vid, Shard)>,
    pub instructions: Vec<Instruction<Vid, Vid>>,
}

#[derive(Debug)]
pub enum InputError {
    IO(std::io::Error),
    Serde(serde_json::Error),
}

impl Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputError::IO(e) => write!(f, "IO error with input: {}", e),
            InputError::Serde(e) => write!(f, "Serialization error with input: {}", e),
        }
    }
}

impl Error for InputError {}

pub fn read_input<P>(path: P) -> Result<InputData, InputError>
where
    P: AsRef<Path>,
{
    let raw = std::fs::read_to_string(&path).map_err(InputError::IO)?;
    let data = serde_json::from_str::<InputData>(&raw).map_err(InputError::Serde)?;
    Ok(data)
}

#[allow(dead_code)]
/// Write some basic layout to path to see the format
/// for generating other inputs
pub fn test_write_input<P>(path: P) -> Result<(), InputError>
where
    P: AsRef<Path>,
{
    let test_data = InputData {
        data_layout: vec![(Vid(1), [1, 2, 3, 4]), (Vid(2), [1337, 322, 123, -1])],
        instructions: vec![
            Instruction::Plus(Vid(1), Vid(2), Vid(3)),
            Instruction::Dot(Vid(1), Vid(2), Vid(4)),
            Instruction::Inv(Vid(4), Vid(5)),
        ],
    };
    let raw = serde_json::to_string_pretty(&test_data).map_err(InputError::Serde)?;
    std::fs::write(path, raw).map_err(InputError::IO)?;
    Ok(())
}
