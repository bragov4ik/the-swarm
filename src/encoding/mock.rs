use thiserror::Error;

use crate::types::{Data, Shard};

use super::{DataEncoding, EncodingSettings};

pub struct MockEncoding {
    settings: EncodingSettings,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Did not recieve enough shards to rebuild data")]
    NotEnoughShards,
}

impl DataEncoding<Data, Shard, Error> for MockEncoding {
    fn encode(data: Data) -> Result<Vec<Shard>, Error> {
        let shards: Vec<Shard> = data
            .chunks_exact(4)
            .map(|slice| {
                slice
                    .try_into()
                    .expect("chunk_exact(4) must return slices of len 4")
            })
            .collect();
        assert!(shards.len() == 3);
        Ok(shards)
    }

    fn decode(shards: Vec<Shard>) -> Result<Data, Error> {
        let kek: Vec<_> = shards
            .into_iter()
            .map(|shard| shard.into_iter())
            .flatten()
            .collect();
        kek.try_into().map_err(|_| Error::NotEnoughShards)
    }

    fn settings(&self) -> EncodingSettings {
        self.settings
    }
}
