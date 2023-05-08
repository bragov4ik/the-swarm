use std::iter::repeat;

use reed_solomon_erasure::galois_8::ReedSolomon;
use thiserror::Error;

use crate::types::{Data, Shard, Sid, DATA_SHARDS_COUNT, SHARD_BYTES_NUMBER};

use super::DataEncoding;

pub struct ReedSolomonWrapper {
    inner: ReedSolomon,
}

impl ReedSolomonWrapper {
    pub fn new(encoding_settings: Settings) -> Self {
        let data_shards = encoding_settings.data_shards_sufficient;
        let total_shards = encoding_settings.data_shards_total;
        let inner = ReedSolomon::new(
            data_shards.try_into().unwrap(),
            (total_shards - data_shards).try_into().unwrap(),
        )
        .unwrap();
        Self { inner }
    }
}

// for returning only, the actual settings are stored in `ReedSolomon`
#[derive(Clone)]
pub struct Settings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ReedSolomon(#[from] reed_solomon_erasure::Error),
    #[error(
        "Provided shard identifier is invalid for this encoding; \
    expected index of shard in vector of all shards"
    )]
    WrongShardId,
}

impl DataEncoding<Data, Sid, Shard, Settings, Error> for ReedSolomonWrapper {
    fn encode(&self, data: Data) -> Result<std::collections::HashMap<Sid, Shard>, Error> {
        let shard_byte_number = SHARD_BYTES_NUMBER
            .try_into()
            .expect("unsupported encoding settings by the system");
        let mut shards: Vec<Shard> = data
            .chunks(shard_byte_number)
            .map(|slice| {
                slice
                    .try_into()
                    .expect("number of bytes in `Data` must be divisible by SHARD_BYTES_NUMBER")
            })
            .collect();
        assert_eq!(
            u64::try_from(shards.len()).unwrap(),
            DATA_SHARDS_COUNT,
            "Data must hace SHARD_BYTES_NUMBER*DATA_SHARDS_COUNT bytes"
        );
        let parity_shard: Shard = [0; SHARD_BYTES_NUMBER as usize];
        let parity_shards = repeat(parity_shard).take(self.inner.parity_shard_count());
        shards.extend(parity_shards);
        self.inner.encode(&mut shards)?;
        let shards = shards
            .into_iter()
            .enumerate()
            .map(|(i, shard)| (Sid(i.try_into().unwrap()), shard))
            .collect();
        Ok(shards)
    }

    fn decode(&self, shards: std::collections::HashMap<Sid, Shard>) -> Result<Data, Error> {
        let mut shards_vec: Vec<Option<Vec<u8>>> =
            repeat(None).take(self.inner.total_shard_count()).collect();
        for (index, shard) in shards {
            let vec_index: usize = index.0.try_into().unwrap();
            let shard_position = shards_vec.get_mut(vec_index).ok_or(Error::WrongShardId)?;
            *shard_position = Some(shard.into());
        }
        self.inner.reconstruct(&mut shards_vec)?;
        let data: Vec<_> = shards_vec
            .into_iter()
            .map(|option| option.unwrap().into_iter())
            .take(DATA_SHARDS_COUNT.try_into().unwrap())
            .flatten()
            .collect();
        Ok(data.try_into().unwrap())
    }

    fn settings(&self) -> Settings {
        Settings {
            data_shards_total: self.inner.total_shard_count().try_into().unwrap(),
            data_shards_sufficient: self.inner.data_shard_count().try_into().unwrap(),
        }
    }
}
