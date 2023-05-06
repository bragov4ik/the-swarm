use std::collections::HashMap;

use thiserror::Error;

use crate::types::{Data, Shard, Sid};

use super::{DataEncoding, EncodingSettings};

pub struct MockEncoding {
    settings: EncodingSettings,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Did not recieve enough shards to rebuild data")]
    NotEnoughShards,
}

impl DataEncoding<Data, Sid, Shard, Error> for MockEncoding {
    fn encode(&self, data: Data) -> Result<HashMap<Sid, Shard>, Error> {
        let shards: HashMap<Sid, Shard> = data
            .chunks_exact(4)
            .enumerate()
            .map(|(i, slice)| {
                (
                    Sid(i.try_into().unwrap()),
                    slice
                        .try_into()
                        .expect("chunk_exact(4) must return slices of len 4"),
                )
            })
            .collect();
        assert!(shards.len() as u64 == self.settings.data_shards_total);
        Ok(shards)
    }

    fn decode(&self, shards: HashMap<Sid, Shard>) -> Result<Data, Error> {
        let mut shards: Vec<_> = shards.into_iter().collect();
        shards.sort_by_key(|(i, _)| i.0);
        let kek: Vec<_> = shards
            .into_iter()
            .map(|(_, shard)| shard.into_iter())
            .flatten()
            .collect();
        kek.try_into().map_err(|_| Error::NotEnoughShards)
    }

    fn settings(&self) -> EncodingSettings {
        self.settings.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        encoding::{DataEncoding, EncodingSettings},
        types::Sid,
    };

    use super::MockEncoding;

    #[test]
    fn it_works() {
        let encoding = MockEncoding {
            settings: EncodingSettings {
                data_shards_total: 3,
                data_shards_sufficient: 3,
            },
        };
        let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let encoded = encoding.encode(data).unwrap();
        assert_eq!(encoded.len(), 3);
        assert_eq!(encoded.get(&Sid(0)).unwrap(), &[1, 2, 3, 4]);
        assert_eq!(encoded.get(&Sid(1)).unwrap(), &[5, 6, 7, 8]);
        assert_eq!(encoded.get(&Sid(2)).unwrap(), &[9, 10, 11, 12]);
        let decoded = encoding.decode(encoded).unwrap();
        assert_eq!(data, decoded)
    }
}
