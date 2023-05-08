use std::collections::{HashMap, HashSet};

use libp2p::PeerId;
use thiserror::Error;

use crate::types::{Data, Shard, Sid};

use super::{DataEncoding, MockEncodingSettings};

pub struct MockEncoding {
    settings: MockEncodingSettings,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Did not recieve enough shards to rebuild data")]
    NotEnoughShards,
}

impl DataEncoding<Data, Sid, Shard, MockEncodingSettings, Error> for MockEncoding {
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

    fn settings(&self) -> MockEncodingSettings {
        self.settings.clone()
    }
}

// impl MockEncoding {
//     fn generate_distribution(&self, other_peers: HashSet<PeerId>, shard_ids: Vec<Sid>) -> HashMap<Sid, PeerId> {
//         // too complex to do properly
//         // let's do reed solomon right away. won't have to worry about saving local after execution done (or do we?)
//         // think about additional ops where we need to actually recollect
//     }
// }

#[cfg(test)]
mod tests {
    use libp2p::PeerId;

    use crate::{
        encoding::{DataEncoding, MockEncodingSettings},
        types::Sid,
    };

    use super::MockEncoding;

    #[test]
    fn it_works() {
        let encoding = MockEncoding {
            settings: MockEncodingSettings {
                data_shards_total: 3,
                data_shards_sufficient: 3,
                locally_assigned_id: Sid(0),
                self_id: PeerId::random(), // doesn't affect the test
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
