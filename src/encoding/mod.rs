use std::collections::HashMap;

use libp2p::PeerId;

use crate::types::Sid;

pub mod mock;
pub mod reed_solomon;

#[derive(Clone)]
pub struct MockEncodingSettings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
    pub locally_assigned_id: Sid,
    pub self_id: PeerId,
}

pub trait DataEncoding<TData, TShardId, TShard, TSettings, TError> {
    fn encode(&self, data: TData) -> Result<HashMap<TShardId, TShard>, TError>;
    fn decode(&self, shards: HashMap<TShardId, TShard>) -> Result<TData, TError>;
    fn settings(&self) -> TSettings;
}
