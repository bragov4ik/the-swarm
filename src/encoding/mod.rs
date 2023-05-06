use std::collections::HashMap;

pub mod mock;

#[derive(Clone)]
pub struct EncodingSettings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
}

pub trait DataEncoding<TData, TShardId, TShard, TError> {
    fn encode(&self, data: TData) -> Result<HashMap<TShardId, TShard>, TError>;
    fn decode(&self, shards: HashMap<TShardId, TShard>) -> Result<TData, TError>;
    fn settings(&self) -> EncodingSettings;
}
