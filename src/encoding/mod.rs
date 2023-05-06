pub mod mock;

pub struct EncodingSettings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
}

pub trait DataEncoding<TData, TShard, TError> {
    fn encode(data: TData) -> Result<Vec<TShard>, TError>;
    fn decode(shards: Vec<TShard>) -> Result<TData, TError>;
    fn settings(&self) -> EncodingSettings;
}
