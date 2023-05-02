pub mod mock;

pub trait DataEncoding<TData, TShard, TError> {
    fn encode(data: TData) -> Result<Vec<TShard>, TError>;

    fn decode(shards: Vec<TShard>) -> Result<TData, TError>;
}
