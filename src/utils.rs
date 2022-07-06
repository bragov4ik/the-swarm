use crate::{protocol::{Message, RequestResponse, RequestResponsePayload}, types::Shard};


pub fn create_shard_response(shard: Option<Shard>) -> Message {
    Message::Pair(
        RequestResponse::Shard(RequestResponsePayload::Response(
            shard,
        )),
    )
}