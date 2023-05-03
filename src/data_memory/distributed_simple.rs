use std::collections::HashMap;

use futures::Stream;
use libp2p::PeerId;
use tokio::sync::mpsc;

use crate::types::{Shard, Sid, Vid};

use super::DataMemory;
pub struct DistributedDataMemory {
    data_locations: HashMap<Vid, Vec<(Sid, PeerId)>>,
    local_storage: HashMap<Vid, (Sid, Vid)>,
    to_distribute: HashMap<Vid, HashMap<Sid, Shard>>,
    currently_assembled: HashMap<Vid, HashMap<Sid, Shard>>,
}

pub struct Settings {
    pub data_pieces_total: u64,
    pub data_pieces_sufficient: u64,
}

struct MemoryBus {
    data_requests: mpsc::Receiver<(Vid, mpsc::Sender<Shard>)>,
    data_writes: mpsc::Receiver<(Vid, Vec<Shard>)>,
    settings: Settings,
}

impl DistributedDataMemory {
    // TODO: proper error
    pub fn handle_received_piece(
        &mut self,
        data_id: Vid,
        shard_id: Sid,
        piece: Shard,
    ) -> Result<(), ()> {
        let pieces = self.currently_assembled.get_mut(&data_id).ok_or(())?;
        if pieces.contains_key(&shard_id) {
            return Err(());
        }
        pieces.insert(shard_id, piece);
        // check if # is enough & respond to request.
        todo!()
    }

    pub async fn operate(&mut self) {
        // check & start handling request
        //
    }
}

impl DataMemory for DistributedDataMemory {
    type Error = ();
    type Piece = Shard;
    type DataId = Vid;
    type PieceId = Sid;

    fn get_piece(&self, full_piece_id: &super::FullPieceId<Self>) -> Option<&Self::Piece> {
        todo!()
    }

    fn store_piece(
        &mut self,
        full_piece_id: super::FullPieceId<Self>,
        data: Self::Piece,
    ) -> Result<Option<Self::Piece>, Self::Error> {
        todo!()
    }

    fn remove_piece(
        &mut self,
        full_piece_id: &super::FullPieceId<Self>,
    ) -> Result<Option<Self::Piece>, Self::Error> {
        todo!()
    }

    fn observe_new_location(&mut self, full_piece_id: &super::FullPieceId<Self>, location: PeerId) {
        todo!()
    }

    fn prepare_to_serve_pieces(
        &mut self,
        data: Self::DataId,
        pieces: HashMap<Self::PieceId, Self::Piece>,
    ) {
        todo!()
    }

    fn serve_piece(&self, full_piece_id: &super::FullPieceId<Self>) -> Option<Self::Piece> {
        todo!()
    }
}

// impl Stream for DistributedDataMemory {
//     type Item = ;

//     fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
//         todo!()
//     }
// }
