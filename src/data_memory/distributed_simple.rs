use std::collections::HashMap;

use libp2p::PeerId;
use tokio::sync::mpsc;

use crate::types::{Data, Shard, Sid, Vid};

use super::DataMemory;

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = ();
}

pub enum OutEvent {
    PreparedForService {
        data_id: Vid,
        distribution: Vec<(PeerId, Sid)>,
    },
    ServedPiece(super::FullPieceId<DistributedDataMemory>, Option<Shard>),
    // assigned
    AssignedStoreSuccess(super::FullPieceId<DistributedDataMemory>),
    AssignedEvent {
        full_piece_id: super::FullPieceId<DistributedDataMemory>,
        shard: Option<Shard>,
    },

    // data recollection
    RequestAssigned {
        full_piece_id: super::FullPieceId<DistributedDataMemory>,
        location: PeerId,
    },
    FinishedRecollection {
        data_id: Vid,
        data: Data,
    },
}

pub enum InEvent {
    // will store the location & set piece as successfully served if applicable
    TrackLocation {
        full_piece_id: super::FullPieceId<DistributedDataMemory>,
        location: PeerId,
    },

    // initial distribution
    PrepareForService {
        data_id: Vid,
        data: Data,
    },
    ServePiece(super::FullPieceId<DistributedDataMemory>),

    // assigned
    StoreAssigned {
        full_piece_id: super::FullPieceId<DistributedDataMemory>,
        shard: Shard,
    },
    GetAssigned(super::FullPieceId<DistributedDataMemory>),

    // data recollection
    RecollectData(Vid),
    HandleRequested {
        full_piece_id: super::FullPieceId<DistributedDataMemory>,
        piece: Shard,
    },
}

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