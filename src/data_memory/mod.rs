//! Data storage and localization.
//!
//! This module is responsible for saving data assigned to this peer,
//! temporarily storing pieces that are currently in the process
//! of distribution, and tracking location of other pieces in the
//! network.
//!
//! Assigned data is handled via commonly-named functions [`DataMemory::get_piece()`],
//! [`DataMemory::store_piece()`], and [`DataMemory::remove_piece()`].
//!
//! The functions responsible for temporal data are [`DataMemory::prepare_to_serve_pieces()`],
//! [`DataMemory::serve_piece()`], and (partly) [`DataMemory::observe_new_location()`].
//!
//! [`DataMemory::observe_new_location()`] mainly tracks locations of other data pieces
//! but also helps to remove unnecessary stored temporal data.

use std::{collections::HashMap, fmt::Debug};

use libp2p::PeerId;

// pub mod mock;
pub mod distributed_simple;

pub type FullPieceId<D> = (<D as DataMemory>::DataId, <D as DataMemory>::PieceId);

pub trait DataMemory {
    type Error: Debug;

    type Piece;
    type DataId;
    type PieceId;

    /// Get locally stored piece assigned to this peer, if present
    fn get_piece(&self, full_piece_id: &FullPieceId<Self>) -> Option<&Self::Piece>;

    /// Put data assigned to this peer in the local storage, updating
    /// and returning old value, if there was any.
    fn store_piece(
        &mut self,
        full_piece_id: FullPieceId<Self>,
        data: Self::Piece,
    ) -> Result<Option<Self::Piece>, Self::Error>;

    /// Remove piece from the local storage and return it (if there was any)
    fn remove_piece(
        &mut self,
        full_piece_id: &FullPieceId<Self>,
    ) -> Result<Option<Self::Piece>, Self::Error>;

    /// Notify the data memory about observed location of some data piece.
    ///
    /// Data memeory is intended to track the pieces (todo: maybe separate this
    /// responsibility into other module?); it's done via this method.
    ///
    /// Also this allows to track how many pieces were already served and
    /// remove them if no longer needed.
    fn observe_new_location(&mut self, full_piece_id: &FullPieceId<Self>, location: PeerId);

    /// Get ready to serve & track the progress of service of data pieces during
    /// data distribution.
    ///
    /// The idea is to store them for some short time until they are all distributed.
    fn prepare_to_serve_pieces(
        &mut self,
        data: Self::DataId,
        pieces: HashMap<Self::PieceId, Self::Piece>,
    );

    /// Provide piece stored temporarily.
    fn serve_piece(&self, full_piece_id: &FullPieceId<Self>) -> Option<Self::Piece>;
}
