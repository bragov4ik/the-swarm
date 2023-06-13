use crate::consensus::graph::GenesisPayload;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct EncodedEd25519Pubkey([u8; 32]);

impl From<EncodedEd25519Pubkey> for libp2p::identity::ed25519::PublicKey {
    fn from(val: EncodedEd25519Pubkey) -> Self {
        libp2p::identity::ed25519::PublicKey::decode(&val.0).expect("signature parse failure")
    }
}

impl From<libp2p::identity::ed25519::PublicKey> for EncodedEd25519Pubkey {
    fn from(value: libp2p::identity::ed25519::PublicKey) -> Self {
        Self(value.encode())
    }
}

impl From<EncodedEd25519Pubkey> for libp2p::identity::PublicKey {
    fn from(val: EncodedEd25519Pubkey) -> Self {
        // TODO: use `From<ed25519::PublicKey> for PublicKey` when released
        // https://github.com/libp2p/rust-libp2p/pull/3866
        #[allow(deprecated)]
        libp2p::identity::PublicKey::Ed25519(val.into())
    }
}

impl TryFrom<libp2p::identity::PublicKey> for EncodedEd25519Pubkey {
    type Error = ();

    fn try_from(value: libp2p::identity::PublicKey) -> Result<Self, Self::Error> {
        // TODO: use `From<ed25519::PublicKey> for PublicKey` when released
        // https://github.com/libp2p/rust-libp2p/pull/3866
        #[allow(deprecated, irrefutable_let_patterns)]
        let libp2p::identity::PublicKey::Ed25519(key) = value else {
            return Err(());
        };
        Ok(Self::from(key))
    }
}

pub struct Ed25519Signer {
    inner: libp2p::identity::ed25519::Keypair,
}

impl Ed25519Signer {
    pub fn new(keypair: libp2p::identity::ed25519::Keypair) -> Self {
        Self { inner: keypair }
    }
}

impl rust_hashgraph::algorithm::Signer<GenesisPayload> for Ed25519Signer {
    type SignerIdentity = PeerId;

    fn sign(
        &self,
        hash: &rust_hashgraph::algorithm::event::Hash,
    ) -> rust_hashgraph::algorithm::event::Signature {
        let signature_bytes: [u8; 64] = self
            .inner
            .sign(hash.as_ref())
            .try_into()
            .expect("signature failure");
        rust_hashgraph::algorithm::event::Signature(
            rust_hashgraph::algorithm::event::Hash::from_array(signature_bytes),
        )
    }

    fn verify(
        &self,
        hash: &rust_hashgraph::algorithm::event::Hash,
        signature: &rust_hashgraph::algorithm::event::Signature,
        identity: &Self::SignerIdentity,
        genesis_payload: &GenesisPayload,
    ) -> bool {
        let public_key = genesis_payload.pubkey.clone().into();
        identity.is_public_key(&public_key).unwrap_or(false);
        public_key.verify(hash.as_ref(), signature.0.as_ref())
    }
}
