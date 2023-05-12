use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct EncodedEd25519Pubkey([u8; 32]);

impl Into<libp2p::identity::ed25519::PublicKey> for EncodedEd25519Pubkey {
    fn into(self) -> libp2p::identity::ed25519::PublicKey {
        libp2p::identity::ed25519::PublicKey::decode(&self.0).expect("signature parse failure")
    }
}

impl From<libp2p::identity::ed25519::PublicKey> for EncodedEd25519Pubkey {
    fn from(value: libp2p::identity::ed25519::PublicKey) -> Self {
        Self(value.encode())
    }
}

impl Into<libp2p::identity::PublicKey> for EncodedEd25519Pubkey {
    fn into(self) -> libp2p::identity::PublicKey {
        // TODO: use `From<ed25519::PublicKey> for PublicKey` when released
        // https://github.com/libp2p/rust-libp2p/pull/3866
        #[allow(deprecated)]
        libp2p::identity::PublicKey::Ed25519(self.into())
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
