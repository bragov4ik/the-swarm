pub trait DataStorage {
    type Error;
    type Identifier;
    type Data;

    fn get(id: Self::Identifier) -> Option<Self::Data>;
    fn put(id: Self::Identifier, data: Self::Data) -> Result<(), Self::Error>;
    fn remove(id: Self::Identifier) -> Result<(), Self::Error>;
}
