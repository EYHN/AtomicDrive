use std::fmt;

#[derive(Clone, Debug)]
pub enum Error {
    RocksdbError(rocksdb::Error),
    EncodeError(String),
    DecodeError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)?;
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self {
            Error::RocksdbError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Self::RocksdbError(err)
    }
}
