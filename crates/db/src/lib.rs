#![feature(btree_cursors)]

pub mod backend;

use std::borrow::Cow;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("rocksdb error")]
    RocksdbError(#[from] rocksdb::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait DBRead {
    type KeyBytes<'a>: AsRef<[u8]>
    where
        Self: 'a;

    type ValueBytes<'a>: AsRef<[u8]>
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>>;

    fn has(&self, key: impl AsRef<[u8]>) -> Result<bool>;

    type IterRange<'a>: Iterator<Item = Result<(Self::KeyBytes<'a>, Self::ValueBytes<'a>)>>
    where
        Self: 'a;
    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_>;
}

pub trait DBWrite {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()>;
}

pub trait DBTransaction: DBWrite + DBRead {
    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>>;

    fn rollback(self) -> Result<()>;

    fn commit(self) -> Result<()>;
}

pub trait DB: DBRead {
    type Transaction<'a>: DBTransaction
    where
        Self: 'a;

    fn start_transaction(&self) -> Result<Self::Transaction<'_>>;
}

#[cfg(test)]
mod tests;
