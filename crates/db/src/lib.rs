#![feature(macro_metavar_expr)]

pub mod backend;

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

pub trait DBReadDyn {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn has(&self, key: &[u8]) -> Result<bool>;

    fn get_range(&self, from: &[u8], to: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

impl<T: DBRead> DBReadDyn for T {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        DBRead::get(self, key).map(|v| v.map(|v| v.as_ref().to_vec()))
    }

    fn has(&self, key: &[u8]) -> Result<bool> {
        DBRead::has(self, key)
    }

    fn get_range(&self, from: &[u8], to: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut vec = vec![];
        for item in DBRead::get_range(self, from, to) {
            let (key, value) = item?;
            vec.push((key.as_ref().to_vec(), value.as_ref().to_vec()))
        }
        Ok(vec)
    }
}

pub trait DBWrite {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()>;

    fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()>;
}

pub trait DBWriteDyn {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;
}

impl<T: DBWrite> DBWriteDyn for T {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        DBWrite::set(self, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        DBWrite::delete(self, key)
    }
}

pub trait DBTransaction: DBWrite + DBRead {
    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>>;

    fn rollback(self) -> Result<()>;

    fn commit(self) -> Result<()>;
}

pub trait DBTransactionDyn: DBWriteDyn + DBReadDyn {
    fn get_for_update(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn rollback(self) -> Result<()>;

    fn commit(self) -> Result<()>;
}

impl<T: DBTransaction> DBTransactionDyn for T {
    fn get_for_update(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        DBTransaction::get_for_update(self, key).map(|v| v.map(|v| v.as_ref().to_vec()))
    }

    fn rollback(self) -> Result<()> {
        todo!()
    }

    fn commit(self) -> Result<()> {
        todo!()
    }
}

pub trait DB: DBRead {
    type Transaction<'a>: DBTransaction
    where
        Self: 'a;

    fn start_transaction(&self) -> Result<Self::Transaction<'_>>;
}

pub trait DBDyn: DBReadDyn {
    fn start_transaction(&self) -> Result<Box<dyn DBTransactionDyn + '_>>;
}

#[cfg(test)]
mod tests;
