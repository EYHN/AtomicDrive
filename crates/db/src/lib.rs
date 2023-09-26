#![feature(allocator_api)]
#![feature(btreemap_alloc)]
#![feature(macro_metavar_expr)] // for the macro in tests.rs

pub mod backend;
pub mod prefix;

use std::alloc::Allocator;

use prefix::Prefix;
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

impl<T: DBRead> DBRead for &T {
    type KeyBytes<'a> = T::KeyBytes<'a>
    where
        Self: 'a;
    type ValueBytes<'a> = T::ValueBytes<'a>
    where
        Self: 'a;
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        T::get(self, key)
    }

    fn has(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        T::has(self, key)
    }

    type IterRange<'a> = T::IterRange<'a>
    where
        Self: 'a;
    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        T::get_range(self, from, to)
    }
}

impl<T: DBRead> DBRead for &mut T {
    type KeyBytes<'a> = T::KeyBytes<'a>
    where
        Self: 'a;
    type ValueBytes<'a> = T::ValueBytes<'a>
    where
        Self: 'a;
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        T::get(self, key)
    }

    fn has(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        T::has(self, key)
    }

    type IterRange<'a> = T::IterRange<'a>
    where
        Self: 'a;
    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        T::get_range(self, from, to)
    }
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

impl<T: DBWrite> DBWrite for &mut T {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        T::set(self, key, value)
    }

    fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        T::delete(self, key)
    }
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

pub trait DBLock {
    type ValueBytes<'a>: AsRef<[u8]>
    where
        Self: 'a;

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>>;
}

impl<T: DBLock> DBLock for &T {
    type ValueBytes<'a> = T::ValueBytes<'a>
    where
        Self: 'a;

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        T::get_for_update(self, key)
    }
}

impl<T: DBLock> DBLock for &mut T {
    type ValueBytes<'a> = T::ValueBytes<'a>
    where
        Self: 'a;

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        T::get_for_update(self, key)
    }
}

pub trait DBLockDyn {
    fn get_for_update(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
}

impl<T: DBLock> DBLockDyn for T {
    fn get_for_update(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        DBLock::get_for_update(self, key).map(|v| v.map(|v| v.as_ref().to_vec()))
    }
}

pub trait DBTransaction: DBWrite + DBRead + DBLock {
    fn rollback(self) -> Result<()>;

    fn commit(self) -> Result<()>;

    fn prefix(self, prefix: impl AsRef<[u8]>) -> Prefix<Self>
    where
        Self: std::marker::Sized,
    {
        Prefix::new(self, prefix)
    }

    fn prefix_in<A: Allocator + Clone>(
        self,
        prefix: impl AsRef<[u8]>,
        alloc: A,
    ) -> Prefix<Self, A>
    where
        Self: std::marker::Sized,
    {
        Prefix::new_in(self, prefix, alloc)
    }
}

pub trait DBTransactionDyn: DBWriteDyn + DBReadDyn + DBLockDyn {
    fn rollback(self) -> Result<()>;

    fn commit(self) -> Result<()>;
}

impl<T: DBTransaction> DBTransactionDyn for T {
    fn rollback(self) -> Result<()> {
        self.rollback()
    }

    fn commit(self) -> Result<()> {
        self.commit()
    }
}

pub trait DB: DBRead {
    type Transaction<'a>: DBTransaction
    where
        Self: 'a;

    fn start_transaction(&self) -> Result<Self::Transaction<'_>>;

    /// for debug purpose
    fn clear(&mut self) -> Result<()>;

    fn prefix(self, prefix: impl AsRef<[u8]>) -> Prefix<Self>
    where
        Self: std::marker::Sized,
    {
        Prefix::new(self, prefix)
    }

    fn prefix_in<A: Allocator + Clone>(self, prefix: impl AsRef<[u8]>, alloc: A) -> Prefix<Self, A>
    where
        Self: std::marker::Sized,
    {
        Prefix::new_in(self, prefix, alloc)
    }
}

impl<T: DB> DB for &T {
    type Transaction<'a> = T::Transaction<'a>
    where
        Self: 'a;

    fn start_transaction(&self) -> Result<Self::Transaction<'_>> {
        T::start_transaction(self)
    }

    fn clear(&mut self) -> Result<()> {
        unreachable!()
    }
}

pub trait DBDyn: DBReadDyn {
    fn start_transaction(&self) -> Result<Box<dyn DBTransactionDyn + '_>>;

    fn clear(&mut self) -> Result<()>;
}

impl<T: DB> DBDyn for T {
    fn start_transaction(&self) -> Result<Box<dyn DBTransactionDyn + '_>> {
        Ok(Box::new(T::start_transaction(self)?))
    }

    fn clear(&mut self) -> Result<()> {
        T::clear(self)
    }
}

#[cfg(test)]
mod tests;
