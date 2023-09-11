use rocksdb::OptimisticTransactionDB;

use crate::{DBRead, DBTransaction, DBWrite, Error, Result, DB};

#[derive(Debug)]
pub struct RocksDB {
    db: OptimisticTransactionDB,
}

impl RocksDB {
    pub fn open_or_create_database(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = OptimisticTransactionDB::open(&opts, path)?;
        Ok(Self { db })
    }

    pub fn drop_all(&mut self) -> Result<()> {
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            self.db.delete(item?.0)?;
        }
        Ok(())
    }
}

pub enum RocksDBBytes<'a> {
    Shared(rocksdb::DBPinnableSlice<'a>),
    Owned(Box<[u8]>),
}

impl<'a> From<rocksdb::DBPinnableSlice<'a>> for RocksDBBytes<'a> {
    fn from(v: rocksdb::DBPinnableSlice<'a>) -> Self {
        Self::Shared(v)
    }
}

impl AsRef<[u8]> for RocksDBBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Shared(v) => v.as_ref(),
            Self::Owned(v) => v.as_ref(),
        }
    }
}

impl DBRead for RocksDB {
    type KeyBytes<'a> = Box<[u8]>
    where
        Self: 'a;

    type ValueBytes<'a> = RocksDBBytes<'a>
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.db.get_pinned(key)?.map(|b| b.into()))
    }

    fn has(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.db.get_pinned(key)?.is_some())
    }

    type IterRange<'a> = RocksDBRangeIter<'a, OptimisticTransactionDB>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        let upper_bound = to.as_ref().to_vec();
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(from.as_ref(), rocksdb::Direction::Forward),
            read_opt,
        );

        Self::IterRange {
            iter,
            check_upper_bound: None,
        }
    }
}

impl DB for RocksDB {
    type Transaction<'a> = RocksDBTransaction<'a>
    where
        Self: 'a;

    fn start_transaction(&self) -> Result<Self::Transaction<'_>> {
        Ok(RocksDBTransaction {
            transaction: self.db.transaction(),
        })
    }
}

pub struct RocksDBRangeIter<'a, D: rocksdb::DBAccess> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, D>,
    /// try fix: https://github.com/facebook/rocksdb/issues/2343
    check_upper_bound: Option<Vec<u8>>,
}

impl<'a, D: rocksdb::DBAccess> Iterator for RocksDBRangeIter<'a, D> {
    type Item = Result<(Box<[u8]>, RocksDBBytes<'a>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|i| {
            i.map_err(Error::from)
                .map(|item| {
                    if let Some(ref check_upper_bound) = self.check_upper_bound {
                        if item.0[..] >= check_upper_bound[..] {
                            return None;
                        }
                    }

                    Some((item.0, RocksDBBytes::Owned(item.1)))
                })
                .transpose()
        })
    }
}

pub struct RocksDBTransaction<'db> {
    transaction: rocksdb::Transaction<'db, OptimisticTransactionDB>,
}

impl<'db> DBRead for RocksDBTransaction<'db> {
    type KeyBytes<'a> = Box<[u8]>
    where
        Self: 'a;

    type ValueBytes<'a> = RocksDBBytes<'a>
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.transaction.get_pinned(key)?.map(|b| b.into()))
    }

    fn has(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.transaction.get_pinned(key)?.is_some())
    }

    type IterRange<'a> = RocksDBRangeIter<'a, rocksdb::Transaction<'db, OptimisticTransactionDB>>
        where
            Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        let upper_bound = to.as_ref().to_vec();
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound.clone());
        let iter = self.transaction.iterator_opt(
            rocksdb::IteratorMode::From(from.as_ref(), rocksdb::Direction::Forward),
            read_opt,
        );

        Self::IterRange {
            iter,
            check_upper_bound: Some(upper_bound),
        }
    }
}

impl DBWrite for RocksDBTransaction<'_> {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.transaction.put(key, value)?;
        Ok(())
    }

    fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        self.transaction.delete(key)?;
        Ok(())
    }
}

impl DBTransaction for RocksDBTransaction<'_> {
    fn rollback(self) -> Result<()> {
        self.transaction.rollback()?;
        Ok(())
    }

    fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        Ok(self
            .transaction
            .get_for_update(key, true)?
            .map(|v| RocksDBBytes::Owned(v.into())))
    }
}
