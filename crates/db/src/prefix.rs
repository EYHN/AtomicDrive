use std::alloc::{Allocator, Global};

use crate::{DBLock, DBRead, DBTransaction, DBWrite, Result, DB};

fn concat_prefix<A: Allocator>(prefix: &[u8], key: &[u8], alloc: A) -> Vec<u8, A> {
    let mut vec = Vec::with_capacity_in(prefix.len() + key.len(), alloc);
    vec.extend_from_slice(prefix);
    vec.extend_from_slice(key);
    vec
}

pub struct PrefixKey<'a, DBKey: AsRef<[u8]>> {
    key: DBKey,
    prefix: &'a [u8],
}

impl<DBKey: AsRef<[u8]>> AsRef<[u8]> for PrefixKey<'_, DBKey> {
    fn as_ref(&self) -> &[u8] {
        &self.key.as_ref()[self.prefix.len()..]
    }
}

pub struct PrefixRangeIter<
    'a,
    DBKey: AsRef<[u8]>,
    DBValue: AsRef<[u8]>,
    DBIter: Iterator<Item = Result<(DBKey, DBValue)>>,
> {
    iter: DBIter,
    prefix: &'a [u8],
}

impl<
        'a,
        DBKey: AsRef<[u8]>,
        DBValue: AsRef<[u8]>,
        DBIter: Iterator<Item = Result<(DBKey, DBValue)>>,
    > Iterator for PrefixRangeIter<'a, DBKey, DBValue, DBIter>
{
    type Item = Result<(PrefixKey<'a, DBKey>, DBValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| {
            r.map(|(key, value)| {
                (
                    PrefixKey {
                        key,
                        prefix: self.prefix,
                    },
                    value,
                )
            })
        })
    }
}

pub struct Prefix<DBImpl, A: Allocator + Clone = Global> {
    db: DBImpl,
    prefix: Box<[u8], A>,
    alloc: A,
}

impl<DBImpl, A: Allocator + Clone> Prefix<DBImpl, A> {
    pub fn new_in(db: DBImpl, prefix: impl AsRef<[u8]>, alloc: A) -> Self {
        Self {
            db,
            prefix: prefix.as_ref().to_vec_in(alloc.clone()).into(),
            alloc,
        }
    }
}

impl<DBImpl> Prefix<DBImpl> {
    pub fn new(db: DBImpl, prefix: impl AsRef<[u8]>) -> Self {
        Self {
            db,
            prefix: prefix.as_ref().to_vec().into(),
            alloc: Default::default(),
        }
    }
}

impl<DBImpl: DBRead, A: Allocator + Clone> DBRead for Prefix<DBImpl, A> {
    type KeyBytes<'a> = PrefixKey<'a, DBImpl::KeyBytes<'a>>
    where
        Self: 'a;

    type ValueBytes<'a> = DBImpl::ValueBytes<'a>
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        self.db.get(concat_prefix(
            &self.prefix,
            key.as_ref(),
            self.alloc.clone(),
        ))
    }

    fn has(&self, key: impl AsRef<[u8]>) -> crate::Result<bool> {
        self.db.has(concat_prefix(
            &self.prefix,
            key.as_ref(),
            self.alloc.clone(),
        ))
    }

    type IterRange<'a> = PrefixRangeIter<'a, DBImpl::KeyBytes<'a>, DBImpl::ValueBytes<'a> ,DBImpl::IterRange<'a>>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        PrefixRangeIter {
            iter: self.db.get_range(
                concat_prefix(&self.prefix, from.as_ref(), self.alloc.clone()),
                concat_prefix(&self.prefix, to.as_ref(), self.alloc.clone()),
            ),
            prefix: &self.prefix,
        }
    }
}

impl<DBImpl: DB, A: Allocator + Clone> DB for Prefix<DBImpl, A> {
    type Transaction<'a> = PrefixTransaction<'a, DBImpl::Transaction<'a>, A>
    where
        Self: 'a;

    fn start_transaction(&self) -> crate::Result<Self::Transaction<'_>> {
        Ok(PrefixTransaction {
            db: self.db.start_transaction()?,
            prefix: &self.prefix,
            alloc: self.alloc.clone(),
        })
    }

    fn clear(&mut self) -> Result<()> {
        self.db.clear()
    }
}

pub struct PrefixTransaction<'a, DBImpl, A: Allocator + Clone = Global> {
    db: DBImpl,
    prefix: &'a [u8],
    alloc: A,
}

impl<DBImpl: DBRead, A: Allocator + Clone> DBRead for PrefixTransaction<'_, DBImpl, A> {
    type KeyBytes<'a> = PrefixKey<'a, DBImpl::KeyBytes<'a>>
    where
        Self: 'a;

    type ValueBytes<'a> = DBImpl::ValueBytes<'a>
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        self.db
            .get(concat_prefix(self.prefix, key.as_ref(), self.alloc.clone()))
    }

    fn has(&self, key: impl AsRef<[u8]>) -> crate::Result<bool> {
        self.db
            .has(concat_prefix(self.prefix, key.as_ref(), self.alloc.clone()))
    }

    type IterRange<'a> = PrefixRangeIter<'a, DBImpl::KeyBytes<'a>, DBImpl::ValueBytes<'a> ,DBImpl::IterRange<'a>>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        PrefixRangeIter {
            iter: self.db.get_range(
                concat_prefix(self.prefix, from.as_ref(), self.alloc.clone()),
                concat_prefix(self.prefix, to.as_ref(), self.alloc.clone()),
            ),
            prefix: self.prefix,
        }
    }
}

impl<DBImpl: DBLock, A: Allocator + Clone> DBLock for PrefixTransaction<'_, DBImpl, A> {
    type ValueBytes<'a> = DBImpl::ValueBytes<'a>
    where
        Self: 'a;

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        self.db
            .get_for_update(concat_prefix(self.prefix, key.as_ref(), self.alloc.clone()))
    }
}

impl<DBImpl: DBWrite, A: Allocator + Clone> DBWrite for PrefixTransaction<'_, DBImpl, A> {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> crate::Result<()> {
        self.db.set(
            concat_prefix(self.prefix, key.as_ref(), self.alloc.clone()),
            value,
        )
    }

    fn delete(&mut self, key: impl AsRef<[u8]>) -> crate::Result<()> {
        self.db
            .delete(concat_prefix(self.prefix, key.as_ref(), self.alloc.clone()))
    }
}

impl<DBImpl: DBTransaction, A: Allocator + Clone> DBTransaction
    for PrefixTransaction<'_, DBImpl, A>
{
    fn rollback(self) -> crate::Result<()> {
        self.db.rollback()
    }

    fn commit(self) -> crate::Result<()> {
        self.db.commit()
    }
}
