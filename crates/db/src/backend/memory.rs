use std::{marker::PhantomData, sync::Arc};

use parking_lot::RwLock;

use crate::{DBRead, DBTransaction, DBWrite, Result, DB};

type KeyBytes = Box<[u8]>;
type ValueBytes = Arc<[u8]>;

type MapType = std::collections::BTreeMap<KeyBytes, ValueBytes>;

#[derive(Debug, Default)]
pub struct MemoryDB {
    map: Arc<RwLock<MapType>>,
}

impl DBRead for MemoryDB {
    type KeyBytes<'a> = KeyBytes;
    type ValueBytes<'a> = ValueBytes;

    fn get(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.map.read().get(key.as_ref()).cloned())
    }

    fn has(&self, key: impl AsRef<[u8]>) -> crate::Result<bool> {
        Ok(self.map.read().get(key.as_ref()).is_some())
    }

    type IterRange<'a> = MemoryDBRangeIter<'a>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        MemoryDBRangeIter {
            iter: self
                .map
                .read()
                .range::<[u8], _>((
                    std::ops::Bound::Included(from.as_ref()),
                    std::ops::Bound::Excluded(to.as_ref()),
                ))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
                .into_iter(),
            l: PhantomData,
        }
    }
}

pub struct MemoryDBRangeIter<'a> {
    iter: std::vec::IntoIter<(KeyBytes, ValueBytes)>,
    l: PhantomData<&'a u8>,
}

impl<'a> Iterator for MemoryDBRangeIter<'a> {
    type Item = Result<(KeyBytes, ValueBytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| Ok((k, v)))
    }
}

pub struct MemoryDBTransaction<'a> {
    write: parking_lot::RwLockWriteGuard<'a, MapType>,
    rollback: Vec<(KeyBytes, Option<ValueBytes>)>,
}

impl DBRead for MemoryDBTransaction<'_> {
    type KeyBytes<'a> = KeyBytes
    where
        Self: 'a;
    type ValueBytes<'a> = ValueBytes
    where
        Self: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.write.get(key.as_ref()).cloned())
    }

    fn has(&self, key: impl AsRef<[u8]>) -> crate::Result<bool> {
        Ok(self.write.get(key.as_ref()).is_some())
    }

    type IterRange<'a> = MemoryDBRangeIter<'a>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        MemoryDBRangeIter {
            iter: self
                .write
                .range::<[u8], _>((
                    std::ops::Bound::Included(from.as_ref()),
                    std::ops::Bound::Excluded(to.as_ref()),
                ))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
                .into_iter(),
            l: PhantomData,
        }
    }
}

impl DBWrite for MemoryDBTransaction<'_> {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec().into_boxed_slice();
        let old = self.write.insert(key.clone(), Arc::from(value.as_ref()));
        self.rollback.push((key, old));
        Ok(())
    }
}

impl DBTransaction for MemoryDBTransaction<'_> {
    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.write.get(key.as_ref()).cloned())
    }

    fn rollback(mut self) -> Result<()> {
        for (key, value) in self.rollback.into_iter().rev() {
            if let Some(value) = value {
                self.write.insert(key, value);
            } else {
                self.write.remove(&key);
            }
        }
        Ok(())
    }

    fn commit(self) -> Result<()> {
        Ok(())
    }
}

impl DB for MemoryDB {
    type Transaction<'a> = MemoryDBTransaction<'a>;

    fn start_transaction(&self) -> crate::Result<Self::Transaction<'_>> {
        Ok(MemoryDBTransaction {
            write: self.map.write(),
            rollback: Vec::new(),
        })
    }
}
