use std::{
    alloc::{Allocator, Global},
    marker::PhantomData,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{DBLock, DBRead, DBTransaction, DBWrite, Result, DB};

type KeyBytes<A> = Box<[u8], A>;
type ValueBytes = Arc<[u8]>;

type MapType<A> = std::collections::BTreeMap<KeyBytes<A>, ValueBytes, A>;

#[derive(Debug)]
pub struct MemoryDB<A: Allocator + Clone = Global> {
    map: Arc<RwLock<MapType<A>>>,
    alloc: A,
}

impl Default for MemoryDB<Global> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            alloc: Default::default(),
        }
    }
}

impl<A: Allocator + Clone> Clone for MemoryDB<A> {
    fn clone(&self) -> Self {
        Self {
            map: Arc::new(RwLock::new(self.map.as_ref().read().clone())),
            alloc: self.alloc.clone(),
        }
    }
}

impl<A: Allocator + Clone> MemoryDB<A> {
    pub fn new_in(alloc: A) -> Self {
        Self {
            map: Arc::new(RwLock::new(MapType::new_in(alloc.clone()))),
            alloc,
        }
    }
}

impl<A: Allocator + Clone> DBRead for MemoryDB<A> {
    type KeyBytes<'a> = KeyBytes<A>
    where A: 'a;

    type ValueBytes<'a> = ValueBytes
    where A: 'a;

    fn get(&self, key: impl AsRef<[u8]>) -> crate::Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.map.read().get(key.as_ref()).cloned())
    }

    fn has(&self, key: impl AsRef<[u8]>) -> crate::Result<bool> {
        Ok(self.map.read().get(key.as_ref()).is_some())
    }

    type IterRange<'a> = MemoryDBRangeIter<'a, A>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        let mut collection = Vec::new_in(self.alloc.clone());
        collection.extend(
            self.map
                .read()
                .range::<[u8], _>((
                    std::ops::Bound::Included(from.as_ref()),
                    std::ops::Bound::Excluded(to.as_ref()),
                ))
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        MemoryDBRangeIter {
            iter: collection.into_iter(),
            l: PhantomData,
        }
    }
}

pub struct MemoryDBRangeIter<'a, A: Allocator + Clone> {
    iter: std::vec::IntoIter<(KeyBytes<A>, ValueBytes), A>,
    l: PhantomData<&'a u8>,
}

impl<'a, A: Allocator + Clone> Iterator for MemoryDBRangeIter<'a, A> {
    type Item = Result<(KeyBytes<A>, ValueBytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| Ok((k, v)))
    }
}

pub struct MemoryDBTransaction<'a, A: Allocator + Clone = Global> {
    write: parking_lot::RwLockWriteGuard<'a, MapType<A>>,
    alloc: A,
    rollback: Vec<(KeyBytes<A>, Option<ValueBytes>), A>,
}

impl<A: Allocator + Clone> DBRead for MemoryDBTransaction<'_, A> {
    type KeyBytes<'a> = KeyBytes<A>
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

    type IterRange<'a> = MemoryDBRangeIter<'a, A>
    where
        Self: 'a;

    fn get_range(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Self::IterRange<'_> {
        let mut collection = Vec::new_in(self.alloc.clone());
        collection.extend(
            self.write
                .range::<[u8], _>((
                    std::ops::Bound::Included(from.as_ref()),
                    std::ops::Bound::Excluded(to.as_ref()),
                ))
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        MemoryDBRangeIter {
            iter: collection.into_iter(),
            l: PhantomData,
        }
    }
}

impl<A: Allocator + Clone> DBWrite for MemoryDBTransaction<'_, A> {
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key: Box<[u8], A> = key.as_ref().to_vec_in(self.alloc.clone()).into();
        let old = self.write.insert(key.clone(), Arc::from(value.as_ref()));
        self.rollback.push((key, old));
        Ok(())
    }

    fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        let old = self.write.remove(key.as_ref());
        self.rollback
            .push((key.as_ref().to_vec_in(self.alloc.clone()).into(), old));
        Ok(())
    }
}

impl<A: Allocator + Clone> DBLock for MemoryDBTransaction<'_, A> {
    type ValueBytes<'a> = ValueBytes
    where
        Self: 'a;

    fn get_for_update(&self, key: impl AsRef<[u8]>) -> Result<Option<Self::ValueBytes<'_>>> {
        Ok(self.write.get(key.as_ref()).cloned())
    }
}

impl<A: Allocator + Clone> DBTransaction for MemoryDBTransaction<'_, A> {
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

impl<A: Allocator + Clone> DB for MemoryDB<A> {
    type Transaction<'a> = MemoryDBTransaction<'a, A>
    where A: 'a;

    fn start_transaction(&self) -> crate::Result<Self::Transaction<'_>> {
        Ok(MemoryDBTransaction {
            write: self.map.write(),
            alloc: self.alloc.clone(),
            rollback: Vec::with_capacity_in(8, self.alloc.clone()),
        })
    }

    fn clear(&mut self) -> Result<()> {
        self.map.write().clear();
        Ok(())
    }
}
