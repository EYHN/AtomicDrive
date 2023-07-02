use std::{
    collections::{btree_map::Entry as BTreeMapEntry, hash_map::Entry as HashMapEntry, HashSet},
    marker::PhantomData,
};

use rocksdb::{DBAccess, Transaction, TransactionDB};

use crate::{
    Error, LogOp, Result, TrieContent, TrieHash, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef,
    CONFLICT, ROOT,
};

use super::{TrieBackend, TrieBackendWriter};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Keys {
    RefIdIndex(TrieRef),
    NodeInfo(TrieId),
    NodeChild(TrieId, TrieKey),
    NodeChildren(TrieId),
    IdRefsIndex(TrieId),
    AutoIncrementId,
    LogTotalLength,
    Log(u64),
}

impl Keys {
    fn write_bytes_label(&self, mut key: Vec<u8>) -> Vec<u8> {
        key.extend_from_slice(match self {
            Keys::RefIdIndex(_) => b"r",
            Keys::NodeInfo(_) => b"n",
            Keys::NodeChild(_, _) => b"c",
            Keys::NodeChildren(_) => b"c",
            Keys::IdRefsIndex(_) => b"i",
            Keys::AutoIncrementId => b"auto_increment_id",
            Keys::LogTotalLength => b"log_total_length",
            Keys::Log(_) => b"l",
        });
        key
    }

    fn write_bytes_args(&self, mut key: Vec<u8>) -> Vec<u8> {
        match self {
            Keys::RefIdIndex(r) => key.extend_from_slice(r.as_bytes()),
            Keys::NodeInfo(id) => key.extend_from_slice(id.as_bytes()),
            Keys::NodeChild(id, k) => {
                key.extend_from_slice(id.as_bytes());
                key.extend_from_slice(k.as_bytes())
            }
            Keys::NodeChildren(id) => {
                key.extend_from_slice(id.as_bytes());
                key.push(b':')
            }
            Keys::IdRefsIndex(id) => key.extend_from_slice(id.as_bytes()),
            Keys::AutoIncrementId => {}
            Keys::LogTotalLength => {}
            Keys::Log(index) => key.extend_from_slice(&index.to_be_bytes()),
        }

        key
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes = self.write_bytes_label(bytes);
        bytes.push(b':');
        bytes = self.write_bytes_args(bytes);

        bytes
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        let error = || Error::DecodeError(format!("Failed to decode key: {bytes:?}"));
        let (label, args) =
            bytes.split_at(bytes.iter().position(|b| b == &b':').ok_or_else(error)?);
        let args = &args[1..];

        match label {
            b"r" => Ok(Self::RefIdIndex(TrieRef::from_bytes(args)?)),
            b"n" => Ok(Self::NodeInfo(TrieId::from_bytes(args)?)),
            b"c" => {
                let (id, key) = args.split_at(8);

                Ok(Self::NodeChild(
                    TrieId::from_bytes(id)?,
                    TrieKey::from_bytes(key)?,
                ))
            }
            b"i" => Ok(Self::IdRefsIndex(TrieId::from_bytes(args)?)),
            b"auto_increment_id" => Ok(Self::AutoIncrementId),
            b"log_total_length" => Ok(Self::LogTotalLength),
            b"l" => Ok(Self::Log(u64::from_be_bytes(
                bytes.try_into().map_err(|_| error())?,
            ))),
            _ => Err(error()),
        }
    }

    fn node_child(self) -> Result<(TrieId, TrieKey)> {
        match self {
            Keys::NodeChild(id, key) => Ok((id, key)),
            _ => Err(Error::DecodeError("Key type error".to_string())),
        }
    }
}

#[cfg(test)]
mod keys_tests {
    use crate::{TrieId, TrieKey, TrieRef};

    use super::Keys;

    #[test]
    fn test_keys() {
        assert_eq!(
            Keys::parse(&Keys::RefIdIndex(TrieRef::from(999)).to_bytes()).unwrap(),
            Keys::RefIdIndex(TrieRef::from(999))
        );
        assert_eq!(
            Keys::parse(&Keys::NodeInfo(TrieId::from(999)).to_bytes()).unwrap(),
            Keys::NodeInfo(TrieId::from(999))
        );
        assert_eq!(
            Keys::parse(
                &Keys::NodeChild(TrieId::from(999), TrieKey::from("hello".to_owned())).to_bytes()
            )
            .unwrap(),
            Keys::NodeChild(TrieId::from(999), TrieKey::from("hello".to_owned()))
        );
        assert_eq!(
            Keys::parse(&Keys::IdRefsIndex(TrieId::from(999)).to_bytes()).unwrap(),
            Keys::IdRefsIndex(TrieId::from(999))
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Values<M: TrieMarker, C: TrieContent> {
    RefIdIndex(TrieId),
    NodeInfo(TrieNode<C>),
    NodeChild(TrieId),
    IdRefsIndex(Vec<TrieRef>),
    AutoIncrementId(TrieId),
    LogTotalLength(u64),
    Log(LogOp<M, C>),
}

impl<M: TrieMarker, C: TrieContent> Values<M, C> {
    fn write_to_bytes(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        match self {
            Values::RefIdIndex(id) => {
                bytes.extend_from_slice(id.as_bytes());
            }
            Values::NodeInfo(node) => {
                bytes = node.write_to_bytes(bytes);
            }
            Values::NodeChild(id) => {
                bytes.extend_from_slice(id.as_bytes());
            }
            Values::IdRefsIndex(refs) => {
                for r in refs.iter() {
                    bytes.extend_from_slice(r.as_bytes());
                }
            }
            Values::AutoIncrementId(id) => bytes.extend_from_slice(&id.id().to_be_bytes()),
            Values::LogTotalLength(id) => bytes.extend_from_slice(&id.to_be_bytes()),
            Values::Log(log) => todo!(),
        }

        bytes
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes = self.write_to_bytes(bytes);

        bytes
    }

    fn parse(key: &Keys, bytes: &[u8]) -> Result<Self> {
        let error = || Error::DecodeError(format!("Failed to decode value: {bytes:?}"));
        Ok(match key {
            Keys::RefIdIndex(_) => Self::RefIdIndex(TrieId::from_bytes(bytes)?),
            Keys::NodeInfo(_) => Self::NodeInfo(TrieNode::<C>::from_bytes(bytes)?),
            Keys::NodeChild(_, _) => Self::NodeChild(TrieId::from_bytes(bytes)?),
            Keys::IdRefsIndex(_) => {
                let mut refs = vec![];
                for r in bytes.chunks(16) {
                    refs.push(TrieRef::from_bytes(r)?)
                }
                Self::IdRefsIndex(refs)
            }
            Keys::NodeChildren(_) => {
                panic!("Keys::NodeChildren not have value format")
            }
            Keys::AutoIncrementId => Self::AutoIncrementId(TrieId::from_bytes(bytes)?),
            Keys::LogTotalLength => {
                Self::LogTotalLength(u64::from_be_bytes(bytes.try_into().map_err(|_| error())?))
            }
            Keys::Log(_) => {
                todo!()
            }
        })
    }

    fn ref_id_index(self) -> Result<TrieId> {
        match self {
            Values::RefIdIndex(id) => Ok(id),
            _ => Err(Error::DecodeError("Value type error".to_string())),
        }
    }

    fn node_info(self) -> Result<TrieNode<C>> {
        match self {
            Values::NodeInfo(node) => Ok(node),
            _ => Err(Error::DecodeError("Value type error".to_string())),
        }
    }

    fn node_child(self) -> Result<TrieId> {
        match self {
            Values::NodeChild(id) => Ok(id),
            _ => Err(Error::DecodeError("Value type error".to_string())),
        }
    }

    fn id_refs_index(self) -> Result<Vec<TrieRef>> {
        match self {
            Values::IdRefsIndex(refs) => Ok(refs),
            _ => Err(Error::DecodeError("Value type error".to_string())),
        }
    }

    fn auto_increment_id(self) -> Result<TrieId> {
        match self {
            Values::AutoIncrementId(id) => Ok(id),
            _ => Err(Error::DecodeError("Value type error".to_string())),
        }
    }
}

#[cfg(test)]
mod values_tests {
    use crate::{TrieHash, TrieId, TrieKey, TrieNode, TrieRef};

    use super::{Keys, Values};

    type TestValue = Values<u64, u64>;

    #[test]
    fn test_values() {
        assert_eq!(
            TestValue::parse(
                &Keys::RefIdIndex(Default::default()),
                &TestValue::RefIdIndex(TrieId::from(12)).to_bytes()
            )
            .unwrap(),
            Values::RefIdIndex(TrieId::from(12))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::NodeInfo(Default::default()),
                &TestValue::NodeInfo(TrieNode {
                    parent: TrieId::from(199),
                    key: TrieKey::from("world".to_string()),
                    hash: TrieHash::expired(),
                    content: 256
                })
                .to_bytes()
            )
            .unwrap(),
            TestValue::NodeInfo(TrieNode {
                parent: TrieId::from(199),
                key: TrieKey::from("world".to_string()),
                hash: TrieHash::expired(),
                content: 256
            })
        );

        assert_eq!(
            TestValue::parse(
                &Keys::NodeChild(Default::default(), Default::default()),
                &TestValue::NodeChild(TrieId::from(999)).to_bytes()
            )
            .unwrap(),
            Values::NodeChild(TrieId::from(999))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::IdRefsIndex(Default::default()),
                &TestValue::IdRefsIndex(vec![TrieRef::from(156), TrieRef::from(8888)]).to_bytes()
            )
            .unwrap(),
            TestValue::IdRefsIndex(vec![TrieRef::from(156), TrieRef::from(8888)])
        );
    }
}

pub struct TrieRocksBackend<M: TrieMarker, C: TrieContent> {
    db: TransactionDB,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker, C: TrieContent> TrieRocksBackend<M, C> {
    pub fn open_or_create_database(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let mut t_opts = rocksdb::TransactionDBOptions::default();
        t_opts.set_default_lock_timeout(5000);

        let db = TransactionDB::open(&opts, &t_opts, path)?;
        todo!();
        Ok(Self {
            db,
            m: Default::default(),
            c: Default::default(),
        })
    }

    fn get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.db.get_pinned(key.to_bytes())? {
            Ok(Some(Values::parse(&key, &value)?))
        } else {
            Ok(None)
        }
    }
}

pub struct TrieRocksBackendChildrenIter<'a, M: TrieMarker, C: TrieContent, D: DBAccess> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, D>,
    c: PhantomData<C>,
    m: PhantomData<M>,
}

impl<'a, M: TrieMarker, C: TrieContent, D: DBAccess> Iterator
    for TrieRocksBackendChildrenIter<'a, M, C, D>
{
    type Item = Result<(TrieKey, TrieId)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| {
            i.map_err(|e| Error::from(e)).and_then(|item| {
                let key = Keys::parse(&item.0)?;
                let value = Values::<M, C>::parse(&key, &item.1)?.node_child()?;
                let key = key.node_child()?.1;

                Ok((key, value))
            })
        })
    }
}

impl<M: TrieMarker, C: TrieContent> TrieBackend<M, C> for TrieRocksBackend<M, C> {
    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.get(Keys::RefIdIndex(r))?
            .map(|v| v.ref_id_index())
            .transpose()
    }

    type GetRefsRef<'a> = TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::vec::IntoIter<TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Result<Option<Self::GetRefs<'_>>> {
        self.get(Keys::IdRefsIndex(id))?
            .map(|v| v.id_refs_index().map(|v| v.into_iter()))
            .transpose()
    }

    type Get<'a> = TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Result<Option<Self::Get<'_>>> {
        self.get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()
    }

    type GetChildrenKey<'a> = TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = TrieId
    where
        Self: 'a;

    type GetChildren<'a> = TrieRocksBackendChildrenIter<'a, M, C, TransactionDB>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let mut iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendChildrenIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.get(Keys::NodeChild(id, key))?
            .map(|v| v.node_child())
            .transpose()
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = std::iter::Rev<std::slice::Iter<'a, LogOp<M, C>>>
    where
        Self: 'a;
    fn iter_log(&self) -> Self::IterLog<'_> {
        todo!()
        // self.log.iter().rev()
    }

    type Writer<'a> = TrieRocksBackendWriter<'a, M, C>
    where Self: 'a;

    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>> {
        Ok(Self::Writer {
            transaction: self.db.transaction(),
            m: Default::default(),
            c: Default::default(),
        })
    }
}

pub struct TrieRocksBackendWriter<'db, M: TrieMarker, C: TrieContent> {
    transaction: rocksdb::Transaction<'db, TransactionDB>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker, C: TrieContent> TrieRocksBackendWriter<'_, M, C> {
    fn get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.transaction.get_pinned(key.to_bytes())? {
            Ok(Some(Values::parse(&key, &value)?))
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: Keys, value: Values<M, C>) -> Result<()> {
        self.transaction.put(&key.to_bytes(), &value.to_bytes())?;
        Ok(())
    }
}

impl<M: TrieMarker, C: TrieContent> TrieBackend<M, C> for TrieRocksBackendWriter<'_, M, C> {
    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.get(Keys::RefIdIndex(r))?
            .map(|v| v.ref_id_index())
            .transpose()
    }

    type GetRefsRef<'a> = TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::vec::IntoIter<TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Result<Option<Self::GetRefs<'_>>> {
        self.get(Keys::IdRefsIndex(id))?
            .map(|v| v.id_refs_index().map(|v| v.into_iter()))
            .transpose()
    }

    type Get<'a> = TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Result<Option<Self::Get<'_>>> {
        self.get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()
    }

    type GetChildrenKey<'a> = TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = TrieId
    where
        Self: 'a;

    type GetChildren<'a> = TrieRocksBackendChildrenIter<'a, M, C, Transaction<'a, TransactionDB>>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let iter = self.transaction.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendChildrenIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.get(Keys::NodeChild(id, key))?
            .map(|v| v.node_child())
            .transpose()
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = std::iter::Rev<std::slice::Iter<'a, LogOp<M, C>>>
    where
        Self: 'a;
    fn iter_log(&self) -> Self::IterLog<'_> {
        todo!()
        // self.log.iter().rev()
    }

    type Writer<'a> = TrieRocksBackendWriter<'a, M, C>
    where Self: 'a;

    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>> {
        Err(Error::InvalidOp("not support".to_string()))
    }
}

impl<'a, M: TrieMarker, C: TrieContent> TrieBackendWriter<'a, M, C>
    for TrieRocksBackendWriter<'a, M, C>
{
    fn set_hash(&mut self, id: TrieId, hash: TrieHash) -> Result<()> {
        todo!()
    }

    fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>> {
        todo!()
    }

    fn create_id(&mut self) -> Result<TrieId> {
        let id = self
            .get(Keys::AutoIncrementId)?
            .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?
            .auto_increment_id()?;
        let new_id = id.inc();

        self.set(Keys::AutoIncrementId, Values::AutoIncrementId(new_id))?;

        Ok(new_id)
    }

    fn set_tree_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        todo!()
    }

    fn pop_log(&mut self) -> Result<Option<LogOp<M, C>>> {
        todo!()
    }

    fn push_log(&mut self, log: LogOp<M, C>) -> Result<()> {
        todo!()
    }
}
