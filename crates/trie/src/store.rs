use std::{borrow::Borrow, marker::PhantomData};

use db::{backend::rocks::RocksDB, DBRead, DBTransaction, DBWrite, DB};
use utils::{Deserialize, PathTools, Serialize};

use crate::{
    Error, LogOp, Result, TrieContent, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef, CONFLICT,
    ROOT,
};

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
    Logs,
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
            Keys::Logs => b"l",
        });
        key
    }

    fn write_bytes_args(&self, mut key: Vec<u8>) -> Vec<u8> {
        match self {
            Keys::RefIdIndex(r) => key = r.write_to_bytes(key),
            Keys::NodeInfo(id) => key = id.write_to_bytes(key),
            Keys::NodeChild(id, k) => {
                key = id.write_to_bytes(key);
                key.push(b':');
                key = k.write_to_bytes(key)
            }
            Keys::NodeChildren(id) => {
                key = id.write_to_bytes(key);
                key.push(b':')
            }
            Keys::IdRefsIndex(id) => key = id.write_to_bytes(key),
            Keys::AutoIncrementId => {}
            Keys::LogTotalLength => {}
            Keys::Log(index) => key = index.write_to_bytes(key),
            Keys::Logs => {}
        }

        key
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
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
            b"r" => Ok(Self::RefIdIndex(
                TrieRef::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
            b"n" => Ok(Self::NodeInfo(
                TrieId::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
            b"c" => {
                let (id, args) = TrieId::deserialize(args).map_err(Error::DecodeError)?;
                let (key, _) = TrieKey::deserialize(&args[1..]).map_err(Error::DecodeError)?;

                Ok(Self::NodeChild(id, key))
            }
            b"i" => Ok(Self::IdRefsIndex(
                TrieId::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
            b"auto_increment_id" => Ok(Self::AutoIncrementId),
            b"log_total_length" => Ok(Self::LogTotalLength),
            b"l" => Ok(Self::Log(
                u64::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
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
        assert_eq!(
            Keys::parse(&Keys::AutoIncrementId.to_bytes()).unwrap(),
            Keys::AutoIncrementId
        );
        assert_eq!(
            Keys::parse(&Keys::LogTotalLength.to_bytes()).unwrap(),
            Keys::LogTotalLength
        );
        assert_eq!(
            Keys::parse(&Keys::Log(111).to_bytes()).unwrap(),
            Keys::Log(111)
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
    fn value_type(&self) -> &'static str {
        match self {
            Values::RefIdIndex(_) => "RefIdIndex",
            Values::NodeInfo(_) => "NodeInfo",
            Values::NodeChild(_) => "NodeChild",
            Values::IdRefsIndex(_) => "IdRefsIndex",
            Values::AutoIncrementId(_) => "AutoIncrementId",
            Values::LogTotalLength(_) => "LogTotalLength",
            Values::Log(_) => "Log",
        }
    }
    fn write_to_bytes(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        match self {
            Values::RefIdIndex(id) => bytes = id.write_to_bytes(bytes),
            Values::NodeInfo(node) => bytes = node.write_to_bytes(bytes),
            Values::NodeChild(id) => bytes = id.write_to_bytes(bytes),
            Values::IdRefsIndex(refs) => bytes = refs.write_to_bytes(bytes),
            Values::AutoIncrementId(id) => bytes = id.write_to_bytes(bytes),
            Values::LogTotalLength(id) => bytes = id.write_to_bytes(bytes),
            Values::Log(log) => bytes = log.write_to_bytes(bytes),
        }

        bytes
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes = self.write_to_bytes(bytes);

        bytes
    }

    fn parse(key: &Keys, bytes: &[u8]) -> Result<Self> {
        Ok(match key {
            Keys::RefIdIndex(_) => Self::RefIdIndex(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::NodeInfo(_) => Self::NodeInfo(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::NodeChild(_, _) => Self::NodeChild(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::IdRefsIndex(_) => Self::IdRefsIndex(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::NodeChildren(_) => {
                panic!("Keys::NodeChildren not have value format")
            }
            Keys::AutoIncrementId => Self::AutoIncrementId(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::LogTotalLength => Self::LogTotalLength(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::Log(_) => Self::Log(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
            Keys::Logs => {
                panic!("Keys::Logs not have value format")
            }
        })
    }

    fn ref_id_index(self) -> Result<TrieId> {
        match self {
            Values::RefIdIndex(id) => Ok(id),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected RefIdIndex but {}",
                self.value_type()
            ))),
        }
    }

    fn node_info(self) -> Result<TrieNode<C>> {
        match self {
            Values::NodeInfo(node) => Ok(node),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected NodeInfo but {}",
                self.value_type()
            ))),
        }
    }

    fn node_child(self) -> Result<TrieId> {
        match self {
            Values::NodeChild(id) => Ok(id),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected NodeChild but {}",
                self.value_type()
            ))),
        }
    }

    fn id_refs_index(self) -> Result<Vec<TrieRef>> {
        match self {
            Values::IdRefsIndex(refs) => Ok(refs),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected IdRefsIndex but {}",
                self.value_type()
            ))),
        }
    }

    fn auto_increment_id(self) -> Result<TrieId> {
        match self {
            Values::AutoIncrementId(id) => Ok(id),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected AutoIncrementId but {}",
                self.value_type()
            ))),
        }
    }

    fn log_total_length(self) -> Result<u64> {
        match self {
            Values::LogTotalLength(len) => Ok(len),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected LogTotalLength but {}",
                self.value_type()
            ))),
        }
    }

    fn log(self) -> Result<LogOp<M, C>> {
        match self {
            Values::Log(log) => Ok(log),
            _ => Err(Error::DecodeError(format!(
                "Value type error, expected Log but {}",
                self.value_type()
            ))),
        }
    }
}

#[cfg(test)]
mod values_tests {
    use crate::{LogOp, Op, TrieId, TrieKey, TrieNode, TrieRef, Undo};

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
                    content: 256
                })
                .to_bytes()
            )
            .unwrap(),
            TestValue::NodeInfo(TrieNode {
                parent: TrieId::from(199),
                key: TrieKey::from("world".to_string()),
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

        assert_eq!(
            TestValue::parse(
                &Keys::AutoIncrementId,
                &TestValue::AutoIncrementId(TrieId::from(555)).to_bytes()
            )
            .unwrap(),
            TestValue::AutoIncrementId(TrieId::from(555))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::LogTotalLength,
                &TestValue::LogTotalLength(456).to_bytes()
            )
            .unwrap(),
            TestValue::LogTotalLength(456)
        );

        let test_log = LogOp {
            op: Op {
                marker: 122,
                child_content: 555,
                child_key: TrieKey("CCC".to_string()),
                child_ref: TrieRef::from(987),
                parent_ref: TrieRef::from(597),
            },
            undos: Vec::from([
                Undo::Move {
                    id: TrieId::from(444),
                    to: Some((TrieId::from(398), TrieKey("eee".to_string()), 494)),
                },
                Undo::Ref(TrieRef::from(375), Some(TrieId::from(222))),
                Undo::Ref(TrieRef::from(664), None),
                Undo::Move {
                    id: TrieId::from(84),
                    to: None,
                },
            ]),
        };

        assert_eq!(
            TestValue::parse(
                &Keys::Log(Default::default()),
                &TestValue::Log(test_log.clone()).to_bytes()
            )
            .unwrap(),
            TestValue::Log(test_log)
        );
    }
}

#[derive(Clone)]
pub struct TrieStore<DBImpl: DB, M: TrieMarker, C: TrieContent> {
    db: DBImpl,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker, C: TrieContent> TrieStore<RocksDB, M, C> {
    pub fn open_or_create_rocks_db() -> Result<Self> {
        // let mut opts = rocksdb::Options::default();
        // opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(11));
        // opts.create_if_missing(true);

        todo!()
    }
}

impl<DBImpl: DB, M: TrieMarker, C: TrieContent> TrieStore<DBImpl, M, C> {
    pub fn init(db: DBImpl) -> Result<Self> {
        let mut this = Self {
            db,
            m: Default::default(),
            c: Default::default(),
        };

        let mut writer = this.write()?;
        writer.db_set(
            Keys::AutoIncrementId,
            Values::AutoIncrementId(TrieId::from(10)),
        )?;
        writer.db_set(Keys::LogTotalLength, Values::LogTotalLength(0))?;
        writer.db_set(
            Keys::NodeInfo(ROOT),
            Values::NodeInfo(TrieNode {
                parent: ROOT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        writer.db_set(
            Keys::NodeInfo(CONFLICT),
            Values::NodeInfo(TrieNode {
                parent: CONFLICT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        writer.db_set(Keys::RefIdIndex(TrieRef::from(0)), Values::RefIdIndex(ROOT))?;
        writer.db_set(
            Keys::IdRefsIndex(ROOT),
            Values::IdRefsIndex(vec![TrieRef::from(0)]),
        )?;

        writer.commit()?;

        Ok(this)
    }

    fn db_get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.db.get(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }

    pub fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.db_get(Keys::RefIdIndex(r))?
            .map(|v| v.ref_id_index())
            .transpose()
    }

    pub fn get_refs(
        &self,
        id: TrieId,
    ) -> Result<Option<impl Iterator<Item = impl Borrow<TrieRef> + '_>>> {
        self.db_get(Keys::IdRefsIndex(id))?
            .map(|v| v.id_refs_index().map(|v| v.into_iter()))
            .transpose()
    }

    pub fn get(&self, id: TrieId) -> Result<Option<impl Borrow<TrieNode<C>> + '_>> {
        self.db_get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()
    }

    pub fn get_children(
        &self,
        id: TrieId,
    ) -> Result<impl Iterator<Item = Result<(impl Borrow<TrieKey> + '_, impl Borrow<TrieId> + '_)>>>
    {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.db.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::parse(item.0.as_ref())?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.node_child()?;
                let key = key.node_child()?.1;

                Ok((key, value))
            })
        }))
    }

    pub fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.db_get(Keys::NodeChild(id, key))?
            .map(|v| v.node_child())
            .transpose()
    }

    pub fn iter_log(&self) -> Result<impl Iterator<Item = Result<impl Borrow<LogOp<M, C>> + '_>>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.db.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::parse(item.0.as_ref())?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.log()?;

                Ok(value)
            })
        }))
    }

    pub fn get_ensure(&self, id: TrieId) -> Result<impl Borrow<TrieNode<C>> + '_> {
        self.get(id)?
            .ok_or_else(|| Error::TreeBroken(format!("Trie id {id} not found")))
    }

    pub fn is_ancestor(&self, child_id: TrieId, ancestor_id: TrieId) -> Result<bool> {
        let mut target_id = child_id;
        while let Some(node) = self.get(target_id)? {
            if node.borrow().parent == ancestor_id {
                return Ok(true);
            }
            target_id = node.borrow().parent;
            if target_id.id() < 10 {
                break;
            }
        }
        Ok(false)
    }

    pub fn get_id_by_path(&self, path: &str) -> Result<Option<TrieId>> {
        let mut id = ROOT;
        if path != "/" {
            for part in PathTools::parts(path) {
                if let Some(child_id) = self.get_child(id, TrieKey(part.to_string()))? {
                    id = child_id
                } else {
                    return Ok(None);
                }
            }
        }

        Ok(Some(id))
    }

    pub fn get_refs_by_path(
        &self,
        path: &str,
    ) -> Result<Option<impl Iterator<Item = impl Borrow<TrieRef> + '_>>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get_refs(id)
            } else {
                Ok(None)
            }
        })
    }

    pub fn get_by_path(&self, path: &str) -> Result<Option<impl Borrow<TrieNode<C>> + '_>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get(id)
            } else {
                Ok(None)
            }
        })
    }

    pub fn write(&'_ mut self) -> Result<TrieStoreWriter<'_, DBImpl, M, C>> {
        Ok(TrieStoreWriter {
            transaction: self.db.start_transaction()?,
            cache_log_total_len: None,
            cache_inc_id: None,
            m: Default::default(),
            c: Default::default(),
        })
    }
}

pub struct TrieStoreWriter<'a, DBImpl: DB + 'a, M: TrieMarker, C: TrieContent> {
    transaction: DBImpl::Transaction<'a>,
    cache_log_total_len: Option<u64>,
    cache_inc_id: Option<TrieId>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<'a, DBImpl: DB + 'a, M: TrieMarker + 'a, C: TrieContent + 'a>
    TrieStoreWriter<'a, DBImpl, M, C>
{
    fn db_get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.transaction.get_for_update(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }

    fn db_set(&mut self, key: Keys, value: Values<M, C>) -> Result<()> {
        self.transaction.set(key.to_bytes(), value.to_bytes())?;
        Ok(())
    }

    fn db_del(&mut self, key: Keys) -> Result<()> {
        self.transaction.delete(key.to_bytes())?;
        Ok(())
    }

    fn log_total_len(&mut self) -> Result<u64> {
        Ok(
            if let Some(cache_log_total_len) = self.cache_log_total_len {
                cache_log_total_len
            } else {
                self.db_get(Keys::LogTotalLength)?
                    .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?
                    .log_total_length()?
            },
        )
    }

    fn update_log_total_len(&mut self, new_len: u64) -> Result<()> {
        self.db_set(Keys::LogTotalLength, Values::LogTotalLength(new_len))
    }

    pub fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.db_get(Keys::RefIdIndex(r))?
            .map(|v| v.ref_id_index())
            .transpose()
    }

    pub fn get_refs(
        &self,
        id: TrieId,
    ) -> Result<Option<impl Iterator<Item = impl Borrow<TrieRef> + '_>>> {
        self.db_get(Keys::IdRefsIndex(id))?
            .map(|v| v.id_refs_index().map(|v| v.into_iter()))
            .transpose()
    }

    pub fn get(&self, id: TrieId) -> Result<Option<impl Borrow<TrieNode<C>> + '_>> {
        self.db_get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()
    }

    pub fn get_children(
        &self,
        id: TrieId,
    ) -> Result<
        impl Iterator<
            Item = Result<(
                impl Borrow<TrieKey> + 'a + '_,
                impl Borrow<TrieId> + 'a + '_,
            )>,
        >,
    > {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.transaction.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::parse(item.0.as_ref())?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.node_child()?;
                let key = key.node_child()?.1;

                Ok((key, value))
            })
        }))
    }

    pub fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.db_get(Keys::NodeChild(id, key))?
            .map(|v| v.node_child())
            .transpose()
    }

    pub fn iter_log(
        &self,
    ) -> Result<impl Iterator<Item = Result<impl Borrow<LogOp<M, C>> + 'a + '_>>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.transaction.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::parse(item.0.as_ref())?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.log()?;

                Ok(value)
            })
        }))
    }

    pub fn get_ensure(&self, id: TrieId) -> Result<impl Borrow<TrieNode<C>> + '_> {
        self.get(id)?
            .ok_or_else(|| Error::TreeBroken(format!("Trie id {id} not found")))
    }

    pub fn is_ancestor(&self, child_id: TrieId, ancestor_id: TrieId) -> Result<bool> {
        let mut target_id = child_id;
        while let Some(node) = self.get(target_id)? {
            if node.borrow().parent == ancestor_id {
                return Ok(true);
            }
            target_id = node.borrow().parent;
            if target_id.id() < 10 {
                break;
            }
        }
        Ok(false)
    }

    pub fn get_id_by_path(&self, path: &str) -> Result<Option<TrieId>> {
        let mut id = ROOT;
        if path != "/" {
            for part in PathTools::parts(path) {
                if let Some(child_id) = self.get_child(id, TrieKey(part.to_string()))? {
                    id = child_id
                } else {
                    return Ok(None);
                }
            }
        }

        Ok(Some(id))
    }

    pub fn get_refs_by_path(
        &self,
        path: &str,
    ) -> Result<Option<impl Iterator<Item = impl Borrow<TrieRef> + '_>>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get_refs(id)
            } else {
                Ok(None)
            }
        })
    }

    pub fn get_by_path(&self, path: &str) -> Result<Option<impl Borrow<TrieNode<C>> + '_>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get(id)
            } else {
                Ok(None)
            }
        })
    }

    pub fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>> {
        let old_id = if let Some(id) = self
            .db_get(Keys::RefIdIndex(r.to_owned()))?
            .map(|v| v.ref_id_index())
            .transpose()?
        {
            if let Some(mut id_refs) = self
                .db_get(Keys::IdRefsIndex(id))?
                .map(|v| v.id_refs_index())
                .transpose()?
            {
                if let Some(i) = id_refs.iter().position(|id_ref| id_ref == &r) {
                    id_refs.remove(i);
                }
                if id_refs.is_empty() {
                    self.db_del(Keys::IdRefsIndex(id))?;
                } else {
                    self.db_set(Keys::IdRefsIndex(id), Values::IdRefsIndex(id_refs))?;
                }
            }
            self.db_del(Keys::RefIdIndex(r.to_owned()))?;
            Some(id)
        } else {
            None
        };

        if let Some(id) = id {
            self.db_set(Keys::RefIdIndex(r.to_owned()), Values::RefIdIndex(id))?;
            if let Some(mut refs) = self
                .db_get(Keys::IdRefsIndex(id))?
                .map(|v| v.id_refs_index())
                .transpose()?
            {
                if refs.iter().all(|item| item != &r) {
                    refs.push(r);
                    self.db_set(Keys::IdRefsIndex(id), Values::IdRefsIndex(refs))?;
                }
            } else {
                self.db_set(Keys::IdRefsIndex(id), Values::IdRefsIndex(vec![r]))?;
            }
        }

        Ok(old_id)
    }

    pub fn create_id(&mut self) -> Result<TrieId> {
        let id = if let Some(cache_inc_id) = self.cache_inc_id {
            cache_inc_id
        } else {
            self.db_get(Keys::AutoIncrementId)?
                .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?
                .auto_increment_id()?
        };
        let new_id = id.inc();

        self.db_set(Keys::AutoIncrementId, Values::AutoIncrementId(new_id))?;

        self.cache_inc_id = Some(new_id);

        Ok(new_id)
    }

    pub fn set_tree_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        let node = self
            .db_get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()?;

        if let Some(node) = &node {
            self.db_del(Keys::NodeInfo(id))?;
            self.db_del(Keys::NodeChild(node.parent, node.key.to_owned()))?;
        }

        if let Some(to) = to {
            self.db_set(
                Keys::NodeChild(to.0, to.1.to_owned()),
                Values::NodeChild(id),
            )?;

            self.db_set(
                Keys::NodeInfo(id),
                Values::NodeInfo(TrieNode {
                    parent: to.0,
                    key: to.1,
                    content: to.2,
                }),
            )?;
        }

        Ok(node.map(|n| (n.parent, n.key, n.content)))
    }

    pub fn pop_log(&mut self) -> Result<Option<LogOp<M, C>>> {
        let log_len = self.log_total_len()?;

        if log_len == 0 {
            return Ok(None);
        }

        let pop_index = u64::MAX - (log_len - 1);
        let log = self
            .db_get(Keys::Log(pop_index))?
            .ok_or(Error::TreeBroken("log not found.".to_owned()))?
            .log()?;
        self.db_del(Keys::Log(pop_index))?;
        self.update_log_total_len(log_len - 1)?;

        Ok(Some(log))
    }

    pub fn push_log(&mut self, log: LogOp<M, C>) -> Result<()> {
        let log_len = self.log_total_len()?;

        let push_index = u64::MAX - log_len;
        self.db_set(Keys::Log(push_index), Values::Log(log))?;
        self.update_log_total_len(log_len + 1)?;

        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.rollback()?;
        Ok(())
    }
}
