use std::{borrow::Borrow, marker::PhantomData};

use db::{DBLock, DBRead, DBTransaction, DBWrite, DB};
use utils::{Deserialize, PathTools, Serialize, Serializer};

use super::{
    Error, LogOp, Result, TrieContent, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef, CONFLICT,
    CONFLICT_REF, RECYCLE, RECYCLE_REF, ROOT, ROOT_REF,
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
    GlobalLock,
}

impl Serialize for Keys {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer.extend_from_slice(self.bytes_label());
        serializer.push(b':');
        match self {
            Keys::RefIdIndex(r) => serializer = r.serialize(serializer),
            Keys::NodeInfo(id) => serializer = id.serialize(serializer),
            Keys::NodeChild(id, k) => {
                serializer = id.serialize(serializer);
                serializer.push(b':');
                serializer = k.serialize(serializer)
            }
            Keys::NodeChildren(id) => {
                serializer = id.serialize(serializer);
                serializer.push(b':')
            }
            Keys::IdRefsIndex(id) => serializer = id.serialize(serializer),
            Keys::AutoIncrementId => {}
            Keys::LogTotalLength => {}
            Keys::Log(index) => serializer = index.serialize(serializer),
            Keys::Logs => {}
            Keys::GlobalLock => {}
        }

        serializer
    }

    fn byte_size(&self) -> Option<usize> {
        Some(
            self.bytes_label().len() + 1 + {
                match self {
                    Keys::RefIdIndex(r) => r.byte_size()?,
                    Keys::NodeInfo(id) => id.byte_size()?,
                    Keys::NodeChild(id, k) => id.byte_size()? + 1 + k.byte_size()?,
                    Keys::NodeChildren(id) => id.byte_size()? + 1,
                    Keys::IdRefsIndex(id) => id.byte_size()?,
                    Keys::AutoIncrementId => 0,
                    Keys::LogTotalLength => 0,
                    Keys::Log(index) => index.byte_size()?,
                    Keys::Logs => 0,
                    Keys::GlobalLock => 0,
                }
            },
        )
    }
}

impl Deserialize for Keys {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (label, args) = bytes.split_at(
            bytes
                .iter()
                .position(|b| b == &b':')
                .ok_or("Failed deserialize keys.")?,
        );
        let args = &args[1..];

        match label {
            b"r" => {
                let (r, rest) = TrieRef::deserialize(args)?;
                Ok((Self::RefIdIndex(r), rest))
            }
            b"n" => {
                let (id, rest) = TrieId::deserialize(args)?;
                Ok((Self::NodeInfo(id), rest))
            }
            b"c" => {
                let (id, args) = TrieId::deserialize(args)?;
                let (key, rest) = TrieKey::deserialize(&args[1..])?;

                Ok((Self::NodeChild(id, key), rest))
            }
            b"i" => {
                let (id, rest) = TrieId::deserialize(args)?;
                Ok((Self::IdRefsIndex(id), rest))
            }
            b"auto_increment_id" => Ok((Self::AutoIncrementId, args)),
            b"log_total_length" => Ok((Self::LogTotalLength, args)),
            b"l" => {
                let (log_id, rest) = u64::deserialize(args)?;
                Ok((Self::Log(log_id), rest))
            }
            b"global_lock" => Ok((Self::GlobalLock, args)),
            _ => Err("Failed deserialize keys.".to_string()),
        }
    }
}
impl Keys {
    fn bytes_label(&self) -> &'static [u8] {
        match self {
            Keys::RefIdIndex(_) => b"r",
            Keys::NodeInfo(_) => b"n",
            Keys::NodeChild(_, _) => b"c",
            Keys::NodeChildren(_) => b"c",
            Keys::IdRefsIndex(_) => b"i",
            Keys::AutoIncrementId => b"auto_increment_id",
            Keys::LogTotalLength => b"log_total_length",
            Keys::Log(_) => b"l",
            Keys::Logs => b"l",
            Keys::GlobalLock => b"global_lock",
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
    use utils::{Deserialize, Serialize};

    use super::{TrieId, TrieKey, TrieRef};

    use super::Keys;

    #[test]
    fn test_keys() {
        assert_eq!(
            Keys::from_bytes(&Keys::RefIdIndex(TrieRef::from(999)).to_bytes()).unwrap(),
            Keys::RefIdIndex(TrieRef::from(999))
        );
        assert_eq!(
            Keys::from_bytes(&Keys::NodeInfo(TrieId::from(999)).to_bytes()).unwrap(),
            Keys::NodeInfo(TrieId::from(999))
        );
        assert_eq!(
            Keys::from_bytes(
                &Keys::NodeChild(TrieId::from(999), TrieKey::from("hello".to_owned())).to_bytes()
            )
            .unwrap(),
            Keys::NodeChild(TrieId::from(999), TrieKey::from("hello".to_owned()))
        );
        assert_eq!(
            Keys::from_bytes(&Keys::IdRefsIndex(TrieId::from(999)).to_bytes()).unwrap(),
            Keys::IdRefsIndex(TrieId::from(999))
        );
        assert_eq!(
            Keys::from_bytes(&Keys::AutoIncrementId.to_bytes()).unwrap(),
            Keys::AutoIncrementId
        );
        assert_eq!(
            Keys::from_bytes(&Keys::LogTotalLength.to_bytes()).unwrap(),
            Keys::LogTotalLength
        );
        assert_eq!(
            Keys::from_bytes(&Keys::Log(111).to_bytes()).unwrap(),
            Keys::Log(111)
        );
        assert_eq!(
            Keys::from_bytes(&Keys::GlobalLock.to_bytes()).unwrap(),
            Keys::GlobalLock
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
    GlobalLock(bool),
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
            Values::GlobalLock(_) => "GlobalLock",
        }
    }
    fn to_bytes(&self) -> impl AsRef<[u8]> {
        match self {
            Values::RefIdIndex(id) => id.to_bytes(),
            Values::NodeInfo(node) => node.to_bytes(),
            Values::NodeChild(id) => id.to_bytes(),
            Values::IdRefsIndex(refs) => refs.to_bytes(),
            Values::AutoIncrementId(id) => id.to_bytes(),
            Values::LogTotalLength(id) => id.to_bytes(),
            Values::Log(log) => log.to_bytes(),
            Values::GlobalLock(lock) => lock.to_bytes(),
        }
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
            Keys::GlobalLock => Self::GlobalLock(
                Deserialize::deserialize(bytes)
                    .map_err(Error::DecodeError)?
                    .0,
            ),
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
    use super::super::{LogOp, Op, TrieId, TrieKey, TrieNode, TrieRef, Undo};

    use super::{Keys, Values};

    type TestValue = Values<u64, u64>;

    #[test]
    fn test_values() {
        assert_eq!(
            TestValue::parse(
                &Keys::RefIdIndex(Default::default()),
                TestValue::RefIdIndex(TrieId::from(12)).to_bytes().as_ref()
            )
            .unwrap(),
            Values::RefIdIndex(TrieId::from(12))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::NodeInfo(Default::default()),
                TestValue::NodeInfo(TrieNode {
                    parent: TrieId::from(199),
                    key: TrieKey::from("world".to_string()),
                    content: 256
                })
                .to_bytes()
                .as_ref()
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
                TestValue::NodeChild(TrieId::from(999)).to_bytes().as_ref()
            )
            .unwrap(),
            Values::NodeChild(TrieId::from(999))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::IdRefsIndex(Default::default()),
                TestValue::IdRefsIndex(vec![TrieRef::from(156), TrieRef::from(8888)])
                    .to_bytes()
                    .as_ref()
            )
            .unwrap(),
            TestValue::IdRefsIndex(vec![TrieRef::from(156), TrieRef::from(8888)])
        );

        assert_eq!(
            TestValue::parse(
                &Keys::AutoIncrementId,
                TestValue::AutoIncrementId(TrieId::from(555))
                    .to_bytes()
                    .as_ref()
            )
            .unwrap(),
            TestValue::AutoIncrementId(TrieId::from(555))
        );

        assert_eq!(
            TestValue::parse(
                &Keys::LogTotalLength,
                TestValue::LogTotalLength(456).to_bytes().as_ref()
            )
            .unwrap(),
            TestValue::LogTotalLength(456)
        );

        let test_log = LogOp {
            op: Op {
                marker: 122,
                child_content: Some(555),
                child_key: TrieKey("CCC".to_string()),
                child_target: TrieRef::from(987).into(),
                parent_target: TrieRef::from(597).into(),
            },
            undos: Vec::from([
                Undo::Move {
                    id: TrieId::from(444),
                    to: Some((TrieId::from(398), TrieKey("eee".to_string()), Some(494))),
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
                TestValue::Log(test_log.clone()).to_bytes().as_ref()
            )
            .unwrap(),
            TestValue::Log(test_log)
        );

        assert_eq!(
            TestValue::parse(
                &Keys::GlobalLock,
                TestValue::GlobalLock(true).to_bytes().as_ref()
            )
            .unwrap(),
            TestValue::GlobalLock(true)
        );
    }
}

pub trait TrieStoreRead<M: TrieMarker, C: TrieContent> {
    type DBReadImpl<'a>: DBRead
    where
        Self: 'a;
    fn db<'a>(&'a self) -> Self::DBReadImpl<'a>;

    fn db_get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.db().get(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }

    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.db_get(Keys::RefIdIndex(r))?
            .map(|v| v.ref_id_index())
            .transpose()
    }

    fn get_id_ensure(&self, r: TrieRef) -> Result<TrieId> {
        self.db_get(Keys::RefIdIndex(r))?
            .ok_or(Error::TreeBroken("ref not found".to_string()))
            .and_then(|v| v.ref_id_index())
    }

    fn get_refs(&self, id: TrieId) -> Result<Option<Vec<TrieRef>>> {
        self.db_get(Keys::IdRefsIndex(id))?
            .map(|v| v.id_refs_index())
            .transpose()
    }

    fn get(&self, id: TrieId) -> Result<Option<TrieNode<C>>> {
        self.db_get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()
    }

    fn get_children(&self, id: TrieId) -> Result<Vec<(TrieKey, TrieId)>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let db = self.db();
        let iter = db.get_range(&prefix, &upper_bound);

        let mut children = vec![];

        for item in iter {
            let item = item?;
            let key = Keys::from_bytes(item.0.as_ref()).map_err(Error::DecodeError)?;
            let value = Values::<M, C>::parse(&key, item.1.as_ref())?.node_child()?;
            let key = key.node_child()?.1;

            children.push((key, value))
        }

        Ok(children)
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.db_get(Keys::NodeChild(id, key))?
            .map(|v| v.node_child())
            .transpose()
    }

    fn get_ensure(&self, id: TrieId) -> Result<TrieNode<C>> {
        self.get(id)?
            .ok_or_else(|| Error::TreeBroken(format!("Trie id {id} not found")))
    }

    fn is_ancestor(&self, child_id: TrieId, ancestor_id: TrieId) -> Result<bool> {
        let mut target_id = child_id;
        while let Some(node) = self.get(target_id)? {
            if node.parent == ancestor_id {
                return Ok(true);
            }
            target_id = node.parent;
            if target_id.id() < 10 {
                break;
            }
        }
        Ok(false)
    }

    fn get_id_by_path(&self, path: &str) -> Result<Option<TrieId>> {
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

    fn get_refs_by_path(&self, path: &str) -> Result<Option<Vec<TrieRef>>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get_refs(id)
            } else {
                Ok(None)
            }
        })
    }

    fn get_by_path(&self, path: &str) -> Result<Option<TrieNode<C>>> {
        self.get_id_by_path(path).and_then(|id| {
            if let Some(id) = id {
                self.get(id)
            } else {
                Ok(None)
            }
        })
    }
}

#[derive(Clone)]
pub struct TrieStore<DBImpl, M: TrieMarker, C: TrieContent> {
    db: DBImpl,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<DBImpl: DBRead, M: TrieMarker, C: TrieContent> TrieStore<DBImpl, M, C> {
    pub fn from_db(db: DBImpl) -> Self {
        Self {
            db,
            m: Default::default(),
            c: Default::default(),
        }
    }

    pub fn iter_log(&self) -> Result<impl Iterator<Item = Result<LogOp<M, C>>> + '_> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.db.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::from_bytes(item.0.as_ref()).map_err(Error::DecodeError)?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.log()?;

                Ok(value)
            })
        }))
    }
}

impl<DBImpl: DBRead, M: TrieMarker, C: TrieContent> TrieStoreRead<M, C>
    for TrieStore<DBImpl, M, C>
{
    type DBReadImpl<'a> = &'a DBImpl
    where Self: 'a;

    fn db(&self) -> Self::DBReadImpl<'_> {
        &self.db
    }
}

impl<DBImpl: DB, M: TrieMarker, C: TrieContent> TrieStore<DBImpl, M, C> {
    pub fn init(db: DBImpl) -> Result<Self> {
        let mut this = Self::from_db(db);

        let mut transaction = this.start_transaction()?;
        transaction.db_set(
            Keys::AutoIncrementId,
            Values::AutoIncrementId(TrieId::from(10)),
        )?;
        transaction.db_set(Keys::LogTotalLength, Values::LogTotalLength(0))?;
        transaction.db_set(
            Keys::NodeInfo(ROOT),
            Values::NodeInfo(TrieNode {
                parent: ROOT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        transaction.db_set(
            Keys::NodeInfo(CONFLICT),
            Values::NodeInfo(TrieNode {
                parent: CONFLICT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        transaction.db_set(
            Keys::NodeInfo(RECYCLE),
            Values::NodeInfo(TrieNode {
                parent: RECYCLE,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        transaction.db_set(Keys::RefIdIndex(ROOT_REF), Values::RefIdIndex(ROOT))?;
        transaction.db_set(Keys::IdRefsIndex(ROOT), Values::IdRefsIndex(vec![ROOT_REF]))?;
        transaction.db_set(Keys::RefIdIndex(CONFLICT_REF), Values::RefIdIndex(CONFLICT))?;
        transaction.db_set(
            Keys::IdRefsIndex(CONFLICT),
            Values::IdRefsIndex(vec![CONFLICT_REF]),
        )?;
        transaction.db_set(Keys::RefIdIndex(RECYCLE_REF), Values::RefIdIndex(RECYCLE))?;
        transaction.db_set(
            Keys::IdRefsIndex(RECYCLE),
            Values::IdRefsIndex(vec![RECYCLE_REF]),
        )?;
        transaction.db_set(Keys::GlobalLock, Values::GlobalLock(true))?;

        transaction.commit()?;

        Ok(this)
    }

    pub fn start_transaction(
        &'_ mut self,
    ) -> Result<TrieStoreTransaction<DBImpl::Transaction<'_>, M, C>> {
        let transaction = TrieStoreTransaction::from_db(self.db.start_transaction()?);

        transaction.lock()?;

        Ok(transaction)
    }
}

pub struct TrieStoreTransaction<DBImpl: DBRead + DBWrite + DBLock, M: TrieMarker, C: TrieContent> {
    transaction: DBImpl,
    cache_log_total_len: Option<u64>,
    cache_inc_id: Option<TrieId>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<DBImpl: DBRead + DBWrite + DBLock, M: TrieMarker, C: TrieContent> TrieStoreRead<M, C>
    for TrieStoreTransaction<DBImpl, M, C>
{
    type DBReadImpl<'a> = &'a DBImpl
    where Self: 'a;

    fn db(&self) -> Self::DBReadImpl<'_> {
        &self.transaction
    }
}

impl<DBImpl: DBRead + DBWrite + DBLock, M: TrieMarker, C: TrieContent>
    TrieStoreTransaction<DBImpl, M, C>
{
    pub fn from_db(db: DBImpl) -> Self {
        Self {
            transaction: db,
            cache_log_total_len: None,
            cache_inc_id: None,
            m: Default::default(),
            c: Default::default(),
        }
    }

    pub fn lock(&self) -> Result<()> {
        self.db_get_for_update(Keys::GlobalLock)?;

        Ok(())
    }

    fn db_get_for_update(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.transaction.get_for_update(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }

    fn db_get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.transaction.get(key.to_bytes())? {
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
                    .ok_or(Error::InvalidOp(
                        "Trie Database not initialized.".to_owned(),
                    ))?
                    .log_total_length()?
            },
        )
    }

    fn update_log_total_len(&mut self, new_len: u64) -> Result<()> {
        self.db_set(Keys::LogTotalLength, Values::LogTotalLength(new_len))
    }

    pub fn iter_log(&self) -> Result<impl Iterator<Item = Result<LogOp<M, C>>> + '_> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.transaction.get_range(&prefix, &upper_bound);

        Ok(iter.map(|item| {
            item.map_err(Error::from).and_then(|item| {
                let key = Keys::from_bytes(item.0.as_ref()).map_err(Error::DecodeError)?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.log()?;

                Ok(value)
            })
        }))
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
                .ok_or(Error::InvalidOp(
                    "Trie Database not initialized.".to_owned(),
                ))?
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
        to: Option<(TrieId, TrieKey, Option<C>)>,
    ) -> Result<Option<(TrieId, TrieKey, Option<C>)>> {
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

            let not_update_content = to.2.is_none();

            self.db_set(
                Keys::NodeInfo(id),
                Values::NodeInfo(TrieNode {
                    parent: to.0,
                    key: to.1,
                    content: to
                        .2
                        .or(node.as_ref().map(|n| n.content.clone()))
                        .unwrap_or(Default::default()),
                }),
            )?;

            Ok(node.map(|n| {
                (
                    n.parent,
                    n.key,
                    if not_update_content {
                        None
                    } else {
                        Some(n.content)
                    },
                )
            }))
        } else {
            Ok(node.map(|n| (n.parent, n.key, Some(n.content))))
        }
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
}

impl<DBImpl: DBTransaction, M: TrieMarker, C: TrieContent> TrieStoreTransaction<DBImpl, M, C> {
    pub fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.rollback()?;
        Ok(())
    }
}
