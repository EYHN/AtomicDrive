use std::marker::PhantomData;

use db::{backend::rocks::RocksDB, DBRead, DBTransaction, DBWrite, DB};
use utils::{Deserialize, Serialize};

use crate::{
    Error, LogOp, Result, TrieContent, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef, CONFLICT,
    ROOT,
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
            b"r" => Ok(Self::RefIdIndex(
                TrieRef::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
            b"n" => Ok(Self::NodeInfo(
                TrieId::deserialize(args).map_err(Error::DecodeError)?.0,
            )),
            b"c" => {
                let (id, args) = TrieId::deserialize(args).map_err(Error::DecodeError)?;
                let (key, _) = TrieKey::deserialize(args).map_err(Error::DecodeError)?;

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
enum Values<M: TrieMarker + Serialize + Deserialize, C: TrieContent + Serialize + Deserialize> {
    RefIdIndex(TrieId),
    NodeInfo(TrieNode<C>),
    NodeChild(TrieId),
    IdRefsIndex(Vec<TrieRef>),
    AutoIncrementId(TrieId),
    LogTotalLength(u64),
    Log(LogOp<M, C>),
}

impl<M: TrieMarker + Serialize + Deserialize, C: TrieContent + Serialize + Deserialize>
    Values<M, C>
{
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
        let error = || Error::DecodeError(format!("Failed to decode value: {bytes:?}"));
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

pub struct TrieDBBackend<
    DBImpl: DB,
    M: TrieMarker + Serialize + Deserialize,
    C: TrieContent + Serialize + Deserialize,
> {
    db: DBImpl,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<
        DBImpl: DB + Clone,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > Clone for TrieDBBackend<DBImpl, M, C>
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            m: self.m,
            c: self.c,
        }
    }
}

impl<
        DBImpl: DB + Default,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > Default for TrieDBBackend<DBImpl, M, C>
{
    fn default() -> Self {
        Self {
            db: Default::default(),
            m: Default::default(),
            c: Default::default(),
        }
    }
}

impl<M: TrieMarker + Serialize + Deserialize, C: TrieContent + Serialize + Deserialize>
    TrieDBBackend<RocksDB, M, C>
{
    pub fn open_or_create_rocks_db() -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(11));
        opts.create_if_missing(true);

        todo!()
    }
}

impl<
        DBImpl: DB,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > TrieDBBackend<DBImpl, M, C>
{
    pub fn init(db: DBImpl) -> Result<Self> {
        let mut this = Self {
            db,
            m: Default::default(),
            c: Default::default(),
        };

        let mut writer = this.write()?;
        writer.set(
            Keys::AutoIncrementId,
            Values::AutoIncrementId(TrieId::from(10)),
        )?;
        writer.set(Keys::LogTotalLength, Values::LogTotalLength(0))?;
        writer.set(
            Keys::NodeInfo(ROOT),
            Values::NodeInfo(TrieNode {
                parent: ROOT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        writer.set(
            Keys::NodeInfo(CONFLICT),
            Values::NodeInfo(TrieNode {
                parent: CONFLICT,
                key: TrieKey(Default::default()),
                content: Default::default(),
            }),
        )?;
        writer.set(Keys::RefIdIndex(TrieRef::from(0)), Values::RefIdIndex(ROOT))?;
        writer.set(
            Keys::IdRefsIndex(ROOT),
            Values::IdRefsIndex(vec![TrieRef::from(0)]),
        )?;

        writer.commit()?;

        Ok(this)
    }

    fn get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.db.get(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }
}

pub struct TrieDBBackendChildrenIter<
    'a,
    M: TrieMarker + Serialize + Deserialize,
    C: TrieContent + Serialize + Deserialize,
    DBReadImpl: DBRead + 'a,
> {
    iter: DBReadImpl::IterRange<'a>,
    c: PhantomData<C>,
    m: PhantomData<M>,
}

impl<
        'a,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
        DBReadImpl: DBRead + 'a,
    > Iterator for TrieDBBackendChildrenIter<'a, M, C, DBReadImpl>
{
    type Item = Result<(TrieKey, TrieId)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|i| {
            i.map_err(Error::from)
                .and_then(|item| {
                    let key = Keys::parse(item.0.as_ref())?;
                    let value = Values::<M, C>::parse(&key, item.1.as_ref())?.node_child()?;
                    let key = key.node_child()?.1;

                    Ok(Some((key, value)))
                })
                .transpose()
        })
    }
}

pub struct TrieDBBackendLogIter<
    'a,
    M: TrieMarker + Serialize + Deserialize,
    C: TrieContent + Serialize + Deserialize,
    DBReadImpl: DBRead + 'a,
> {
    iter: DBReadImpl::IterRange<'a>,
    c: PhantomData<C>,
    m: PhantomData<M>,
}

impl<
        'a,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
        DBReadImpl: DBRead,
    > Iterator for TrieDBBackendLogIter<'a, M, C, DBReadImpl>
{
    type Item = Result<LogOp<M, C>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| {
            i.map_err(Error::from).and_then(|item| {
                let key = Keys::parse(item.0.as_ref())?;
                let value = Values::<M, C>::parse(&key, item.1.as_ref())?.log()?;

                Ok(value)
            })
        })
    }
}

impl<
        DBImpl: DB,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > TrieBackend<M, C> for TrieDBBackend<DBImpl, M, C>
{
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

    type GetChildren<'a> = TrieDBBackendChildrenIter<'a, M, C, DBImpl>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.db.get_range(&prefix, &upper_bound);

        Ok(TrieDBBackendChildrenIter {
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

    type IterLogItem<'a> = LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = TrieDBBackendLogIter<'a, M, C, DBImpl>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.db.get_range(&prefix, &upper_bound);

        Ok(TrieDBBackendLogIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
    }

    type Writer<'a> = TrieDBBackendWriter<'a, DBImpl, M, C>
    where Self: 'a;

    fn write(&'_ mut self) -> Result<Self::Writer<'_>> {
        Ok(Self::Writer {
            transaction: self.db.start_transaction()?,
            m: Default::default(),
            c: Default::default(),
        })
    }
}

pub struct TrieDBBackendWriter<'a, DBImpl: DB + 'a, M: TrieMarker, C: TrieContent> {
    transaction: DBImpl::Transaction<'a>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<
        DBImpl: DB,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > TrieDBBackendWriter<'_, DBImpl, M, C>
{
    fn get(&self, key: Keys) -> Result<Option<Values<M, C>>> {
        if let Some(value) = self.transaction.get(key.to_bytes())? {
            Ok(Some(Values::parse(&key, value.as_ref())?))
        } else {
            Ok(None)
        }
    }

    fn set(&mut self, key: Keys, value: Values<M, C>) -> Result<()> {
        self.transaction.set(key.to_bytes(), value.to_bytes())?;
        Ok(())
    }

    fn del(&mut self, key: Keys) -> Result<()> {
        self.transaction.delete(key.to_bytes())?;
        Ok(())
    }
}

impl<
        'db,
        DBImpl: DB,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > TrieBackend<M, C> for TrieDBBackendWriter<'db, DBImpl, M, C>
{
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

    type GetChildren<'a> = TrieDBBackendChildrenIter<'a, M, C, DBImpl::Transaction<'db>>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.transaction.get_range(&prefix, &upper_bound);

        Ok(TrieDBBackendChildrenIter {
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

    type IterLogItem<'a> = LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = TrieDBBackendLogIter<'a, M, C, DBImpl::Transaction<'db>>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let iter = self.transaction.get_range(&prefix, &upper_bound);

        Ok(TrieDBBackendLogIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
    }

    type Writer<'a> = TrieDBBackendWriter<'a, DBImpl, M, C>
    where Self: 'a;

    fn write(&'_ mut self) -> Result<Self::Writer<'_>> {
        Err(Error::InvalidOp("not support".to_string()))
    }
}

impl<
        'a,
        DBImpl: DB,
        M: TrieMarker + Serialize + Deserialize,
        C: TrieContent + Serialize + Deserialize,
    > TrieBackendWriter<'a, M, C> for TrieDBBackendWriter<'a, DBImpl, M, C>
{
    fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>> {
        let old_id = if let Some(id) = self
            .get(Keys::RefIdIndex(r.to_owned()))?
            .map(|v| v.ref_id_index())
            .transpose()?
        {
            if let Some(mut id_refs) = self
                .get(Keys::IdRefsIndex(id))?
                .map(|v| v.id_refs_index())
                .transpose()?
            {
                if let Some(i) = id_refs.iter().position(|id_ref| id_ref == &r) {
                    id_refs.remove(i);
                }
                if id_refs.is_empty() {
                    self.del(Keys::IdRefsIndex(id))?;
                } else {
                    self.set(Keys::IdRefsIndex(id), Values::IdRefsIndex(id_refs))?;
                }
            }
            self.del(Keys::RefIdIndex(r.to_owned()))?;
            Some(id)
        } else {
            None
        };

        if let Some(id) = id {
            self.set(Keys::RefIdIndex(r.to_owned()), Values::RefIdIndex(id))?;
            if let Some(mut refs) = self
                .get(Keys::IdRefsIndex(id))?
                .map(|v| v.id_refs_index())
                .transpose()?
            {
                if refs.iter().all(|item| item != &r) {
                    refs.push(r);
                    self.set(Keys::IdRefsIndex(id), Values::IdRefsIndex(refs))?;
                }
            } else {
                self.set(Keys::IdRefsIndex(id), Values::IdRefsIndex(vec![r]))?;
            }
        }

        Ok(old_id)
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
        let node = self
            .get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()?;

        if let Some(node) = &node {
            self.del(Keys::NodeInfo(id))?;
            self.del(Keys::NodeChild(node.parent, node.key.to_owned()))?;
        }

        if let Some(to) = to {
            self.set(
                Keys::NodeChild(to.0, to.1.to_owned()),
                Values::NodeChild(id),
            )?;

            self.set(
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

    fn pop_log(&mut self) -> Result<Option<LogOp<M, C>>> {
        let log_len = self
            .get(Keys::LogTotalLength)?
            .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?
            .log_total_length()?;

        if log_len == 0 {
            return Ok(None);
        }

        let pop_index = u64::MAX - (log_len - 1);
        let log = self
            .get(Keys::Log(pop_index))?
            .ok_or(Error::TreeBroken("log not found.".to_owned()))?
            .log()?;
        self.set(Keys::LogTotalLength, Values::LogTotalLength(log_len - 1))?;
        self.del(Keys::Log(pop_index))?;

        Ok(Some(log))
    }

    fn push_log(&mut self, log: LogOp<M, C>) -> Result<()> {
        let log_len = self
            .get(Keys::LogTotalLength)?
            .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?
            .log_total_length()?;

        let push_index = u64::MAX - log_len;
        self.set(Keys::Log(push_index), Values::Log(log))?;
        self.set(Keys::LogTotalLength, Values::LogTotalLength(log_len + 1))?;

        Ok(())
    }

    fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    fn rollback(self) -> Result<()> {
        self.transaction.rollback()?;
        Ok(())
    }
}
