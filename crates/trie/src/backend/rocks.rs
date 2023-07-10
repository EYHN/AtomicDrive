use std::marker::PhantomData;

use rocksdb::{DBAccess, OptimisticTransactionDB, Transaction};

use crate::{
    Error, LogOp, Result, TrieContent, TrieHash, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef,
    TrieSerialize, CONFLICT, ROOT,
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
            Keys::Log(index) => key.extend_from_slice(&index.to_be_bytes()),
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
            b"r" => Ok(Self::RefIdIndex(TrieRef::from_bytes(args)?)),
            b"n" => Ok(Self::NodeInfo(TrieId::from_bytes(args)?)),
            b"c" => {
                let (id, key) = args.split_at(8);

                Ok(Self::NodeChild(
                    TrieId::from_bytes(id)?,
                    TrieKey::from_bytes(&key[1..])?,
                ))
            }
            b"i" => Ok(Self::IdRefsIndex(TrieId::from_bytes(args)?)),
            b"auto_increment_id" => Ok(Self::AutoIncrementId),
            b"log_total_length" => Ok(Self::LogTotalLength),
            b"l" => Ok(Self::Log(u64::from_be_bytes(
                args.try_into().map_err(|_| error())?,
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
enum Values<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> {
    RefIdIndex(TrieId),
    NodeInfo(TrieNode<C>),
    NodeChild(TrieId),
    IdRefsIndex(Vec<TrieRef>),
    AutoIncrementId(TrieId),
    LogTotalLength(u64),
    Log(LogOp<M, C>),
}

impl<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> Values<M, C> {
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
            Values::IdRefsIndex(refs) => {
                for r in refs.iter() {
                    bytes = r.write_to_bytes(bytes);
                }
            }
            Values::AutoIncrementId(id) => bytes.extend_from_slice(&id.id().to_be_bytes()),
            Values::LogTotalLength(id) => bytes.extend_from_slice(&id.to_be_bytes()),
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
            Keys::Log(_) => Self::Log(LogOp::<M, C>::from_bytes(bytes)?),
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
    use crate::{LogOp, Op, TrieHash, TrieId, TrieKey, TrieNode, TrieRef, Undo};

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

pub struct TrieRocksBackend<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> {
    db: OptimisticTransactionDB,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> TrieRocksBackend<M, C> {
    pub fn open_or_create_database(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(11));
        opts.create_if_missing(true);

        let db = OptimisticTransactionDB::open(&opts, path)?;
        let mut this = Self {
            db,
            m: Default::default(),
            c: Default::default(),
        };

        let writer = this.write()?;
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
                hash: TrieHash::expired(),
                content: Default::default(),
            }),
        )?;
        writer.set(
            Keys::NodeInfo(CONFLICT),
            Values::NodeInfo(TrieNode {
                parent: CONFLICT,
                key: TrieKey(Default::default()),
                hash: TrieHash::expired(),
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
        if let Some(value) = self.db.get_pinned(key.to_bytes())? {
            Ok(Some(Values::parse(&key, &value)?))
        } else {
            Ok(None)
        }
    }

    pub fn duplicate(&self, path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = OptimisticTransactionDB::open(&opts, path)?;

        for kv in self.db.iterator(rocksdb::IteratorMode::Start) {
            let kv = kv?;
            db.put(kv.0, kv.1)?;
        }

        Ok(Self {
            db,
            m: Default::default(),
            c: Default::default(),
        })
    }
}

pub struct TrieRocksBackendChildrenIter<
    'a,
    M: TrieMarker + TrieSerialize,
    C: TrieContent + TrieSerialize,
    D: DBAccess,
> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, D>,
    /// try fix: https://github.com/facebook/rocksdb/issues/2343
    check_upper_bound: Option<Vec<u8>>,
    c: PhantomData<C>,
    m: PhantomData<M>,
}

impl<'a, M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize, D: DBAccess> Iterator
    for TrieRocksBackendChildrenIter<'a, M, C, D>
{
    type Item = Result<(TrieKey, TrieId)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|i| {
            i.map_err(|e| Error::from(e))
                .and_then(|item| {
                    if let Some(ref check_upper_bound) = self.check_upper_bound {
                        if &item.0[..] >= &check_upper_bound[..] {
                            return Ok(None);
                        }
                    }
                    let key = Keys::parse(&item.0)?;
                    let value = Values::<M, C>::parse(&key, &item.1)?.node_child()?;
                    let key = key.node_child()?.1;

                    Ok(Some((key, value)))
                })
                .transpose()
        })
    }
}

pub struct TrieRocksBackendLogIter<
    'a,
    M: TrieMarker + TrieSerialize,
    C: TrieContent + TrieSerialize,
    D: DBAccess,
> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, D>,
    c: PhantomData<C>,
    m: PhantomData<M>,
}

impl<'a, M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize, D: DBAccess> Iterator
    for TrieRocksBackendLogIter<'a, M, C, D>
{
    type Item = Result<LogOp<M, C>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| {
            i.map_err(|e| Error::from(e)).and_then(|item| {
                let key = Keys::parse(&item.0)?;
                let value = Values::<M, C>::parse(&key, &item.1)?.log()?;

                Ok(value)
            })
        })
    }
}

impl<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> TrieBackend<M, C>
    for TrieRocksBackend<M, C>
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

    type GetChildren<'a> = TrieRocksBackendChildrenIter<'a, M, C, OptimisticTransactionDB>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendChildrenIter {
            iter,
            check_upper_bound: None,
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
    type IterLog<'a> = TrieRocksBackendLogIter<'a, M, C, OptimisticTransactionDB>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendLogIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
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
    transaction: rocksdb::Transaction<'db, OptimisticTransactionDB>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize>
    TrieRocksBackendWriter<'_, M, C>
{
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

    fn del(&self, key: Keys) -> Result<()> {
        self.transaction.delete(&key.to_bytes())?;
        Ok(())
    }
}

impl<M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> TrieBackend<M, C>
    for TrieRocksBackendWriter<'_, M, C>
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

    type GetChildren<'a> = TrieRocksBackendChildrenIter<'a, M, C, Transaction<'a, OptimisticTransactionDB>>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        let prefix = Keys::NodeChildren(id).to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound.clone());
        let iter = self.transaction.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendChildrenIter {
            iter,
            check_upper_bound: Some(upper_bound),
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
    type IterLog<'a> = TrieRocksBackendLogIter<'a, M, C, Transaction<'a, OptimisticTransactionDB>>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
        let prefix = Keys::Logs.to_bytes();
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let mut iter = self.transaction.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        Ok(TrieRocksBackendLogIter {
            iter,
            c: Default::default(),
            m: Default::default(),
        })
    }

    type Writer<'a> = TrieRocksBackendWriter<'a, M, C>
    where Self: 'a;

    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>> {
        Err(Error::InvalidOp("not support".to_string()))
    }
}

impl<'a, M: TrieMarker + TrieSerialize, C: TrieContent + TrieSerialize> TrieBackendWriter<'a, M, C>
    for TrieRocksBackendWriter<'a, M, C>
{
    fn set_hash(&mut self, id: TrieId, hash: TrieHash) -> Result<()> {
        if let Some(mut info) = self
            .get(Keys::NodeInfo(id))?
            .map(|v| v.node_info())
            .transpose()?
        {
            info.hash = hash;
            self.set(Keys::NodeInfo(id), Values::NodeInfo(info))?;
            Ok(())
        } else {
            Err(Error::TreeBroken(format!("id {id} not found")))
        }
    }

    fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>> {
        let old_id = if let Some(id) = self
            .get(Keys::RefIdIndex(r.to_owned()))?
            .map(|v| v.ref_id_index())
            .transpose()?
        {
            if let Some(mut refs) = self
                .get(Keys::IdRefsIndex(id))?
                .map(|v| v.id_refs_index())
                .transpose()?
            {
                if let Some(i) = refs.iter().position(|r| r == r) {
                    refs.remove(i);
                }
                if refs.is_empty() {
                    self.del(Keys::IdRefsIndex(id))?;
                } else {
                    self.set(Keys::IdRefsIndex(id), Values::IdRefsIndex(refs))?;
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
                    refs.push(r.to_owned());
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
                    hash: TrieHash::expired(),
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
