//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

mod error;

use std::{ops::Deref, path::Path};

use file::{FileFullPath, FileType};
use rocksdb::Direction;

pub type Result<T> = std::result::Result<T, error::Error>;

type TransactionDB = rocksdb::TransactionDB<rocksdb::MultiThreaded>;

#[derive(Debug)]
enum Keys {
    FilePath(FileFullPath),
    FileInfo(FileIdentifier),
}

impl Keys {
    fn bytes_label(&self) -> &'static [u8] {
        match self {
            Keys::FilePath(_) => b"r",
            Keys::FileInfo(_) => b"f",
        }
    }

    fn path_prefix(path_prefix: String) -> Vec<u8> {
        let path_bytes = path_prefix.as_bytes();
        debug_assert!(path_bytes[path_bytes.len() - 1] == b'/');
        let size = path_bytes.len() + 2;
        let mut bytes = Vec::with_capacity(size);
        bytes.push(b'r');
        bytes.push(b':');
        bytes.extend_from_slice(path_bytes);

        debug_assert!(bytes.len() == size);
        bytes
    }

    fn bytes_args(&self) -> Vec<u8> {
        match self {
            Keys::FilePath(path) => path.as_bytes().into(),
            Keys::FileInfo(file_id) => file_id.clone(),
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>> {
        let label = self.bytes_label();
        let args = &self.bytes_args();
        let size = label.len() + args.len() + 1;
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(label);
        bytes.push(b':');
        bytes.extend_from_slice(&args);
        debug_assert!(bytes.len() == size);

        Ok(bytes)
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        let error = || error::Error::DecodeError(format!("Failed to decode key: {bytes:?}"));
        let (label, args) =
            bytes.split_at(bytes.iter().position(|b| b == &b':').ok_or_else(error)?);
        let args = &args[1..];

        match label {
            b"r" => Ok(Self::FilePath(
                FileFullPath::from_bytes(args.into()).map_err(|_| error())?,
            )),
            b"f" => Ok(Self::FileInfo(args.to_vec())),
            _ => Err(error()),
        }
    }
}

#[derive(Debug)]
enum Values {
    FilePath(FileType, FileIdentifier),
    FileInfo(FileUpdateToken),
}

impl Values {
    fn to_bytes(self) -> Result<Vec<u8>> {
        match self {
            Values::FilePath(file_type, file_id) => {
                let size = file_id.len() + 1;
                let mut data: Vec<u8> = Vec::with_capacity(size);
                data.push(file_type.into());
                data.extend_from_slice(&file_id);
                debug_assert!(data.len() == size);
                Ok(data)
            }
            Values::FileInfo(file_update_token) => Ok(file_update_token),
        }
    }

    fn parse(key: &Keys, bytes: Vec<u8>) -> Result<Self> {
        Ok(match key {
            Keys::FilePath(_) => {
                let (file_type, file_handle) = Values::parse_file_path(bytes)?;
                Values::FilePath(file_type, file_handle)
            }
            Keys::FileInfo(_) => Values::FileInfo(Values::parse_file_info(bytes)?),
        })
    }

    fn parse_file_path(mut bytes: Vec<u8>) -> Result<(FileType, FileIdentifier)> {
        let file_type_byte = bytes.remove(0);
        Ok((
            FileType::try_from(file_type_byte)
                .map_err(|err| error::Error::DecodeError(err.to_string()))?,
            bytes,
        ))
    }

    fn parse_file_info(bytes: Vec<u8>) -> Result<FileUpdateToken> {
        Ok(bytes)
    }
}

type FileIdentifier = Vec<u8>;
type FileName = String;
type FileUpdateToken = Vec<u8>;

#[derive(Debug)]
pub enum IndexInput {
    File(FileFullPath, FileType, FileIdentifier, FileUpdateToken),
    Directory(
        FileFullPath,
        FileIdentifier,
        FileUpdateToken,
        Vec<(FileName, FileType, FileIdentifier, FileUpdateToken)>,
    ),
    Empty(FileFullPath),
}

#[derive(Debug)]
pub enum EventType {
    FilePathCreate,
    FilePathUpdate,
    FilePathDelete,
}

#[derive(Debug)]
pub struct LocalFileSystemTrackerEvent {
    pub event_type: EventType,
    pub file_identifier: FileIdentifier,
    pub file_path: FileFullPath,
}

impl From<LocalFileSystemTrackerEvent> for file::FileEvent {
    fn from(value: LocalFileSystemTrackerEvent) -> Self {
        Self {
            event_type: match value.event_type {
                EventType::FilePathCreate => file::FileEventType::Created,
                EventType::FilePathUpdate => file::FileEventType::Changed,
                EventType::FilePathDelete => file::FileEventType::Deleted,
            },
            path: value.file_path,
        }
    }
}

pub type LocalFileSystemTrackerEventPack = Vec<LocalFileSystemTrackerEvent>;

pub type LocalFileSystemTrackerCallback =
    Box<dyn Fn(LocalFileSystemTrackerEventPack) + Send + Sync + 'static>;

pub struct LocalFileSystemTracker {
    db: TransactionDB,
    callback: LocalFileSystemTrackerCallback,
}

impl LocalFileSystemTracker {
    pub fn open_or_create_database(
        path: impl AsRef<Path>,
        cb: LocalFileSystemTrackerCallback,
    ) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let mut t_opts = rocksdb::TransactionDBOptions::default();
        t_opts.set_default_lock_timeout(5000);

        let db = TransactionDB::open(&opts, &t_opts, path)?;
        Ok(Self { db, callback: cb })
    }

    pub fn drop_database(path: impl AsRef<Path>) -> Result<()> {
        let opts = rocksdb::Options::default();
        TransactionDB::destroy(&opts, path)?;
        Ok(())
    }

    pub fn index(&self, input: IndexInput) -> Result<()> {
        // prev_file: The file associated with the url before the operation
        // next_file: The file associated with the url after the operation

        fn explore(
            operation: &Operation,
            file_path: FileFullPath,
            prev_file: Option<(FileType, FileIdentifier)>,
            next_file: Option<(FileType, FileIdentifier, FileUpdateToken)>,
            depth: bool,
            create: &mut Vec<(FileFullPath, FileType, FileIdentifier, FileUpdateToken)>,
            update: &mut Vec<(FileFullPath, FileType, FileIdentifier, FileUpdateToken)>,
            delete: &mut Vec<(FileFullPath, FileType, FileIdentifier)>,
        ) -> Result<()> {
            if let Some((prev_file_type, prev_file_id)) = prev_file {
                if let Some((next_file_type, next_file_id, next_file_update_token)) = next_file {
                    if next_file_id == prev_file_id {
                        update.push((
                            file_path,
                            next_file_type,
                            next_file_id,
                            next_file_update_token,
                        ));
                        return Ok(());
                    } else {
                        create.push((
                            file_path.clone(),
                            next_file_type,
                            next_file_id,
                            next_file_update_token,
                        ))
                    }
                }
                delete.push((file_path.clone(), prev_file_type, prev_file_id));

                if depth {
                    let children_prefix = if file_path.to_string() == "/" {
                        "/".to_owned()
                    } else {
                        format!("{file_path}/")
                    };
                    let children_paths =
                        operation.get_for_update_all_children_paths(children_prefix)?;

                    for (child_file_path, child_file_type, child_file_id) in children_paths {
                        delete.push((child_file_path, child_file_type, child_file_id))
                    }
                }

                return Ok(());
            } else {
                if let Some((next_file_type, next_file_id, next_file_update_token)) = next_file {
                    create.push((
                        file_path,
                        next_file_type,
                        next_file_id,
                        next_file_update_token,
                    ));
                    return Ok(());
                } else {
                    // do nothing
                    return Ok(());
                }
            }
        }

        fn execute(
            operation: &Operation,
            callback: &LocalFileSystemTrackerCallback,
            create: Vec<(FileFullPath, FileType, FileIdentifier, FileUpdateToken)>,
            update: Vec<(FileFullPath, FileType, FileIdentifier, FileUpdateToken)>,
            delete: Vec<(FileFullPath, FileType, FileIdentifier)>,
        ) -> Result<()> {
            let fetch_size = update.len() + create.len() + delete.len();
            let mut file_handle_list = Vec::with_capacity(fetch_size);
            file_handle_list.extend(create.iter().map(|c| c.2.clone()));
            file_handle_list.extend(update.iter().map(|c| c.2.clone()));
            file_handle_list.extend(delete.iter().map(|c| c.2.clone()));

            let fetched_infos =
                operation.get_for_update_file_info_batch(file_handle_list.clone())?;
            let (create_file_tokens, other_file_tokens) = fetched_infos.split_at(create.len());
            let (update_file_tokens, _delete_file_infos) = other_file_tokens.split_at(update.len());

            let mut create_events = LocalFileSystemTrackerEventPack::new();
            let mut delete_events = LocalFileSystemTrackerEventPack::new();
            let mut update_events = LocalFileSystemTrackerEventPack::new();

            for (file_path, _, file_id) in delete.into_iter() {
                operation.delete_file_path(file_path.clone())?;
                operation.delete_file_info(file_id.clone())?;

                delete_events.push(LocalFileSystemTrackerEvent {
                    event_type: EventType::FilePathDelete,
                    file_path,
                    file_identifier: file_id,
                })
            }

            for (i, (file_path, file_type, file_id, file_update_token)) in
                create.into_iter().enumerate()
            {
                let old_file_update_token = &create_file_tokens[i];

                operation.put_file_path(file_path.clone(), file_type, file_id.clone())?;

                if let Some(old_file_update_token) = old_file_update_token {
                    if old_file_update_token != &file_update_token {
                        // set new token
                        operation.put_file_info(file_id.clone(), file_update_token)?;
                    } else {
                        // token not change
                    }
                } else {
                    operation.put_file_info(file_id.clone(), file_update_token)?;
                }

                create_events.push(LocalFileSystemTrackerEvent {
                    event_type: EventType::FilePathCreate,
                    file_path,
                    file_identifier: file_id,
                })
            }

            for (i, (file_path, _, file_id, file_update_token)) in update.into_iter().enumerate() {
                let old_file_info = &update_file_tokens[i];

                if let Some(old_file_info) = old_file_info {
                    if old_file_info != &file_update_token {
                        operation.put_file_info(file_id.clone(), file_update_token)?;
                        update_events.push(LocalFileSystemTrackerEvent {
                            event_type: EventType::FilePathUpdate,
                            file_path,
                            file_identifier: file_id,
                        })
                    } else {
                    }
                } else {
                    unreachable!()
                }
            }

            delete_events.append(&mut create_events);
            delete_events.append(&mut update_events);
            callback(delete_events);

            Ok(())
        }

        let operation = Operation::new(self.db.transaction());

        let mut update = Vec::new();
        let mut create = Vec::new();
        let mut delete = Vec::new();

        match input {
            IndexInput::File(file_full_path, file_type, file_id, file_update_token) => {
                let prev_file = operation.get_for_update_file_path(file_full_path.clone())?;

                explore(
                    &operation,
                    file_full_path,
                    prev_file,
                    Some((file_type, file_id, file_update_token)),
                    true,
                    &mut create,
                    &mut update,
                    &mut delete,
                )?;
            }
            IndexInput::Empty(file_full_path) => {
                let prev_file = operation.get_for_update_file_path(file_full_path.clone())?;

                explore(
                    &operation,
                    file_full_path,
                    prev_file,
                    None,
                    true,
                    &mut create,
                    &mut update,
                    &mut delete,
                )?;
            }
            IndexInput::Directory(file_full_path, file_id, file_update_token, mut children) => {
                let prev_file = operation.get_for_update_file_path(file_full_path.clone())?;
                explore(
                    &operation,
                    file_full_path.clone(),
                    prev_file,
                    Some((FileType::Directory, file_id, file_update_token)),
                    false,
                    &mut create,
                    &mut update,
                    &mut delete,
                )?;

                let children_prefix = if file_full_path.to_string() == "/" {
                    "/".to_owned()
                } else {
                    format!("{file_full_path}/")
                };
                let prev_children = operation.get_for_update_directory(children_prefix.clone())?;

                let mut diff = Vec::new();

                for prev_child in prev_children {
                    let child_position = children.iter().position(|child| child.0 == prev_child.0);
                    if let Some(child_position) = child_position {
                        let child = children.remove(child_position);
                        diff.push((prev_child.0.clone(), Some(prev_child), Some(child)))
                    } else {
                        diff.push((prev_child.0.clone(), Some(prev_child), None))
                    }
                }

                for child in children {
                    diff.push((child.0.clone(), None, Some(child)))
                }

                for (name, prev_child, child) in diff {
                    let file_path = FileFullPath::parse(&format!("{children_prefix}{name}"));
                    let prev_file = prev_child.map(|prev_child| (prev_child.1, prev_child.2));
                    let next_file = child.map(|child| (child.1, child.2, child.3));
                    explore(
                        &operation,
                        file_path,
                        prev_file,
                        next_file,
                        true,
                        &mut create,
                        &mut update,
                        &mut delete,
                    )?;
                }
            }
        }

        execute(&operation, &self.callback, create, update, delete)?;
        operation.commit()?;
        Ok(())
    }

    pub fn dump(&self) -> Result<Vec<(Keys, Values)>> {
        let mut records = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for record in iter {
            let (key, value) = record?;
            let key = Keys::parse(&key)?;
            let value = Values::parse(&key, value.into_vec())?;
            records.push((key, value));
        }
        Ok(records)
    }
}

struct Operation<'db> {
    transaction: rocksdb::Transaction<'db, TransactionDB>,
}

impl<'db> Deref for Operation<'db> {
    type Target = rocksdb::Transaction<'db, TransactionDB>;
    fn deref(&self) -> &rocksdb::Transaction<'db, TransactionDB> {
        &self.transaction
    }
}

impl Operation<'_> {
    fn new<'db>(transaction: rocksdb::Transaction<'db, TransactionDB>) -> Operation<'db> {
        Operation::<'db> { transaction }
    }

    fn get_for_update_file_path_batch(
        &self,
        file_path: Vec<FileFullPath>,
    ) -> Result<Vec<Option<(FileType, FileIdentifier)>>> {
        let mut keys = Vec::with_capacity(file_path.len());
        for file_path in file_path {
            keys.push(Keys::FilePath(file_path).to_bytes()?)
        }

        let values = self.get_for_update_in_order(keys)?;

        let mut result = Vec::with_capacity(values.len());

        for value in values {
            result.push(if let Some(value) = value {
                Some(Values::parse_file_path(value)?)
            } else {
                None
            })
        }

        Ok(result)
    }

    fn get_for_update_file_path(
        &self,
        file_path: FileFullPath,
    ) -> Result<Option<(FileType, FileIdentifier)>> {
        let key = Keys::FilePath(file_path);
        let value = self.get_for_update(key.to_bytes()?, true)?;
        if let Some(value) = value {
            Ok(Some(Values::parse_file_path(value)?))
        } else {
            Ok(None)
        }
    }

    fn get_for_update_file_info_batch(
        &self,
        file_id: Vec<FileIdentifier>,
    ) -> Result<Vec<Option<FileUpdateToken>>> {
        let mut keys = Vec::with_capacity(file_id.len());
        for file_handle in file_id {
            keys.push(Keys::FileInfo(file_handle).to_bytes()?)
        }

        let values = self.get_for_update_in_order(keys)?;

        let mut result = Vec::with_capacity(values.len());

        for value in values {
            result.push(if let Some(value) = value {
                Some(Values::parse_file_info(value)?)
            } else {
                None
            })
        }

        Ok(result)
    }

    fn get_for_update_file_info(&self, file_id: FileIdentifier) -> Result<Option<FileUpdateToken>> {
        let key = Keys::FileInfo(file_id.clone());
        let value = self.get_for_update(key.to_bytes()?, true)?;
        if let Some(value) = value {
            Ok(Some(Values::parse_file_info(value)?))
        } else {
            Ok(None)
        }
    }

    fn get_for_update_all_children_paths(
        &self,
        children_prefix: String,
    ) -> Result<Vec<(FileFullPath, FileType, FileIdentifier)>> {
        let prefix = Keys::path_prefix(children_prefix);
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let iter = self.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        let mut result = Vec::new();
        for key in iter {
            let key = key?.0;
            if let Some(value) = self.get_for_update(&key, true)? {
                if let Keys::FilePath(file_path) = Keys::parse(&key)? {
                    let (file_type, file_id) = Values::parse_file_path(value)?;
                    result.push((file_path, file_type, file_id));
                }
            }
        }
        Ok(result)
    }

    fn get_for_update_directory(
        &self,
        directory_prefix: String,
    ) -> Result<Vec<(FileName, FileType, FileIdentifier)>> {
        let prefix: Vec<u8> = Keys::path_prefix(directory_prefix.clone());
        let mut upper_bound = prefix.clone();
        *upper_bound.last_mut().unwrap() += 1;
        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_upper_bound(upper_bound);
        let mut iter = self.iterator_opt(
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            read_opt,
        );

        let base_path_len = directory_prefix.len();

        let mut result = Vec::new();
        loop {
            let row = iter.next();
            if let Some(row) = row {
                let (key_bytes, _) = row?;
                if let Keys::FilePath(file_path) = Keys::parse(&key_bytes)? {
                    let file_path_str = file_path.to_string();
                    if file_path_str == directory_prefix {
                        continue;
                    }
                    let suffix = &file_path_str[base_path_len..];
                    let slash_position = suffix.chars().position(|a| a == '/');
                    if let Some(slash_position) = slash_position {
                        let mut next: Vec<u8> = Keys::path_prefix(
                            (&file_path_str[0..slash_position + base_path_len + 1]).to_owned(),
                        );
                        *next.last_mut().unwrap() += 1;
                        iter.set_mode(rocksdb::IteratorMode::From(&next, Direction::Forward))
                    } else {
                        if let Some(value) = self.get_for_update(&key_bytes, true)? {
                            let (file_type, file_id) = Values::parse_file_path(value)?;
                            result.push((suffix.to_owned(), file_type, file_id));
                        }
                    }
                }
            } else {
                break;
            }
        }

        Ok(result)
    }

    fn get_for_update_in_order(&self, mut keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        let mut pointers: Vec<usize> = (0..keys.len()).collect();
        pointers.sort_by(|a, b| keys[*a].cmp(&keys[*b]));

        let mut result = Vec::with_capacity(pointers.len());
        result.resize(pointers.len(), None);

        for pointer in pointers {
            let file_path_bytes = core::mem::take(&mut keys[pointer]);
            result[pointer] = self.get_for_update(file_path_bytes, true)?;
        }

        Ok(result)
    }

    fn put_file_info(
        &self,
        file_id: FileIdentifier,
        file_update_token: FileUpdateToken,
    ) -> Result<()> {
        let key = Keys::FileInfo(file_id).to_bytes()?;
        let value = Values::FileInfo(file_update_token).to_bytes()?;
        Ok(self.put(key, value)?)
    }

    fn put_file_path(
        &self,
        file_path: FileFullPath,
        file_type: FileType,
        file_id: FileIdentifier,
    ) -> Result<()> {
        let key = Keys::FilePath(file_path).to_bytes()?;
        let value = Values::FilePath(file_type, file_id).to_bytes()?;
        Ok(self.put(key, value)?)
    }

    fn delete_file_info(&self, file_id: FileIdentifier) -> Result<()> {
        let key = Keys::FileInfo(file_id).to_bytes()?;
        Ok(self.delete(key)?)
    }

    fn delete_file_path(&self, file_path: FileFullPath) -> Result<()> {
        let key = Keys::FilePath(file_path).to_bytes()?;
        Ok(self.delete(key)?)
    }

    fn commit(self) -> Result<()> {
        self.transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
