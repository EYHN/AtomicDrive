use std::{
    ffi::OsStr,
    fs::Metadata,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

mod walker;
use utils::{Digest, Digestible, PathTools, Serialize, Xxhash};
use walker::*;
mod error;
pub use error::{Error, Result};

use db::backend::memory::MemoryDB;
use tracker::{Discovery, DiscoveryEntity, FileMarker, FileTypeMarker, FileUpdateMarker, Tracker};

use crate::{FileStats, FileType};

#[derive(Debug, Clone)]
pub struct LocalFileSystemConfiguration {
    pub root: PathBuf,
    pub use_inode: bool,
}

pub struct LocalFileSystem<DBImpl: db::DB> {
    configuration: Arc<LocalFileSystemConfiguration>,
    current_walker: Option<Walker>,
    tracker: Tracker<DBImpl>,
}

impl<DBImpl: db::DB> LocalFileSystem<DBImpl> {
    pub fn init(configuration: LocalFileSystemConfiguration, db: DBImpl) -> Result<Self> {
        let configuration = Arc::new(configuration);

        Ok(Self {
            configuration: configuration.clone(),
            current_walker: None,
            tracker: Tracker::init(db)?,
        })
    }

    pub fn poll_ops(&mut self) -> Result<()> {
        if let WalkerItem::Reached {
            folder,
            metadata,
            children,
        } = self.poll_walker()?
        {
            let mut transaction = self.tracker.start_transaction()?;
            let ops = transaction.apply(Discovery {
                entities: children
                    .into_iter()
                    .map(|(name, metadata)| DiscoveryEntity {
                        name: self.convert_name(&name),
                        marker: self.make_marker(&metadata),
                        type_marker: self.make_type_marker(&metadata),
                        update_marker: self.make_update_marker(&metadata),
                    })
                    .collect(),
                location: (self.convert_path(&folder).unwrap(), Default::default()),
            })?;
            transaction.commit()?;
            if !ops.is_empty() {
                dbg!(ops);
            }
        }

        Ok(())
    }

    fn poll_walker(&mut self) -> Result<WalkerItem> {
        let walker = if let Some(ref mut walker) = &mut self.current_walker {
            walker
        } else {
            self.current_walker = Some(Walker::new(&self.configuration.root));
            self.current_walker.as_mut().unwrap()
        };

        if let Some(next) = walker.iter().next() {
            Ok(next?)
        } else {
            Ok(WalkerItem::Pending)
        }
    }

    fn convert_path(&self, path: &Path) -> Option<String> {
        let path = path.to_string_lossy().to_string();

        let relative = PathTools::relative(&self.configuration.root.to_string_lossy(), &path);

        if relative.starts_with("..")
            || relative
                .chars()
                .any(|c| c == std::char::REPLACEMENT_CHARACTER)
        {
            None
        } else {
            Some(PathTools::resolve("/", &relative).to_string())
        }
    }

    fn make_marker(&self, metadata: &Metadata) -> FileMarker {
        if self.configuration.use_inode && metadata.is_dir() {
            (FileType::from(metadata.file_type()), metadata.ino())
                .to_bytes()
                .to_vec()
        } else {
            Default::default()
        }
    }

    fn make_update_marker(&self, metadata: &Metadata) -> FileUpdateMarker {
        let mut hash = Xxhash::new();
        if !metadata.is_dir() {
            metadata.ctime().digest(&mut hash);
            metadata.ctime_nsec().digest(&mut hash);
            metadata.mtime().digest(&mut hash);
            metadata.mtime_nsec().digest(&mut hash);
            metadata.size().digest(&mut hash);
        }
        self.make_type_marker(metadata).digest(&mut hash);
        hash.finish().to_vec()
    }

    fn make_type_marker(&self, metadata: &Metadata) -> FileTypeMarker {
        FileType::from(metadata.file_type()).to_bytes().into_vec()
    }

    fn convert_stats(&self, metadata: &Metadata) -> FileStats {
        FileStats {
            creation_time: metadata.ctime() as u64,
            last_write_time: metadata.mtime() as u64,
            size: metadata.size(),
            file_type: metadata.file_type().into(),
        }
    }

    fn convert_fspath(&self, path: &str) -> PathBuf {
        self.configuration
            .root
            .join(PathBuf::from(format!(".{}", path)))
    }

    fn convert_name(&self, file_name: &OsStr) -> String {
        file_name.to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::Thread, time::Duration};

    use db::backend::memory::MemoryDB;

    use super::{LocalFileSystem, LocalFileSystemConfiguration};

    #[test]
    fn test() {
        let mut fs = LocalFileSystem::init(
            LocalFileSystemConfiguration {
                root: "/Users/admin/Desktop/AtomicDrive/test_dir".into(),
                use_inode: true,
            },
            MemoryDB::default(),
        )
        .unwrap();

        loop {
            fs.poll_ops();
            std::thread::sleep(Duration::from_secs(1))
        }
    }
}
