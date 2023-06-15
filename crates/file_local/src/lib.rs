use std::{
    collections::hash_map::DefaultHasher,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use file::{FileEvent, FileEventCallback, FileFullPath, FileStats};
use memmap2::Mmap;
use parking_lot::Mutex;
use utils::PathTools;

mod tracker;
mod walker;
pub mod watcher;

fn calc_file_identifier(vpath: &FileFullPath, metadata: &std::fs::Metadata) -> Vec<u8> {
    let mut hasher: DefaultHasher = DefaultHasher::new();
    std::hash::Hash::hash(&vpath, &mut hasher);
    std::hash::Hash::hash(&metadata.file_type(), &mut hasher);
    std::hash::Hasher::finish(&hasher).to_be_bytes().to_vec()
}

fn calc_file_update_token(metadata: &std::fs::Metadata) -> Vec<u8> {
    let mut hasher = DefaultHasher::new();
    std::hash::Hash::hash(&metadata.len(), &mut hasher);
    if let Ok(time) = metadata.created() {
        std::hash::Hash::hash(&time, &mut hasher);
    }
    if let Ok(time) = metadata.modified() {
        std::hash::Hash::hash(&time, &mut hasher);
    }
    std::hash::Hasher::finish(&hasher).to_be_bytes().to_vec()
}

#[derive(Debug, Clone)]
pub struct LocalFileSystemConfiguration {
    pub root: PathBuf,
    pub data_dir: PathBuf,
}

pub struct LocalFileSystem {
    configuration: Arc<LocalFileSystemConfiguration>,
    operator: Arc<LocalFileSystemOperator>,
    tracker: Arc<Mutex<tracker::LocalFileSystemTracker>>,
}

impl LocalFileSystem {
    pub fn new(configuration: LocalFileSystemConfiguration) -> Self {
        let tracker = Arc::new(Mutex::new(
            tracker::LocalFileSystemTracker::open_or_create_database(
                configuration.data_dir.join("db"),
            )
            .unwrap(),
        ));

        let configuration = Arc::new(configuration);

        Self {
            configuration: configuration.clone(),
            operator: Arc::new(LocalFileSystemOperator {
                configuration: configuration,
                tracker: tracker.clone(),
            }),
            tracker,
        }
    }

    pub fn watch(&self, cb: FileEventCallback) -> watcher::LocalFileSystemWatcher {
        let operator = self.operator.clone();

        let mut watcher = watcher::LocalFileSystemWatcher::new(
            self.configuration.root.clone(),
            Box::new(move |paths| cb(operator.update(paths.iter()))),
        );
        watcher.watch().unwrap();
        watcher
    }

    pub fn read_file(&self, path: FileFullPath) -> Vec<u8> {
        std::fs::read(self.operator.convert_to_fspath(path)).unwrap()
    }

    pub unsafe fn map_file(&self, path: FileFullPath) -> Mmap {
        let file = std::fs::File::open(self.operator.convert_to_fspath(path)).unwrap();

        Mmap::map(&file).unwrap()
    }

    pub fn stat_file(&self, path: FileFullPath) -> FileStats {
        self.operator.stat(path)
    }

    pub fn quick_full_walk(&self) -> Vec<FileEvent> {
        self.operator.update([&self.configuration.root].iter())
    }
}

struct LocalFileSystemOperator {
    configuration: Arc<LocalFileSystemConfiguration>,
    tracker: Arc<Mutex<tracker::LocalFileSystemTracker>>,
}

impl LocalFileSystemOperator {
    fn update(&self, paths: impl Iterator<Item = impl AsRef<Path>>) -> Vec<FileEvent> {
        let tracker = self.tracker.lock();
        let mut events: Vec<FileEvent> = vec![];
        for path in paths {
            let mut walker = walker::LocalFileSystemWalker::new(path);
            walker.start_new_walking();
            for item in walker.iter() {
                if let Ok(item) = item {
                    let item_path = self.convert_vpath(&item.path());
                    if let Some(item_path) = item_path {
                        match item {
                            walker::LocalFileSystemWalkerItem::File(_, metadata) => {
                                events.extend(
                                    tracker
                                        .index(tracker::IndexInput::File(
                                            item_path.clone(),
                                            metadata.file_type().into(),
                                            calc_file_identifier(&item_path, &metadata),
                                            calc_file_update_token(&metadata),
                                        ))
                                        .unwrap()
                                        .into_iter()
                                        .map(|e| e.into()),
                                );
                            }
                            walker::LocalFileSystemWalkerItem::Directory(_, metadata, children) => {
                                events.extend(
                                    tracker
                                        .index(tracker::IndexInput::Directory(
                                            item_path.clone(),
                                            calc_file_identifier(&item_path, &metadata),
                                            calc_file_update_token(&metadata),
                                            children
                                                .into_iter()
                                                .filter_map(|(filename, metadata)| {
                                                    let filename = filename.to_string_lossy();
                                                    if filename.chars().any(|c| {
                                                        c == std::char::REPLACEMENT_CHARACTER
                                                    }) {
                                                        None
                                                    } else {
                                                        Some((filename.to_string(), metadata))
                                                    }
                                                })
                                                .map(|(file_name, metadata)| {
                                                    (
                                                        file_name.clone(),
                                                        metadata.file_type().into(),
                                                        calc_file_identifier(
                                                            &item_path.join(&file_name),
                                                            &metadata,
                                                        ),
                                                        calc_file_update_token(&metadata),
                                                    )
                                                })
                                                .collect(),
                                        ))
                                        .unwrap()
                                        .into_iter()
                                        .map(|e| e.into()),
                                );
                            }
                        }
                    }
                }
            }
        }
        events
    }

    fn write_file(&self, path: FileFullPath, stats: FileStats, content: &[u8]) {
        let tracker = self.tracker.lock();

        if PathTools::dirname(path.as_ref()) != "/" {
            let dir_metadata = std::fs::symlink_metadata(
                self.convert_to_fspath(FileFullPath::parse(PathTools::dirname(path.as_ref()))),
            );

            match dir_metadata {
                Ok(metadata) => {
                    if !metadata.is_dir() {
                        todo!()
                    }
                }
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                    } else {
                        panic!("{:?}", err)
                    }
                }
            }
        }

        // for  in PathTools::dirname(path)
        // .to_owned()
        // .split(PathTools::DIRECTORY_SEPARATOR_CHAR) {

        // }
    }

    fn stat(&self, path: FileFullPath) -> FileStats {
        let metadata = std::fs::symlink_metadata(self.convert_to_fspath(path)).unwrap();
        FileStats {
            creation_time: 0,
            last_write_time: 0,
            size: metadata.size(),
            file_type: metadata.file_type().into(),
        }
    }

    fn convert_vpath(&self, path: &Path) -> Option<FileFullPath> {
        let path = path.to_string_lossy().to_string();

        let relative = PathTools::relative(&self.configuration.root.to_string_lossy(), &path);

        if relative.starts_with("..")
            || relative
                .chars()
                .any(|c| c == std::char::REPLACEMENT_CHARACTER)
        {
            None
        } else {
            Some(FileFullPath::parse(&relative))
        }
    }

    fn convert_to_fspath(&self, path: FileFullPath) -> PathBuf {
        self.configuration
            .root
            .join(PathBuf::from(format!(".{}", path)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::LocalFileSystem;

    #[test]
    fn test() {
        let fs = LocalFileSystem::new(crate::LocalFileSystemConfiguration {
            root: PathBuf::from("/Users/admin/Projects/AtomicDrive/test_dir"),
            data_dir: PathBuf::from("/Users/admin/Projects/AtomicDrive/cache"),
        });

        dbg!(fs.quick_full_walk());
    }
}
