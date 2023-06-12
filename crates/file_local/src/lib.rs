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

fn convert_fspath_to_vpath(fs_root: &Path, path: &Path) -> Option<FileFullPath> {
    let path = path.to_string_lossy().to_string();

    let relative = PathTools::relative(&fs_root.to_string_lossy(), &path);

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

fn convert_vpath_to_fspath(fs_root: &Path, path: FileFullPath) -> PathBuf {
    fs_root.join(PathBuf::from(format!(".{}", path)))
}

#[derive(Debug, Clone)]
pub struct LocalFileSystemConfiguration {
    pub root: PathBuf,
    pub data_dir: PathBuf,
}

pub struct LocalFileSystem {
    configuration: LocalFileSystemConfiguration,
    tracker: Arc<Mutex<tracker::LocalFileSystemTracker>>,
    // watcher: Mutex<watcher::LocalFileSystemWatcher>,
    walker: Mutex<walker::LocalFileSystemWalker>,
}

impl LocalFileSystem {
    pub fn new(configuration: LocalFileSystemConfiguration) -> Self {
        let walker = Mutex::new(walker::LocalFileSystemWalker::new(
            configuration.root.clone(),
        ));

        let tracker = Arc::new(Mutex::new(
            tracker::LocalFileSystemTracker::open_or_create_database(
                configuration.data_dir.join("db"),
            )
            .unwrap(),
        ));

        Self {
            configuration,
            tracker,
            walker,
        }
    }

    pub fn watch(&self, cb: FileEventCallback) -> watcher::LocalFileSystemWatcher {
        let tracker_for_watcher = self.tracker.clone();
        let cfg_for_watcher = self.configuration.clone();

        let mut watcher = watcher::LocalFileSystemWatcher::new(
            self.configuration.root.clone(),
            Box::new(move |paths| {
                let tracker = tracker_for_watcher.lock();

                for path in paths.into_iter() {
                    let vpath = convert_fspath_to_vpath(&cfg_for_watcher.root, &path);
                    if let Some(vpath) = vpath {
                        match std::fs::metadata(path) {
                            Ok(metadata) => {
                                cb(tracker
                                    .index(tracker::IndexInput::File(
                                        vpath.clone(),
                                        metadata.file_type().into(),
                                        calc_file_identifier(&vpath, &metadata),
                                        calc_file_update_token(&metadata),
                                    ))
                                    .unwrap()
                                    .into_iter()
                                    .map(|e| e.into())
                                    .collect());
                            }
                            Err(err) => {
                                if err.kind() == std::io::ErrorKind::NotFound {
                                    cb(tracker
                                        .index(tracker::IndexInput::Empty(vpath))
                                        .unwrap()
                                        .into_iter()
                                        .map(|e| e.into())
                                        .collect())
                                } else {
                                    todo!()
                                }
                            }
                        }
                    }
                }
            }),
        );
        watcher.watch().unwrap();
        watcher
    }

    pub fn read_file(&self, path: FileFullPath) -> Vec<u8> {
        std::fs::read(convert_vpath_to_fspath(&self.configuration.root, path)).unwrap()
    }

    pub unsafe fn map_file(&self, path: FileFullPath) -> Mmap {
        let file =
            std::fs::File::open(convert_vpath_to_fspath(&self.configuration.root, path)).unwrap();

        Mmap::map(&file).unwrap()
    }

    pub fn stat_file(&self, path: FileFullPath) -> FileStats {
        let metadata =
            std::fs::metadata(convert_vpath_to_fspath(&self.configuration.root, path)).unwrap();
        FileStats {
            creation_time: 0,
            last_write_time: 0,
            size: metadata.size(),
            file_type: metadata.file_type().into(),
        }
    }

    pub fn quick_full_walk(&self) -> Vec<FileEvent> {
        let tracker = self.tracker.lock();
        let mut walker = self.walker.lock();
        let mut events: Vec<FileEvent> = vec![];
        walker.start_new_walking();
        for item in walker.iter() {
            if let Ok(item) = item {
                let item_path = convert_fspath_to_vpath(&self.configuration.root, &item.path);
                if let Some(item_path) = item_path {
                    events.extend(
                        tracker
                            .index(tracker::IndexInput::Directory(
                                item_path.clone(),
                                calc_file_identifier(&item_path, &item.metadata),
                                calc_file_update_token(&item.metadata),
                                item.children
                                    .into_iter()
                                    .filter_map(|(filename, metadata)| {
                                        let filename = filename.to_string_lossy();
                                        if filename
                                            .chars()
                                            .any(|c| c == std::char::REPLACEMENT_CHARACTER)
                                        {
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
        events
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
