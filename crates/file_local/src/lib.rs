use std::{
    collections::hash_map::DefaultHasher,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use file::{FileEventCallback, FileFullPath};
use parking_lot::Mutex;
use utils::PathTools;

mod tracker;
mod walker;
mod watcher;

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
struct LocalFileSystemConfiguration {
    root: PathBuf,
    data_dir: PathBuf,
}

struct LocalFileSystem {
    configuration: LocalFileSystemConfiguration,
    tracker: Arc<Mutex<tracker::LocalFileSystemTracker>>,
    watcher: Mutex<watcher::LocalFileSystemWatcher>,
    walker: Mutex<walker::LocalFileSystemWalker>,
}

impl LocalFileSystem {
    pub fn new(
        event_callback: FileEventCallback,
        configuration: LocalFileSystemConfiguration,
    ) -> Self {
        let walker = Mutex::new(walker::LocalFileSystemWalker::new(
            configuration.root.clone(),
        ));

        let tracker = Arc::new(Mutex::new(
            tracker::LocalFileSystemTracker::open_or_create_database(
                configuration.data_dir.join("db"),
                Box::new(move |events| {
                    event_callback(events.into_iter().map(|e| e.into()).collect())
                }),
            )
            .unwrap(),
        ));

        let tracker_for_watcher = tracker.clone();
        let cfg_for_watcher = configuration.clone();

        let watcher = Mutex::new(watcher::LocalFileSystemWatcher::new(
            configuration.root.clone(),
            Box::new(move |paths| {
                let tracker = tracker_for_watcher.lock();

                for path in paths.into_iter() {
                    let vpath =
                        convert_fspath_to_vpath(&cfg_for_watcher.root, &path);
                    if let Some(vpath) = vpath {
                        match fs::metadata(path) {
                            Ok(metadata) => {
                                tracker
                                    .index(tracker::IndexInput::File(
                                        vpath.clone(),
                                        metadata.file_type().into(),
                                        calc_file_identifier(&vpath, &metadata),
                                        calc_file_update_token(&metadata),
                                    ))
                                    .unwrap();
                            }
                            Err(err) => {
                                if err.kind() == std::io::ErrorKind::NotFound {
                                    tracker
                                        .index(tracker::IndexInput::Empty(vpath))
                                        .unwrap()
                                } else {
                                    todo!()
                                }
                            }
                        }
                    }
                }
            }),
        ));

        Self {
            configuration,
            tracker,
            walker,
            watcher,
        }
    }

    fn watch(&self) {
        let mut watcher = self.watcher.lock();
        watcher.watch();
    }

    fn unwatch(&self) {
        let mut watcher = self.watcher.lock();
        watcher.unwatch();
    }

    pub fn walk(&self) {
        let tracker = self.tracker.lock();
        let mut walker = self.walker.lock();
        walker.start_new_walking();
        for item in walker.iter() {
            if let Ok(item) = item {
                let item_path =
                    convert_fspath_to_vpath(&self.configuration.root, &item.path);
                if let Some(item_path) = item_path {
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
                        .unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::LocalFileSystem;

    #[test]
    fn test() {
        let fs = LocalFileSystem::new(
            Box::new(|ev| {
                dbg!(ev);
            }),
            crate::LocalFileSystemConfiguration {
                root: PathBuf::from("/Users/admin/Projects/AtomicDrive/test_dir"),
                data_dir: PathBuf::from("/Users/admin/Projects/AtomicDrive/cache"),
            },
        );

        fs.walk();
    }
}
