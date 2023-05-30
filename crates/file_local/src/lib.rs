use std::{collections::hash_map::DefaultHasher, fs, path::PathBuf, sync::Arc};

use file::{FileEventCallback, FileFullPath};
use parking_lot::Mutex;

mod tracker;
mod walker;
mod watcher;

fn index_file(path: FileFullPath, metadata: std::fs::Metadata) -> tracker::IndexInput {
    tracker::IndexInput::File(
        path.clone(),
        metadata.file_type().into(),
        calc_file_identifier(&path, &metadata),
        calc_file_update_token(&metadata),
    )
}

fn index_empty(path: FileFullPath) -> tracker::IndexInput {
    tracker::IndexInput::Empty(path)
}

fn index_file_path(path: &PathBuf) -> std::io::Result<tracker::IndexInput> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(index_file(
            FileFullPath::parse(&path.to_string_lossy().to_string()),
            metadata,
        )),
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(index_empty(FileFullPath::parse(
                    &path.to_string_lossy().to_string(),
                )))
            } else {
                Err(err)
            }
        }
    }
}

fn index_directory(
    path: FileFullPath,
    metadata: std::fs::Metadata,
    children: impl Iterator<Item = (String, std::fs::Metadata)>,
) -> tracker::IndexInput {
    tracker::IndexInput::Directory(
        path.clone(),
        calc_file_identifier(&path, &metadata),
        calc_file_update_token(&metadata),
        children
            .into_iter()
            .map(|(file_name, metadata)| {
                (
                    file_name.clone(),
                    metadata.file_type().into(),
                    calc_file_identifier(&path.join(&file_name), &metadata),
                    calc_file_update_token(&metadata),
                )
            })
            .collect(),
    )
}

fn calc_file_identifier(path: &FileFullPath, metadata: &std::fs::Metadata) -> Vec<u8> {
    let mut hasher: DefaultHasher = DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
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

        let watcher = Mutex::new(watcher::LocalFileSystemWatcher::new(
            configuration.root.clone(),
            Box::new(move |paths| {
                let tracker = tracker_for_watcher.lock();

                for path in paths.into_iter() {
                    tracker.index(index_file_path(&path).unwrap()).unwrap()
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
                tracker
                    .index(index_directory(
                        FileFullPath::parse(&item.path.to_string_lossy()),
                        item.metadata,
                        item.children.into_iter().map(|(filename, metadata)| {
                            (filename.to_string_lossy().to_string(), metadata)
                        }),
                    ))
                    .unwrap();
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
