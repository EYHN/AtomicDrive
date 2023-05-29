use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use file::{FileEventCallback, FileFullPath};

mod tracker;
mod walker;
mod watcher;

struct LocalFileSystemConfiguration {
    root: PathBuf,
    data_dir: PathBuf,
}

struct LocalFileSystem {
    configuration: LocalFileSystemConfiguration,
    tracker: Arc<Mutex<tracker::LocalFileSystemTracker>>,
    watcher: watcher::LocalFileSystemWatcher,
    walker: Mutex<walker::LocalFileSystemWalker>,
}

impl LocalFileSystem {
    pub fn new(
        event_callback: FileEventCallback,
        configuration: LocalFileSystemConfiguration,
    ) -> Self {
        let walker =
            Mutex::new(walker::LocalFileSystemWalker::new(configuration.root.clone()).unwrap());

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

        let watcher = watcher::LocalFileSystemWatcher::new(
            configuration.root.clone(),
            Box::new(move |paths| {
                let tracker = tracker_for_watcher.lock().unwrap();

                for path in paths.into_iter() {
                    tracker.index(file_path_to_index(&path).unwrap()).unwrap()
                }
            }),
        )
        .unwrap();

        Self {
            configuration,
            tracker,
            walker,
            watcher,
        }
    }

    pub fn walk(&self) {
        let tracker = self.tracker.lock().unwrap();
        let mut walker = self.walker.lock().unwrap();
        walker.start_new_walking().unwrap();
        for item in walker.iter() {
            if let Ok(item) = item {
                tracker
                    .index(tracker::IndexInput::new_directory(
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

fn file_path_to_index(path: &PathBuf) -> std::io::Result<tracker::IndexInput> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(tracker::IndexInput::new_file(
            FileFullPath::parse(&path.to_string_lossy().to_string()),
            metadata,
        )),
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(tracker::IndexInput::new_empty(FileFullPath::parse(
                    &path.to_string_lossy().to_string(),
                )))
            } else {
                Err(err)
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
                root: PathBuf::from("/Users/admin/Projects/AtomicDrive/crates/file_local/src"),
                data_dir: PathBuf::from("/Users/admin/Projects/AtomicDrive/cache"),
            },
        );
        fs.walk();
    }
}
