use std::{path::PathBuf, sync::Arc};

mod walker;
use walker::{*};

#[derive(Debug, Clone)]
pub struct LocalFileSystemConfiguration {
    pub root: PathBuf,
}

pub struct LocalFileSystem {
    configuration: Arc<LocalFileSystemConfiguration>,
    current_walker: Option<LocalFileSystemWalker>,
    tracker: Tracker
}

impl LocalFileSystem {
    pub fn new(configuration: LocalFileSystemConfiguration) -> Self {
        let configuration = Arc::new(configuration);

        Self {
            configuration: configuration.clone(),
            current_walker: None
        }
    }

    pub fn poll_ops(&mut self) {

      todo!()
    }
}
