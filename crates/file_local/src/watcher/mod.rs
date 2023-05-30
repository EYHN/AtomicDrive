use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use notify::Watcher;

mod error;

pub type Result<T> = std::result::Result<T, error::Error>;

pub struct LocalFileSystemWatcher {
    base_path: PathBuf,
    watcher: Option<Box<dyn notify::Watcher>>,
    cb: Arc<dyn Fn(Vec<PathBuf>) + Send + Sync + 'static>,
}

impl LocalFileSystemWatcher {
    pub fn new(base_path: PathBuf, cb: Box<dyn Fn(Vec<PathBuf>) + Send + Sync + 'static>) -> Self {
        Self {
            base_path,
            watcher: None,
            cb: Arc::from(cb),
        }
    }

    pub fn watch(&mut self) -> Result<()> {
        self.unwatch();
        let cb = self.cb.clone();
        let mut watcher = Box::new(notify::recommended_watcher(
            move |res: notify::Result<notify::Event>| match res {
                Ok(event) => cb(event.paths),
                Err(e) => println!("watch error: {:?}", e),
            },
        )?);
        watcher.watch(&self.base_path, notify::RecursiveMode::Recursive)?;
        self.watcher = Some(watcher);
        Ok(())
    }

    pub fn unwatch(&mut self) -> Result<()> {
        if let Some(ref mut watcher) = self.watcher {
            watcher.unwatch(&self.base_path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, thread::sleep, time::Duration};

    use notify::{RecursiveMode, Watcher};

    #[test]
    fn test() {
        let mut watcher = notify::recommended_watcher(|res| match res {
            Ok(event) => println!("event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        })
        .unwrap();

        watcher
            .watch(Path::new("."), RecursiveMode::Recursive)
            .unwrap();

        loop {
            sleep(Duration::from_secs(1))
        }
    }
}
