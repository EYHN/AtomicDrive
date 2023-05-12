use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use notify::Watcher;

mod error;

pub type Result<T> = std::result::Result<T, error::Error>;

pub struct LocalFileSystemWatcher {
    base_path: PathBuf,
    watcher: Box<dyn notify::Watcher>,
}

impl LocalFileSystemWatcher {
    pub fn new(
        base_path: PathBuf,
        cb: Box<dyn Fn(Vec<PathBuf>) + Send + Sync + 'static>,
    ) -> Result<Self> {
        let watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => cb(event.paths),
                Err(e) => println!("watch error: {:?}", e),
            })?;

        Ok(Self {
            base_path,
            watcher: Box::new(watcher),
        })
    }

    pub fn watch(&mut self) -> Result<()> {
        self.watcher
            .watch(Path::new("."), notify::RecursiveMode::Recursive)?;
        Ok(())
    }

    pub fn unwatch(&mut self) -> Result<()> {
        self.watcher.unwatch(Path::new("."))?;
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
