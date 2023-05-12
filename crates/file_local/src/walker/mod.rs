mod error;

use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, error::Error>;

pub struct LocalFileSystemWalkerIter {
    iter: walkdir::IntoIter,
}

impl LocalFileSystemWalkerIter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            iter: walkdir::WalkDir::new(root).follow_links(true).into_iter(),
        }
    }
}

impl Iterator for LocalFileSystemWalkerIter {
    type Item = Result<(PathBuf, std::fs::FileType, std::fs::Metadata)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| {
            r.map_err(|e| error::Error::from(e)).and_then(|entry| {
                Ok((
                    entry.path().to_owned(),
                    entry.file_type(),
                    entry.metadata()?,
                ))
            })
        })
    }
}

pub struct LocalFileSystemWalker {
    root: PathBuf,
    current: Option<LocalFileSystemWalkerIter>,
    total: Option<usize>,
    current_position: usize,
}

impl LocalFileSystemWalker {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_owned(),
            current: None,
            total: None,
            current_position: 0,
        }
    }

    pub fn start_new_walking(&mut self) -> &mut LocalFileSystemWalkerIter {
        self.current = Some(LocalFileSystemWalkerIter::new(&self.root));
        self.current_position = 0;
        self.current.as_mut().unwrap()
    }

    pub fn next(&mut self) {
        if let Some(current) = &mut self.current {
            self.current_position += 1;
            if self.current_position >= current.iter.depth() {
                self.current = None;
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        use walkdir::WalkDir;

        struct WalkCache {
            dir: String,
        }

        for entry in WalkDir::new(".").follow_links(true) {
            println!("{}", entry.unwrap().path().display());
        }
    }
}
