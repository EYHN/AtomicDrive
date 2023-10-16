mod error;

use std::{
    collections::LinkedList,
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Debug)]
pub struct LocalFileSystemWalkerItem {
    pub dir: PathBuf,
    pub metadata: std::fs::Metadata,
    pub children: Vec<(std::ffi::OsString, std::fs::Metadata)>,
}

pub struct LocalFileSystemWalkerIter<'a> {
    walker: &'a mut LocalFileSystemWalker,
}

impl<'a> LocalFileSystemWalkerIter<'a> {
    fn new(walker: &'a mut LocalFileSystemWalker) -> Self {
        Self { walker }
    }
}

impl<'a> Iterator for LocalFileSystemWalkerIter<'a> {
    type Item = Result<LocalFileSystemWalkerItem>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.walker.next() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl std::ops::Deref for LocalFileSystemWalkerIter<'_> {
    type Target = LocalFileSystemWalker;

    fn deref(&self) -> &Self::Target {
        self.walker
    }
}

impl std::ops::DerefMut for LocalFileSystemWalkerIter<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.walker
    }
}

pub struct LocalFileSystemWalker {
    root: PathBuf,
    current_stack: LinkedList<PathBuf>,
    current_position: usize,
}

impl LocalFileSystemWalker {
    pub fn new(root: impl AsRef<Path>) -> Self {
        let mut walker = Self {
            root: root.as_ref().to_owned(),
            current_stack: Default::default(),
            current_position: 0,
        };
        walker.start_new_walking();
        walker
    }

    pub fn start_new_walking(&mut self) {
        self.current_stack = LinkedList::from([(self.root.clone())]);
        self.current_position = 0
    }

    pub fn iter(&mut self) -> LocalFileSystemWalkerIter {
        LocalFileSystemWalkerIter::new(self)
    }

    fn next(&mut self) -> Result<Option<LocalFileSystemWalkerItem>> {
        loop {
            let base = self.current_stack.pop_front();
            if let Some(base_path) = base {
                let base_metadata = std::fs::symlink_metadata(&base_path)?;
                if base_metadata.is_dir() {
                    let read_dir = std::fs::read_dir(&base_path)?;
                    let mut children = vec![];
                    for entry in read_dir.into_iter() {
                        let child = entry?;
                        let file_type = child.file_type()?;
                        let file_name = child.file_name();
                        let file_metadata = child.metadata()?;
                        if file_type.is_dir() {
                            self.current_stack.push_back(base_path.join(&file_name))
                        }
                        children.push((file_name, file_metadata));
                    }
                    return Ok(Some(LocalFileSystemWalkerItem {
                        dir: base_path,
                        metadata: base_metadata,
                        children,
                    }));
                } else {
                    continue;
                }
            } else {
                self.start_new_walking();
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LocalFileSystemWalker;

    #[test]
    fn test() {
        let mut walker = LocalFileSystemWalker::new(std::fs::canonicalize("..").unwrap());
        walker.iter().for_each(|r| {
            println!("{:?}", r.unwrap().dir);
        });
    }
}
