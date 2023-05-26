mod error;

use std::{
    collections::LinkedList,
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Debug)]
pub struct LocalFileSystemWalkerItem {
    pub path: PathBuf,
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
        // if self.walker.current.is_none() {
        //     self.walker.start_new_walking();
        // }
        // if let Some(next) = self
        //     .walker
        //     .current
        //     .as_mut()
        //     .and_then(|c| c.next())
        //     .map(|r| {
        //         r.map_err(|e| error::Error::from(e)).and_then(|entry| {
        //             Ok((
        //                 entry.path().to_owned(),
        //                 entry.file_type(),
        //                 entry.metadata()?,
        //             ))
        //         })
        //     })
        // {
        //     self.walker.current_position += 1;
        //     Some(next)
        // } else {
        //     self.walker.current = None;
        //     self.walker.total = Some(self.walker.current_position);
        //     None
        // }
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
    current_stack: LinkedList<(PathBuf, std::fs::Metadata)>,
    total: Option<usize>,
    current_position: usize,
}

impl LocalFileSystemWalker {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let mut walker = Self {
            root: root.as_ref().to_owned(),
            current_stack: Default::default(),
            total: None,
            current_position: 0,
        };
        walker.start_new_walking()?;
        Ok(walker)
    }

    pub fn start_new_walking(&mut self) -> Result<()> {
        self.current_stack =
            LinkedList::from([(self.root.clone(), std::fs::metadata(&self.root)?)]);
        self.current_position = 0;
        Ok(())
    }

    pub fn iter(&mut self) -> LocalFileSystemWalkerIter {
        LocalFileSystemWalkerIter::new(self)
    }

    fn next(&mut self) -> Result<Option<LocalFileSystemWalkerItem>> {
        let folder = self.current_stack.pop_front();
        if let Some((folder_path, folder_metadata)) = folder {
            let read_dir = std::fs::read_dir(&folder_path)?;
            let mut children = vec![];
            for entry in read_dir.into_iter() {
                let child = entry?;
                let file_type = child.file_type()?;
                let file_name = child.file_name();
                let file_metadata = child.metadata()?;
                if !file_type.is_symlink() && file_type.is_dir() {
                    self.current_stack
                        .push_back((folder_path.join(&file_name), child.metadata()?))
                }
                children.push((file_name, file_metadata));
            }
            Ok(Some(LocalFileSystemWalkerItem {
                path: folder_path,
                metadata: folder_metadata,
                children,
            }))
        } else {
            self.start_new_walking()?;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::walker::LocalFileSystemWalker;

    #[test]
    fn test() {
        let mut walker = LocalFileSystemWalker::new(std::fs::canonicalize("..").unwrap()).unwrap();
        walker.iter().for_each(|r| {
            println!("{:?}", r.unwrap().path);
        });
        // walker.iter().for_each(|r| {
        //     println!("{:?}", r.unwrap().0);
        // });
    }
}
