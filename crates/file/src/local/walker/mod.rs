use std::{
    collections::LinkedList,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum WalkerItem {
    Pending,
    Reached {
        folder: PathBuf,
        metadata: std::fs::Metadata,
        children: Vec<(std::ffi::OsString, std::fs::Metadata)>,
    },
}

impl WalkerItem {
    pub fn folder(&self) -> Option<&PathBuf> {
        match self {
            WalkerItem::Pending => None,
            WalkerItem::Reached {
                folder,
                metadata: _,
                children: _,
            } => Some(folder),
        }
    }
}

pub struct WalkerIter<'a> {
    walker: &'a mut Walker,
}

impl<'a> WalkerIter<'a> {
    fn new(walker: &'a mut Walker) -> Self {
        Self { walker }
    }
}

impl<'a> Iterator for WalkerIter<'a> {
    type Item = Result<WalkerItem, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.walker.next() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl std::ops::Deref for WalkerIter<'_> {
    type Target = Walker;

    fn deref(&self) -> &Self::Target {
        self.walker
    }
}

impl std::ops::DerefMut for WalkerIter<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.walker
    }
}

pub struct Walker {
    root: PathBuf,
    current_stack: LinkedList<PathBuf>,
    current_position: usize,
}

impl Walker {
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

    pub fn iter(&mut self) -> WalkerIter {
        WalkerIter::new(self)
    }

    fn next(&mut self) -> Result<Option<WalkerItem>, std::io::Error> {
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
                Ok(Some(WalkerItem::Reached {
                    folder: base_path,
                    metadata: base_metadata,
                    children,
                }))
            } else {
                Ok(Some(WalkerItem::Pending))
            }
        } else {
            self.start_new_walking();
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Walker;

    #[test]
    fn test() {
        let mut walker = Walker::new(std::fs::canonicalize("..").unwrap());
        walker.iter().for_each(|r| {
            println!("{:?}", r.unwrap().folder().unwrap());
        });
    }
}
