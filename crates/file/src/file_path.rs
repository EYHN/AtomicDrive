use std::{fmt::Display, string::FromUtf8Error};

use utils::PathTools;

#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct FileFullPath {
    value: String,
}

impl FileFullPath {
    pub fn parse(path: &str) -> FileFullPath {
        FileFullPath {
            value: PathTools::resolve("/", path).to_string(),
        }
    }

    pub fn join(&self, path: &str) -> FileFullPath {
        FileFullPath {
            value: PathTools::join(&self.value, path).to_string(),
        }
    }

    pub fn dirname(&self) -> FileFullPath {
        FileFullPath {
            value: PathTools::dirname(&self.value).to_string(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<FileFullPath, FromUtf8Error> {
        // TODO: check path
        Ok(FileFullPath {
            value: String::from_utf8(bytes)?,
        })
    }
}

impl From<FileFullPath> for String {
    fn from(value: FileFullPath) -> Self {
        value.value
    }
}

impl AsRef<str> for FileFullPath {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl Display for FileFullPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
