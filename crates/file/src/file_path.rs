use std::string::FromUtf8Error;

use utils::PathTools;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct FileFullPath {
    value: String,
}

impl FileFullPath {
    pub fn parse(path: &str) -> FileFullPath {
        FileFullPath {
            value: PathTools::resolve("/", path).to_string(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.value.as_bytes()
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
