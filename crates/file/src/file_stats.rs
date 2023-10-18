use crate::FileType;

#[derive(Debug, Clone, Hash, PartialEq, Default)]
pub struct FileStats {
    pub creation_time: u64,
    pub last_write_time: u64,
    /// the size of the file, in bytes.
    pub size: u64,
    pub file_type: FileType,
}
