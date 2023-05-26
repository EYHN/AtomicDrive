use crate::FileFullPath;

#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub enum FileEventType {
    /// Event when file is created.
    Created,

    /// Event when file is deleted.
    Deleted,

    /// Event when file is changed.
    Changed,
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct FileEvent {
    pub event_type: FileEventType,
    pub path: FileFullPath,
}

pub type FileEventCallback = Box<dyn Fn(Vec<FileEvent>) + Sync + Send + 'static>;
