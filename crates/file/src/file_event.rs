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
    event_type: FileEventType,
    path: FileFullPath,
}