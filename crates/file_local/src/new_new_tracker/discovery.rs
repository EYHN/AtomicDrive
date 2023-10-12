use file::FileFullPath;

use super::{FileMarker, FileName, FileUpdateMarker};

#[derive(Debug)]
pub struct Discovery {
    pub location: (FileFullPath, Option<FileMarker>),
    pub entities: Vec<(FileName, Option<FileMarker>, FileUpdateMarker)>,
}

impl Discovery {
    pub fn location_full_path(&self) -> &FileFullPath {
        &self.location.0
    }
    pub fn location_marker(&self) -> &Option<FileMarker> {
        &self.location.1
    }
}
