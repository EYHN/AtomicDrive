use super::{FileMarker, FileName, FileUpdateMarker, FileTypeMarker};

#[derive(Debug, Clone)]
pub struct DiscoveryEntity {
    pub name: FileName,
    pub marker: FileMarker,
    pub type_marker: FileTypeMarker,
    pub update_marker: FileUpdateMarker,
}

#[derive(Debug)]
pub struct Discovery {
    pub location: (String, FileMarker),
    pub entities: Vec<DiscoveryEntity>,
}

impl Discovery {
    pub fn location_full_path(&self) -> &str {
        &self.location.0
    }
    pub fn location_marker(&self) -> &FileMarker {
        &self.location.1
    }
}
