use std::{
    ffi::OsStr,
    fs::Metadata,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
};

use utils::{Digestible, PathTools, Serialize, Xxhash};

use crate::{
    tracker::{FileMarker, FileTypeMarker, FileUpdateMarker},
    FileStats, FileType,
};

use super::Configuration;

pub struct Helper<'a> {
    configuration: &'a Configuration,
}

impl Helper<'_> {
    pub fn convert_path(&self, path: &Path) -> Option<String> {
        let path = path.to_string_lossy().to_string();

        let relative = PathTools::relative(&self.configuration.root.to_string_lossy(), &path);

        if relative.starts_with("..")
            || relative
                .chars()
                .any(|c| c == std::char::REPLACEMENT_CHARACTER)
        {
            None
        } else {
            Some(PathTools::resolve("/", &relative).to_string())
        }
    }

    pub fn make_marker(&self, metadata: &Metadata) -> FileMarker {
        if self.configuration.use_inode && metadata.is_dir() {
            (FileType::from(metadata.file_type()), metadata.ino())
                .to_bytes()
                .to_vec()
        } else {
            Default::default()
        }
    }

    pub fn make_update_marker(&self, metadata: &Metadata) -> FileUpdateMarker {
        let mut hash = Xxhash::new();
        if !metadata.is_dir() {
            metadata.ctime().digest(&mut hash);
            metadata.ctime_nsec().digest(&mut hash);
            metadata.mtime().digest(&mut hash);
            metadata.mtime_nsec().digest(&mut hash);
            metadata.size().digest(&mut hash);
        }
        self.make_type_marker(metadata).digest(&mut hash);
        hash.finish().to_vec()
    }

    pub fn make_type_marker(&self, metadata: &Metadata) -> FileTypeMarker {
        FileType::from(metadata.file_type()).to_bytes().into_vec()
    }

    pub fn convert_stats(&self, metadata: &Metadata) -> FileStats {
        FileStats {
            creation_time: metadata.ctime() as u64,
            last_write_time: metadata.mtime() as u64,
            size: metadata.size(),
            file_type: metadata.file_type().into(),
        }
    }

    pub fn convert_fspath(&self, path: &str) -> PathBuf {
        self.configuration
            .root
            .join(PathBuf::from(format!(".{}", path)))
    }

    pub fn convert_name(&self, file_name: &OsStr) -> String {
        file_name.to_string_lossy().to_string()
    }
}
