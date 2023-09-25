//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

mod error;

use db::DB;
use file::{FileFullPath, FileType};
use trie::{trie::TrieId, vindex::VIndex};

pub type Result<T> = std::result::Result<T, error::Error>;

type FileIdentifier = Vec<u8>;
type FileName = String;
type FileUpdateToken = Vec<u8>;

#[derive(Debug)]
pub enum IndexInput {
    File(FileFullPath, FileType, FileIdentifier, FileUpdateToken),
    Directory(
        FileFullPath,
        FileIdentifier,
        FileUpdateToken,
        Vec<(FileName, FileType, FileIdentifier, FileUpdateToken)>,
    ),
    Empty(FileFullPath),
}

pub struct LocalFileSystemTracker<DBImpl: DB> {
    vindex: VIndex<DBImpl>,
}

impl<DBImpl: DB> LocalFileSystemTracker<DBImpl> {
    fn get_file_status_on_vindex(
        &self,
        trie_id: TrieId,
    ) -> Result<(FileIdentifier, FileUpdateToken)> {
        todo!()
    }

    fn get_id_by_full_path(&self, full_path: FileFullPath) -> Result<Option<TrieId>> {
        todo!()
    }

    pub fn index(&self, input: IndexInput) -> Result<()> {
        let full_path = match input {
            IndexInput::File(file_path, _, _, _) => file_path,
            IndexInput::Directory(file_path, _, _, _) => file_path,
            IndexInput::Empty(file_path) => file_path,
        };

        let parent_full_path = full_path.dirname();
        if let Some(parent_id) = self.get_id_by_full_path(parent_full_path)? {
            let 
        } else {

        }



        dbg!(full_path);

        todo!()
    }
}
