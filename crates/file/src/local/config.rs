use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Configuration {
    pub root: PathBuf,
    pub use_inode: bool,
}
