use utils::Serialize;

#[repr(u8)]
#[derive(
    Debug, Copy, Clone, Hash, PartialEq, Eq, num_enum::IntoPrimitive, num_enum::TryFromPrimitive,
)]
pub enum FileType {
    File = b'f',
    Directory = b'd',
    SymbolicLink = b's',
}

impl From<std::fs::FileType> for FileType {
    fn from(value: std::fs::FileType) -> Self {
        if value.is_symlink() {
            Self::SymbolicLink
        } else if value.is_dir() {
            Self::Directory
        } else {
            Self::File
        }
    }
}

impl Default for FileType {
    fn default() -> Self {
        Self::File
    }
}

impl Serialize for FileType {
    fn serialize(&self, serializer: utils::Serializer) -> utils::Serializer {
        u8::from(*self).serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(1)
    }
}
