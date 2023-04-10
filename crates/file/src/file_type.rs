#[repr(u8)]
#[derive(
    Debug,
    Copy,
    Clone,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    num_enum::IntoPrimitive,
    num_enum::TryFromPrimitive,
)]
pub enum FileType {
    File = 0,
    Directory = 1,
    SymbolicLink = 2,
}

impl Default for FileType {
    fn default() -> Self {
        Self::File
    }
}
