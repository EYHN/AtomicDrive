use utils::{Deserialize, Digest, Digestible, Serialize};

use super::{FileMarker, FileUpdateMarker};

#[derive(Debug, Clone, Default)]
pub struct Entity {
    pub marker: FileMarker,
    pub update_marker: FileUpdateMarker,
    pub is_directory: bool,
}

impl Serialize for Entity {
    fn serialize(&self, serializer: utils::Serializer) -> utils::Serializer {
        let serializer = self.marker.serialize(serializer);
        let serializer = self.update_marker.serialize(serializer);
        self.is_directory.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(
            self.marker.byte_size()?
                + self.update_marker.byte_size()?
                + self.is_directory.byte_size()?,
        )
    }
}

impl Deserialize for Entity {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (marker, bytes) = <_>::deserialize(bytes)?;
        let (update_marker, bytes) = <_>::deserialize(bytes)?;
        let (is_directory, bytes) = <_>::deserialize(bytes)?;

        Ok((
            Self {
                marker,
                update_marker,
                is_directory,
            },
            bytes,
        ))
    }
}

impl Digestible for Entity {
    fn digest(&self, data: &mut impl Digest) {
        self.marker.digest(data);
        self.update_marker.digest(data);
        self.is_directory.digest(data)
    }
}
