use std::fmt::Display;

use utils::{bytes_stringify, Deserialize, Digest, Digestible, Serialize};

use super::{FileMarker, FileTypeMarker, FileUpdateMarker};

#[derive(Debug, Clone, Default)]
pub struct Entity {
    pub marker: FileMarker,
    pub update_marker: FileUpdateMarker,
    pub type_marker: FileTypeMarker,
}

impl Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "`{}`({})({})",
            bytes_stringify(&self.marker),
            bytes_stringify(&self.update_marker),
            bytes_stringify(&self.type_marker)
        ))
    }
}

impl Serialize for Entity {
    fn serialize(&self, serializer: utils::Serializer) -> utils::Serializer {
        let serializer = self.marker.serialize(serializer);
        let serializer = self.update_marker.serialize(serializer);
        self.type_marker.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(
            self.marker.byte_size()?
                + self.update_marker.byte_size()?
                + self.type_marker.byte_size()?,
        )
    }
}

impl Deserialize for Entity {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (marker, bytes) = <_>::deserialize(bytes)?;
        let (update_marker, bytes) = <_>::deserialize(bytes)?;
        let (type_marker, bytes) = <_>::deserialize(bytes)?;

        Ok((
            Self {
                marker,
                update_marker,
                type_marker,
            },
            bytes,
        ))
    }
}

impl Digestible for Entity {
    fn digest(&self, data: &mut impl Digest) {
        self.marker.digest(data);
        self.update_marker.digest(data);
        self.type_marker.digest(data);
    }
}
