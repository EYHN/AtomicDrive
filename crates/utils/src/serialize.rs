pub trait Serialize: Sized {
    fn write_to_bytes(&self, bytes: Vec<u8>) -> Vec<u8>;

    fn write_to_bytes_with_u32_be_header(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        let start = bytes.len();
        bytes = self.write_to_bytes(bytes);
        let size = ((bytes.len() - start) as u32).to_be_bytes();
        bytes[start - 4] = size[0];
        bytes[start - 3] = size[1];
        bytes[start - 2] = size[2];
        bytes[start - 1] = size[3];

        bytes
    }
}

pub trait Deserialize: Sized {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>;

    fn parse_from_buffer(bytes: &[u8], size: usize) -> Result<(Self, &[u8]), String> {
        if bytes.len() < size {
            return Err(format!("Failed to decode: {bytes:?}"));
        }
        let (value, rest) = bytes.split_at(size);
        let value = Self::from_bytes(value)?;

        Ok((value, rest))
    }

    fn parse_from_buffer_with_u32_be_header(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let key_lens = u32::from_be_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| format!("Failed to decode: {bytes:?}"))?,
        ) as usize;

        Self::parse_from_buffer(&bytes[4..], key_lens)
    }
}

impl Serialize for String {
    fn write_to_bytes(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(self.as_bytes());
        bytes
    }
}

impl Deserialize for String {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        Self::from_utf8(bytes.to_vec()).map_err(|_| format!("Failed to decode content: {bytes:?}"))
    }
}

impl Serialize for u64 {
    fn write_to_bytes(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }
}

impl Deserialize for u64 {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self::from_be_bytes(bytes.try_into().map_err(|_| {
            format!("Failed to decode content: {bytes:?}")
        })?))
    }
}
