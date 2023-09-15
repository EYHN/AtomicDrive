use std::mem::MaybeUninit;

pub trait Serialize: Sized {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8>;

    /// alias for serialize
    fn write_to_bytes(&self, bytes: Vec<u8>) -> Vec<u8> {
        self.serialize(bytes)
    }
}

pub trait Deserialize: Sized {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String>;

    /// alias for deserialize
    fn parse_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Self::deserialize(bytes)
    }

    // fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
    //     panic!()
    // }

    // fn parse_from_buffer_with_u32_be_header(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
    //     let key_lens = u32::from_be_bytes(
    //         bytes[0..4]
    //             .try_into()
    //             .map_err(|_| format!("Failed to decode: {bytes:?}"))?,
    //     ) as usize;

    //     Self::parse_from_buffer(&bytes[4..], key_lens)
    // }
}

impl Serialize for String {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&(self.len() as u32).to_be_bytes());
        bytes.extend_from_slice(self.as_bytes());
        bytes
    }
}

impl Deserialize for String {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let size = u32::from_be_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| format!("Failed to decode: {bytes:?}"))?,
        ) as usize;

        if bytes.len() < size {
            return Err(format!("Failed to decode: {bytes:?}"));
        }
        let (bytes, rest) = bytes.split_at(size);
        let value = Self::from_utf8(bytes.to_vec())
            .map_err(|_| format!("Failed to decode content: {bytes:?}"))?;

        Ok((value, rest))
    }
}

impl Serialize for u8 {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }
}

impl Deserialize for u8 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..1]
                    .try_into()
                    .map_err(|_| format!("Failed to decode content: {bytes:?}"))?,
            ),
            &bytes[1..],
        ))
    }
}

impl Serialize for u32 {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }
}

impl Deserialize for u32 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..4]
                    .try_into()
                    .map_err(|_| format!("Failed to decode content: {bytes:?}"))?,
            ),
            &bytes[4..],
        ))
    }
}

impl Serialize for u64 {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }
}

impl Deserialize for u64 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..8]
                    .try_into()
                    .map_err(|_| format!("Failed to decode content: {bytes:?}"))?,
            ),
            &bytes[8..],
        ))
    }
}

impl<T: Serialize, const N: usize> Serialize for [T; N] {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        for elem in self {
            bytes = elem.serialize(bytes)
        }
        bytes
    }
}

impl<T: Deserialize, const N: usize> Deserialize for [T; N] {
    fn deserialize(mut bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let mut out: [MaybeUninit<T>; N] = MaybeUninit::uninit_array();
        dbg!(N);
        for elem in &mut out[..] {
            let (val, rest) = T::deserialize(bytes)?;
            bytes = rest;
            elem.write(val);
        }
        Ok((unsafe { MaybeUninit::array_assume_init(out) }, bytes))
    }
}

impl<A: Serialize, B: Serialize> Serialize for (A, B) {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8> {
        let bytes = self.0.serialize(bytes);
        self.1.serialize(bytes)
    }
}

impl<A: Deserialize, B: Deserialize> Deserialize for (A, B) {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let (a, rest) = A::deserialize(bytes)?;
        let (b, rest) = B::deserialize(rest)?;
        Ok(((a, b), rest))
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes = (self.len() as u32).serialize(bytes);
        for elem in self.iter() {
            bytes = elem.serialize(bytes);
        }
        bytes
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let mut rest = bytes;
        let (len, bytes) = u32::deserialize(rest)?;
        rest = bytes;

        let mut arr = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let (elem, bytes) = T::deserialize(rest)?;
            rest = bytes;
            arr.push(elem)
        }

        Ok((arr, rest))
    }
}
