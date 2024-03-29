use std::mem::{size_of, MaybeUninit};

use smallvec::SmallVec;

#[derive(Debug)]
pub struct Serializer {
    bytes: SmallVec<[u8; 16]>,
}

impl Serializer {
    fn new() -> Self {
        Self {
            bytes: Default::default(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    pub fn reserve(&mut self, size: usize) {
        self.bytes.reserve(size)
    }

    pub fn push(&mut self, byte: u8) {
        self.bytes.push(byte)
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.bytes.extend_from_slice(slice)
    }

    pub fn finish(self) -> SmallVec<[u8; 16]> {
        self.bytes
    }
}

impl From<Vec<u8>> for Serializer {
    fn from(value: Vec<u8>) -> Self {
        Self {
            bytes: value.into(),
        }
    }
}

impl Extend<u8> for Serializer {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        self.bytes.extend(iter)
    }
}

pub trait Serialize {
    fn serialize(&self, serializer: Serializer) -> Serializer;

    fn to_bytes(&self) -> SmallVec<[u8; 16]> {
        let mut serializer = Serializer::new();
        if let Some(size) = self.byte_size() {
            serializer.reserve(size)
        } else {
            panic!()
        }
        self.serialize(Serializer::new()).finish()
    }

    fn byte_size(&self) -> Option<usize>;
}

pub trait Deserialize: Sized {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self::deserialize(bytes)?.0)
    }
}

impl Serialize for String {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&(self.len() as u32).to_be_bytes());
        bytes.extend_from_slice(self.as_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.as_bytes().len() + size_of::<u32>())
    }
}

impl Deserialize for String {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let size = u32::from_be_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| format!("Failed to decode string: {bytes:?}"))?,
        ) as usize;

        let bytes = &bytes[4..];

        if bytes.len() < size {
            return Err(format!("Failed to decode string: {bytes:?}"));
        }
        let (bytes, rest) = bytes.split_at(size);
        let value = Self::from_utf8(bytes.to_vec())
            .map_err(|_| format!("Failed to decode string: {bytes:?}"))?;

        Ok((value, rest))
    }
}

impl Serialize for u8 {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(size_of::<u8>())
    }
}

impl Deserialize for u8 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..1]
                    .try_into()
                    .map_err(|_| format!("Failed to decode u8: {bytes:?}"))?,
            ),
            &bytes[1..],
        ))
    }
}

impl Serialize for u32 {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(size_of::<u32>())
    }
}

impl Deserialize for u32 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..4]
                    .try_into()
                    .map_err(|_| format!("Failed to decode u32: {bytes:?}"))?,
            ),
            &bytes[4..],
        ))
    }
}

impl Serialize for u64 {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(size_of::<u64>())
    }
}

impl Deserialize for u64 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..8]
                    .try_into()
                    .map_err(|_| format!("Failed to decode u64: {bytes:?}"))?,
            ),
            &bytes[8..],
        ))
    }
}

impl Serialize for i64 {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(size_of::<i64>())
    }
}

impl Deserialize for i64 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..8]
                    .try_into()
                    .map_err(|_| format!("Failed to decode u64: {bytes:?}"))?,
            ),
            &bytes[8..],
        ))
    }
}

impl Serialize for u128 {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.extend_from_slice(&self.to_be_bytes());
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(size_of::<u128>())
    }
}

impl Deserialize for u128 {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((
            Self::from_be_bytes(
                bytes[0..16]
                    .try_into()
                    .map_err(|_| format!("Failed to decode u128: {bytes:?}"))?,
            ),
            &bytes[16..],
        ))
    }
}

impl<T: Serialize, const N: usize> Serialize for [T; N] {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        for elem in self {
            bytes = elem.serialize(bytes)
        }
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        let mut size = 0;
        for elem in self {
            size += elem.byte_size()?;
        }
        Some(size)
    }
}

impl<T: Deserialize, const N: usize> Deserialize for [T; N] {
    fn deserialize(mut bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let mut out: [MaybeUninit<T>; N] = MaybeUninit::uninit_array();
        for elem in &mut out[..] {
            let (val, rest) = T::deserialize(bytes)?;
            bytes = rest;
            elem.write(val);
        }
        Ok((unsafe { MaybeUninit::array_assume_init(out) }, bytes))
    }
}

impl<A: Serialize, B: Serialize> Serialize for (A, B) {
    fn serialize(&self, bytes: Serializer) -> Serializer {
        let bytes = self.0.serialize(bytes);
        self.1.serialize(bytes)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.0.byte_size()? + self.1.byte_size()?)
    }
}

impl<A: Deserialize, B: Deserialize> Deserialize for (A, B) {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let (a, rest) = A::deserialize(bytes)?;
        let (b, rest) = B::deserialize(rest)?;
        Ok(((a, b), rest))
    }
}

impl<A: Serialize, B: Serialize, C: Serialize> Serialize for (A, B, C) {
    fn serialize(&self, bytes: Serializer) -> Serializer {
        let bytes = self.0.serialize(bytes);
        let bytes = self.1.serialize(bytes);
        self.2.serialize(bytes)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.0.byte_size()? + self.1.byte_size()? + self.2.byte_size()?)
    }
}

impl<A: Deserialize, B: Deserialize, C: Deserialize> Deserialize for (A, B, C) {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let (a, rest) = A::deserialize(bytes)?;
        let (b, rest) = B::deserialize(rest)?;
        let (c, rest) = C::deserialize(rest)?;
        Ok(((a, b, c), rest))
    }
}

impl<A: Serialize, B: Serialize, C: Serialize, D: Serialize> Serialize for (A, B, C, D) {
    fn serialize(&self, bytes: Serializer) -> Serializer {
        let bytes = self.0.serialize(bytes);
        let bytes = self.1.serialize(bytes);
        let bytes = self.2.serialize(bytes);
        self.3.serialize(bytes)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.0.byte_size()? + self.1.byte_size()? + self.2.byte_size()? + self.3.byte_size()?)
    }
}

impl<A: Deserialize, B: Deserialize, C: Deserialize, D: Deserialize> Deserialize for (A, B, C, D) {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let (a, rest) = A::deserialize(bytes)?;
        let (b, rest) = B::deserialize(rest)?;
        let (c, rest) = C::deserialize(rest)?;
        let (d, rest) = D::deserialize(rest)?;
        Ok(((a, b, c, d), rest))
    }
}

impl<T: Serialize> Serialize for Option<T> {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        match self {
            Some(elem) => {
                bytes.push(1);
                bytes = elem.serialize(bytes);
            }
            None => bytes.push(0),
        }
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(match self {
            Some(elem) => 1 + elem.byte_size()?,
            None => 1,
        })
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        if bytes[0] == 0 {
            Ok((None, &bytes[1..]))
        } else {
            let (elem, bytes) = T::deserialize(&bytes[1..])?;
            Ok((Some(elem), bytes))
        }
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes = (self.len() as u32).serialize(bytes);
        for elem in self.iter() {
            bytes = elem.serialize(bytes);
        }
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        let mut size = 0;
        for elem in self {
            size += elem.byte_size()?;
        }
        Some(size)
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

impl<K: Serialize, V: Serialize> Serialize for std::collections::BTreeMap<K, V> {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes = (self.len() as u32).serialize(bytes);
        for (key, value) in self.iter() {
            bytes = key.serialize(bytes);
            bytes = value.serialize(bytes);
        }
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        let mut size = 0;
        for (k, v) in self {
            size += k.byte_size()?;
            size += v.byte_size()?;
        }
        Some(size)
    }
}

impl<K: Deserialize + std::cmp::Ord, V: Deserialize> Deserialize
    for std::collections::BTreeMap<K, V>
{
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let mut rest = bytes;
        let (len, bytes) = u32::deserialize(rest)?;
        rest = bytes;

        let mut arr = Self::new();
        for _ in 0..len {
            let (key, bytes) = K::deserialize(rest)?;
            let (value, bytes) = V::deserialize(bytes)?;
            rest = bytes;
            arr.insert(key, value);
        }

        Ok((arr, rest))
    }
}

impl Serialize for bool {
    fn serialize(&self, mut bytes: Serializer) -> Serializer {
        bytes.push(if self == &true { 1u8 } else { 0u8 });
        bytes
    }

    fn byte_size(&self) -> Option<usize> {
        Some(1)
    }
}

impl Deserialize for bool {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        Ok((bytes[0] != 0, &bytes[1..]))
    }
}
