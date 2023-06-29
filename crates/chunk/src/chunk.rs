use std::fmt::{Debug, Display};

use utils::bytes_stringify;
use xxhash_rust::xxh3::xxh3_128;

#[derive(Default, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HashChunks {
    chunks: Vec<HashChunk>,
    hash: [u8; 16],
}

impl Display for HashChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} chunks", self.chunks.len())
    }
}

impl Debug for HashChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashChunks")
            .field("chunks", &self.chunks)
            .field("hash", &bytes_stringify(&self.hash))
            .finish()
    }
}

#[derive(Default, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HashChunk {
    size: u32,
    hash: [u8; 16],
}

impl Debug for HashChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashChunk")
            .field("size", &self.size)
            .field("hash", &bytes_stringify(&self.hash))
            .finish()
    }
}

pub fn chunks(data: &[u8]) -> HashChunks {
    let chunker = fastcdc::v2020::FastCDC::new(
        data, 65536,  /* 64 KiB */
        131072, /* 128 KiB */
        262144, /* 256 KiB */
    );
    let mut chunks = if let Some(max_size) = chunker.size_hint().1 {
        Vec::with_capacity(max_size)
    } else {
        Vec::new()
    };
    for chunk in chunker {
        chunks.push(HashChunk {
            size: chunk.length.try_into().unwrap(),
            hash: xxh3_128(&data[chunk.offset..chunk.offset + chunk.length]).to_be_bytes(),
        })
    }

    HashChunks {
        hash: {
            let mut hashs = vec![];
            for chunk in chunks.iter() {
                hashs.extend(&chunk.hash)
            }
            xxh3_128(&hashs).to_be_bytes()
        },
        chunks,
    }
}

#[cfg(test)]
mod tests {
    use super::chunks;

    #[test]
    fn test() {
        dbg!(chunks(include_bytes!("test.jpg")));
    }
}
