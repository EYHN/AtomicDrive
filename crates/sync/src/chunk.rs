use std::fmt::Debug;

use sha2::{Digest, Sha256};
use utils::bytes_stringify;

#[derive(Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileHashChunks {
    chunks: Vec<HashChunk>,
    hash: [u8; 16],
}

impl Debug for FileHashChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHashChunks")
            .field("chunks", &self.chunks)
            .field("hash", &bytes_stringify(&self.hash))
            .finish()
    }
}

#[derive(Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
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

fn chunks(data: &[u8]) -> FileHashChunks {
    let chunker = fastcdc::v2020::FastCDC::new(
        data, 65536,  /* 64 KiB */
        131072, /* 128 KiB */
        262144, /* 256 KiB */
    );
    let mut chunks = if let Some(maxSize) = chunker.size_hint().1 {
        Vec::with_capacity(maxSize)
    } else {
        Vec::new()
    };
    for chunk in chunker {
        chunks.push(HashChunk {
            size: chunk.length.try_into().unwrap(),
            hash: Sha256::digest(&data[chunk.offset..chunk.offset + chunk.length])[0..16]
                .try_into()
                .unwrap(),
        })
    }

    FileHashChunks {
        hash: {
            let mut fileHash = Sha256::new();
            for chunk in chunks.iter() {
                fileHash.update(&chunk.hash)
            }
            fileHash.finalize()[0..16].try_into().unwrap()
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
