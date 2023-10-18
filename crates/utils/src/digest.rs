use xxhash_rust::xxh3::{Xxh3, Xxh3Builder};

pub trait Digest {
    fn update(&mut self, data: impl AsRef<[u8]>);
}

pub struct Xxhash {
    xxh: Xxh3,
}

impl Default for Xxhash {
    fn default() -> Self {
        Self::new()
    }
}

impl Xxhash {
    #[inline(always)]
    pub fn new() -> Self {
        Xxhash { xxh: Xxh3::new() }
    }

    #[inline(always)]
    pub fn finish(&self) -> [u8; 8] {
        self.xxh.digest().to_be_bytes()
    }

    #[inline(always)]
    pub fn finish128(&self) -> [u8; 16] {
        self.xxh.digest128().to_be_bytes()
    }
}

impl Digest for Xxhash {
    #[inline(always)]
    fn update(&mut self, data: impl AsRef<[u8]>) {
        self.xxh.update(data.as_ref())
    }
}

pub trait Digestible {
    fn digest(&self, data: &mut impl Digest);
}

impl Digestible for String {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self.as_bytes())
    }
}

impl Digestible for u64 {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self.to_be_bytes())
    }
}

impl Digestible for i64 {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self.to_be_bytes())
    }
}

impl Digestible for u128 {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self.to_be_bytes())
    }
}

impl Digestible for bool {
    fn digest(&self, d: &mut impl Digest) {
        d.update(if self == &true { [1u8] } else { [0u8] })
    }
}

impl Digestible for Vec<u8> {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self)
    }
}
