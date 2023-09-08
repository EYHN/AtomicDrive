pub trait Digest {
    fn update(&mut self, data: impl AsRef<[u8]>);
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