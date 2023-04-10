use crdts::{CmRDT, CvRDT, LWWReg};

use crate::{IndexPeerId, IndexedFile};

#[derive(
    Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
struct LWWMarker {
    timestamp: u64,
    peer_id: IndexPeerId,
}

type LWW = LWWReg<IndexedFile, LWWMarker>;

#[derive(Debug, Hash, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VIndexReg {
    lww: LWW,
}

impl VIndexReg {
    fn update(&self, file: IndexedFile, timestamp: u64, peer_id: IndexPeerId) -> Self {
        VIndexReg {
            lww: self.lww.update(
                file,
                LWWMarker {
                    timestamp: timestamp,
                    peer_id: peer_id,
                },
            ),
        }
    }
}

impl PartialOrd for VIndexReg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.lww.marker.partial_cmp(&other.lww.marker)
    }
}

impl CvRDT for VIndexReg {
    type Validation = <LWW as CvRDT>::Validation;

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        self.lww.validate_merge(&other.lww)
    }

    fn merge(&mut self, other: Self) {
        self.lww.merge(other.lww)
    }
}

impl CmRDT for VIndexReg {
    type Op = Self;

    type Validation = <LWW as CmRDT>::Validation;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        self.lww.validate_op(&op.lww)
    }

    fn apply(&mut self, op: Self::Op) {
        self.lww.apply(op.lww)
    }
}
