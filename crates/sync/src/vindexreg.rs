use crdts::{CmRDT, LWWReg, ResetRemove, CvRDT};

use crate::{IndexPeerId, IndexedFile};

#[derive(
    Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct VIndexRegMarker {
    timestamp: u64,
    peer_id: IndexPeerId,
}

type LWW = LWWReg<IndexedFile, VIndexRegMarker>;

#[derive(Debug, Default, Hash, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VIndexReg {
    lww: Option<LWW>,
}

impl VIndexReg {
    pub fn new(file: IndexedFile, timestamp: u64, peer_id: IndexPeerId) -> Self {
        VIndexReg {
            lww: Some(LWW {
                val: file,
                marker: VIndexRegMarker {
                    timestamp: timestamp,
                    peer_id: peer_id,
                },
            }),
        }
    }

    pub fn val(&self) -> Option<&IndexedFile> {
        if let Some(lww) = &self.lww {
            Some(&lww.val)
        } else {
            None
        }
    }
}

// impl PartialOrd for VIndexReg {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         if let Some(lww) = self.lww {
//             if let Some(other) = other.lww {
    
//                 lww.marker.partial_cmp(&other.marker)
//             } else {
//                 Some(std::cmp::Ordering::Greater)
//             }
//         } else {
//             if let Some(_) = other.lww {
//                 Some(std::cmp::Ordering::Less)
//             } else {
//                 Some(std::cmp::Ordering::Equal)
//             }
//         }
//     }
// }

impl CvRDT for VIndexReg {
    type Validation = <LWW as CvRDT>::Validation;

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        if let Some(ref lww) = self.lww {
            if let Some(ref other) = other.lww {
                return lww.validate_merge(&other);
            }
        }
        return Ok(());
    }

    fn merge(&mut self, other: Self) {
        if let Some(ref mut lww) = self.lww {
            if let Some(other) = other.lww {
                lww.merge(other);
            }
        } else {
            if let Some(other) = other.lww {
                self.lww = Some(other);
            }
        }
    }
}

impl CmRDT for VIndexReg {
    type Op = Self;

    type Validation = <LWW as CmRDT>::Validation;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        if let Some(ref lww) = self.lww {
            if let Some(ref op) = op.lww {
                return lww.validate_op(op);
            }
        }
        Ok(())
    }

    fn apply(&mut self, op: Self::Op) {
        if let Some(ref mut lww) = self.lww {
            if let Some(op) = op.lww {
                lww.apply(op)
            }
        } else {
            if let Some(op) = op.lww {
                self.lww = Some(op)
            }
        }
    }
}

impl<A: Ord> ResetRemove<A> for VIndexReg {
    fn reset_remove(&mut self, clock: &crdts::VClock<A>) {
        // not need
    }
}
