use crdts::{CmRDT, CvRDT, LWWReg, ResetRemove, VClock};

use crate::IndexPeerId;

#[derive(Debug, Hash, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VIndexRegMarker {
    clock: VClock<IndexPeerId>,
    timestamp: u64,
    peer_id: IndexPeerId,
}

impl Ord for VIndexRegMarker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.clock.partial_cmp(&other.clock) {
            Some(std::cmp::Ordering::Equal) | None => match self.timestamp.cmp(&other.timestamp) {
                std::cmp::Ordering::Equal => self.peer_id.cmp(&other.peer_id),
                ord => ord,
            },
            Some(ord) => ord,
        }
    }
}

impl PartialOrd for VIndexRegMarker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[allow(clippy::upper_case_acronyms)]
type LWW<IndexedObject> = LWWReg<IndexedObject, VIndexRegMarker>;

#[derive(Debug, Default, Hash, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VIndexReg<IndexedObject> {
    lww: Option<LWW<IndexedObject>>,
}

impl<IndexedObject> VIndexReg<IndexedObject> {
    pub fn new(
        obj: IndexedObject,
        clock: VClock<IndexPeerId>,
        timestamp: u64,
        peer_id: IndexPeerId,
    ) -> Self {
        VIndexReg {
            lww: Some(LWW {
                val: obj,
                marker: VIndexRegMarker {
                    clock,
                    timestamp,
                    peer_id,
                },
            }),
        }
    }

    pub fn val(&self) -> Option<&IndexedObject> {
        if let Some(lww) = &self.lww {
            Some(&lww.val)
        } else {
            None
        }
    }
}

impl<IndexedObject: PartialEq> PartialOrd for VIndexReg<IndexedObject> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if let Some(lww) = self.lww.as_ref() {
            if let Some(other) = other.lww.as_ref() {
                lww.marker.partial_cmp(&other.marker)
            } else {
                Some(std::cmp::Ordering::Greater)
            }
        } else if other.lww.is_some() {
            Some(std::cmp::Ordering::Less)
        } else {
            Some(std::cmp::Ordering::Equal)
        }
    }
}

impl<IndexedObject: PartialEq> CvRDT for VIndexReg<IndexedObject> {
    type Validation = <LWW<IndexedObject> as CvRDT>::Validation;

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        if let Some(lww) = self.lww.as_ref() {
            if let Some(other) = other.lww.as_ref() {
                return lww.validate_merge(other);
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: Self) {
        if let Some(lww) = self.lww.as_mut() {
            if let Some(other) = other.lww {
                lww.merge(other);
            }
        } else if let Some(other) = other.lww {
            self.lww = Some(other);
        }
    }
}

impl<IndexedObject: PartialEq> CmRDT for VIndexReg<IndexedObject> {
    type Op = Self;

    type Validation = <LWW<IndexedObject> as CmRDT>::Validation;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        if let Some(lww) = self.lww.as_ref() {
            if let Some(op) = op.lww.as_ref() {
                return lww.validate_op(op);
            }
        }
        Ok(())
    }

    fn apply(&mut self, op: Self::Op) {
        if let Some(lww) = self.lww.as_mut() {
            if let Some(op) = op.lww {
                lww.apply(op)
            }
        } else if let Some(op) = op.lww {
            self.lww = Some(op)
        }
    }
}

impl<A: Ord, IndexedObject> ResetRemove<A> for VIndexReg<IndexedObject> {
    fn reset_remove(&mut self, _clock: &crdts::VClock<A>) {
        // not need
    }
}
