use std::{
    cmp::Ordering,
    collections::BTreeMap,
    convert::Infallible,
    fmt::{self, Debug, Display}
};

use crdts::{
    ctx::{AddCtx, ReadCtx},
    CmRDT, CvRDT, Dot, ResetRemove, VClock,
};

#[derive(Debug, PartialEq, Clone, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct VLabel<M: Ord, A: Ord> {
    clock: VClock<A>,
    marker: M,
    dot: Dot<A>,
}

impl<M: Ord, A: Ord> PartialOrd for VLabel<M, A> {
    fn partial_cmp(&self, other: &VLabel<M, A>) -> Option<Ordering> {
        match self.partial_cmp(other) {
            Some(Ordering::Equal) | None => Some(
                self.marker.cmp(&other.marker).then(
                    self.dot
                        .actor
                        .cmp(&other.dot.actor)
                        .then(self.dot.counter.cmp(&other.dot.counter)),
                ),
            ),
            Some(ordering) => Some(ordering),
        }
    }
}

/// VReg - Version Register
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VReg<V, M: Ord, A: Ord> {
    history: BTreeMap<VLabel<M, A>, V>,
}

/// Defines the set of operations over the VReg
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Op<V, M: Ord, A: Ord> {
    /// Put a value
    Put {
        /// context of the operation
        clock: VClock<A>,
        /// the value to put
        val: V,
        /// the marker should be monotonic value associated with this val
        marker: M,
        /// The Actor and the Actor's version at the time of the add
        dot: Dot<A>,
    },
}

impl<V: Display, M: Ord + Display, A: Ord + Display> Display for VReg<V, M, A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "|")?;
        for (i, (label, val)) in self.history.iter().enumerate() {
            let ctx = label.clock;
            let dot = label.dot;
            let marker = label.marker;
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}by@{}[#{}]", val, ctx, dot.actor, marker)?;
        }
        write!(f, "|")
    }
}

impl<V: PartialEq, M: Ord + PartialEq, A: Ord> PartialEq for VReg<V, M, A> {
    fn eq(&self, other: &Self) -> bool {
        self.history == other.history
    }
}

impl<V: Eq, M: Ord + Eq, A: Ord> Eq for VReg<V, M, A> {}

impl<V, M: Ord, A: Ord> ResetRemove<A> for VReg<V, M, A> {
    fn reset_remove(&mut self, clock: &VClock<A>) {
        debug_assert!(false, "No reset")
    }
}

impl<V, M: Ord, A: Ord> Default for VReg<V, M, A> {
    fn default() -> Self {
        Self {
            history: Default::default(),
        }
    }
}

// impl<V, M, A: Ord> CvRDT for VReg<V, M, A> {
//     type Validation = Infallible;

//     fn validate_merge(&self, _other: &Self) -> Result<(), Self::Validation> {
//         Ok(())
//     }

//     fn merge(&mut self, other: Self) {
//         self.vals = mem::take(&mut self.vals)
//             .into_iter()
//             .filter(|(clock, _, _)| other.vals.iter().filter(|(c, _, _)| clock < c).count() == 0)
//             .collect();

//         self.vals.extend(
//             other
//                 .vals
//                 .into_iter()
//                 .filter(|(clock, _, _)| self.vals.iter().filter(|(c, _, _)| clock < c).count() == 0)
//                 .filter(|(clock, _, _)| self.vals.iter().all(|(c, _, _)| clock != c))
//                 .collect::<Vec<_>>(),
//         );
//     }
// }

impl<V, M: Ord, A: Ord> CmRDT for VReg<V, M, A> {
    type Op = Op<V, M, A>;
    type Validation = Infallible;

    fn validate_op(&self, _op: &Self::Op) -> Result<(), Self::Validation> {
        Ok(())
    }

    fn apply(&mut self, op: Self::Op) {
        match op {
            Op::Put {
                clock,
                val,
                marker,
                dot,
            } => {
                if clock.is_empty() {
                    return;
                }

                let label = 
                // first filter out all values that are dominated by the Op clock
                self.vals.retain(|(val_clock, _, _)| {
                    matches!(
                        val_clock.partial_cmp(&clock),
                        None | Some(Ordering::Greater)
                    )
                });

                // TAI: in the case were the Op has a context that already was present,
                //      the above line would remove that value, the next lines would
                //      keep the val from the Op, so.. a malformed Op could break
                //      commutativity.

                // now check if we've already seen this op
                let mut should_add = true;
                for (existing_clock, _, _) in self.vals.iter() {
                    if existing_clock > &clock {
                        // we've found an entry that dominates this op
                        should_add = false;
                    }
                }

                if should_add {
                    self.vals.push((clock, val, marker));
                }
            }
        }
    }
}

impl<V, M, A: Ord + Clone + Debug> MVVReg<V, M, A> {
    /// Construct a new empty MVReg
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the value of the register
    pub fn write(&self, val: V, marker: M, ctx: AddCtx<A>) -> Op<V, M, A> {
        Op::Put {
            clock: ctx.clock,
            val,
            marker,
        }
    }

    /// Consumes the register and returns the values
    pub fn read(&self) -> ReadCtx<Vec<V>, A>
    where
        V: Clone,
    {
        let clock = self.clock();
        let concurrent_vals = self.vals.iter().map(|(_, v, _)| v).cloned().collect();

        ReadCtx {
            add_clock: clock.clone(),
            rm_clock: clock,
            val: concurrent_vals,
        }
    }

    /// Consumes the register and returns single value.
    /// Marker needs to implement [Ord] for comparing priorities.
    /// When there is a version conflict, the value with the highest priority is returned.
    pub fn read_single(&self) -> ReadCtx<Option<V>, A>
    where
        V: Clone,
        M: Ord,
    {
        let max_value = self.vals.iter().max_by(|a, b| {
            let clock_order = a.0.partial_cmp(&b.0);
            if matches!(clock_order, None | Some(Ordering::Equal)) {
                let value_order = a.2.cmp(&b.2);
                if matches!(value_order, Ordering::Equal) {
                    panic!("Conflicting versions and values with the same marker.")
                } else {
                    value_order
                }
            } else {
                clock_order.expect("checked")
            }
        });

        if let Some((clock, value, _)) = max_value {
            ReadCtx {
                add_clock: clock.clone(),
                rm_clock: clock.clone(),
                val: Some(value.clone()),
            }
        } else {
            let clock = self.clock();
            ReadCtx {
                add_clock: clock.clone(),
                rm_clock: clock,
                val: None,
            }
        }
    }

    /// Retrieve the current read context
    pub fn read_ctx(&self) -> ReadCtx<(), A> {
        let clock = self.clock();
        ReadCtx {
            add_clock: clock.clone(),
            rm_clock: clock,
            val: (),
        }
    }

    /// A clock with latest versions of all actors operating on this register
    fn clock(&self) -> VClock<A> {
        self.vals
            .iter()
            .fold(VClock::new(), |mut accum_clock, (c, _, _)| {
                accum_clock.merge(c.clone());
                accum_clock
            })
    }
}

#[cfg(test)]
mod tests {
    use crdts::{CmRDT, VClock};

    use crate::MVVReg;

    #[test]
    fn test_mvvreg() {
        let mut mvvreg = MVVReg::<u32, u32, u32>::new();

        let read_ctx_1 = mvvreg.read_ctx();
        let read_ctx_2 = mvvreg.read_ctx();
        mvvreg.apply(mvvreg.write(123, 1, read_ctx_1.derive_add_ctx(1)));
        mvvreg.apply(mvvreg.write(321, 2, read_ctx_2.derive_add_ctx(2)));

        assert_eq!(mvvreg.read().val, vec![123, 321]);
        assert_eq!(mvvreg.read_single().val.unwrap(), 321);
        assert_eq!(mvvreg.read_single().add_clock, {
            let mut vc = VClock::<u32>::new();
            vc.apply(vc.inc(2));
            vc
        });

        mvvreg.apply(mvvreg.write(512, 3, mvvreg.read_single().derive_add_ctx(2)));
        assert_eq!(mvvreg.read().val, vec![123, 512]);
        assert_eq!(mvvreg.read_single().val.unwrap(), 512);
        assert_eq!(mvvreg.read_single().add_clock, {
            let mut vc = VClock::<u32>::new();
            vc.apply(vc.inc(2));
            vc.apply(vc.inc(2));
            vc
        });

        dbg!(mvvreg);
    }
}
