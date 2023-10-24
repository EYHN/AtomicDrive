use db::{DBLock, DBRead, DBWrite};

use crate::tracker::{Tracker as RawTracker, TrackerTransaction as RawTrackerTransaction};

pub struct Tracker<DBImpl> {
    db: DBImpl,
}

impl<DBImpl: DBRead> Tracker<DBImpl> {
    fn tracker(&self) -> RawTracker<DBImpl> {
        RawTracker::from_db(self.db)
    }
}

pub struct TrackerTransaction<DBImpl> {
    transaction: DBImpl,
}

impl<DBImpl: DBRead + DBWrite + DBLock> TrackerTransaction<DBImpl> {
    fn tracker(&mut self) -> RawTrackerTransaction<&mut DBImpl> {
        RawTrackerTransaction::from_db(&mut self.transaction)
    }
}
