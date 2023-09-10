use crate::{backend, DBRead, DBTransaction, DBWrite, Error, DB};

#[test]
fn test_memory_db() -> Result<(), Error> {
    let db = backend::memory::MemoryDB::default();

    let mut a = db.start_transaction()?;

    a.set(*b"123", *b"321")?;

    a.commit()?;

    dbg!(db.get(*b"123")?.unwrap());

    Ok(())
}
