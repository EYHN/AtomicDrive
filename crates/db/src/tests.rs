use crate::{backend, DBReadDyn, DBTransaction, DBWrite, Result, DB};

macro_rules! testing {
    (@db: $($db:ident)* ,@tests: $($test:ident)*) => {
        {
            macro_rules! call_tests {
                ($$db:ident) => {
                    $(
                        $$db.drop_all()?;
                        println!("Starting run test {} for {}", stringify!($test), stringify!($$db));
                        $test(&$$db)?;
                    )*
                };
            }

            $({call_tests!($db);})*
        }
    };
}

#[test]
fn test_db() -> Result<()> {
    let mut memory_db = backend::memory::MemoryDB::default();
    let mut rocks_db =
        backend::rocks::RocksDB::open_or_create_database(test_results::save_dir!("rocks"))?;

    testing!(
        @db: rocks_db memory_db,
        @tests: basic_write get_range rollback
    );

    Ok(())
}

fn basic_write<D: DB>(db: &D) -> Result<()> {
    assert!(db.get(*b"test")?.is_none());

    let mut t = db.start_transaction()?;

    t.set(*b"test", *b"hello")?;

    t.commit()?;

    assert_eq!(db.get(*b"test")?.unwrap().as_ref(), b"hello");

    Ok(())
}

fn get_range<D: DB>(db: &D) -> Result<()> {
    let mut t = db.start_transaction()?;

    t.set(*b"100", *b"0")?;
    t.set(*b"101", *b"1")?;
    t.set(*b"102", *b"2")?;
    t.set(*b"103", *b"3")?;
    t.set(*b"104", *b"4")?;
    t.set(*b"105", *b"5")?;

    t.commit()?;

    let kvs = db
        .get_range(b"101", b"104")
        .map(|d| d.unwrap())
        .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec()))
        .collect::<Vec<_>>();

    assert_eq!(
        kvs,
        vec![
            (b"101".to_vec(), b"1".to_vec()),
            (b"102".to_vec(), b"2".to_vec()),
            (b"103".to_vec(), b"3".to_vec())
        ]
    );

    Ok(())
}

fn rollback<D: DB>(db: &D) -> Result<()> {
    let mut t = db.start_transaction()?;
    t.set(*b"100", *b"0")?;
    t.set(*b"101", *b"1")?;
    t.commit()?;

    assert_eq!(db.get(*b"100")?.unwrap().as_ref(), b"0");
    assert_eq!(db.get(*b"101")?.unwrap().as_ref(), b"1");

    let mut t = db.start_transaction()?;
    t.set(*b"100", *b"hello")?;
    t.delete(*b"101")?;
    t.set(*b"102", *b"2")?;
    t.rollback()?;

    assert_eq!(db.get(*b"100")?.unwrap().as_ref(), b"0");
    assert_eq!(db.get(*b"101")?.unwrap().as_ref(), b"1");
    assert!(db.get(*b"102")?.is_none());

    Ok(())
}
