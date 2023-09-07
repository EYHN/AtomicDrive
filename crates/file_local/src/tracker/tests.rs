use file::{FileFullPath, FileType};
use utils::bytes_stringify;

use crate::tracker::LocalFileSystemTrackerEventPack;

use super::LocalFileSystemTracker;

#[test]
fn test() {
    let db_dir = test_results::save_dir!("tracker_db");
    std::fs::remove_dir_all(&db_dir).unwrap();
    fn dbg_events(events: LocalFileSystemTrackerEventPack) {
        for event in events.iter() {
            println!(
                "event!: {:?} {} [{}]",
                event.event_type,
                bytes_stringify(&event.file_identifier),
                event.file_path
            )
        }
    }
    let db = LocalFileSystemTracker::open_or_create_database(&db_dir).unwrap();

    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/a"),
            FileType::File,
            b"test_file_a".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );
    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/b"),
            FileType::File,
            b"test_file_b".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );
    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/b/1"),
            FileType::File,
            b"test_file_b1".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );
    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/b/2"),
            FileType::File,
            b"test_file_b1".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );
    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/c"),
            FileType::File,
            b"test_file_c".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );
    dbg_events(
        db.index(crate::tracker::IndexInput::File(
            FileFullPath::parse("/test/c;1"),
            FileType::File,
            b"test_file_c;1".to_vec(),
            b"1".to_vec(),
        ))
        .unwrap(),
    );

    dbg_events(
        db.index(crate::tracker::IndexInput::Directory(
            FileFullPath::parse("/test"),
            b"test_dir".to_vec(),
            b"1".to_vec(),
            vec![
                (
                    "a".to_owned(),
                    FileType::File,
                    b"test_file_a".to_vec(),
                    b"1".to_vec(),
                ),
                (
                    "e".to_owned(),
                    FileType::File,
                    b"test_file_e".to_vec(),
                    b"1".to_vec(),
                ),
            ],
        ))
        .unwrap(),
    );

    println!("\n---- DB Dump ----\n");

    for (key, value) in db.dump().unwrap() {
        println!(
            "{} => {}",
            bytes_stringify(&key.to_bytes().unwrap()),
            bytes_stringify(&value.into_bytes().unwrap())
        );
    }

    println!("\n-----------------\n");
}
