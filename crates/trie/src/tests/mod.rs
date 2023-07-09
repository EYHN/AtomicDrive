#[macro_use]
mod tools;

#[test]
fn write_with_rename() {
    testing!(
        have { local(1) }
        on local {
            mkdir "/hello";
            write "/hello/file" "world";
        }
        clone { local => remote(2) }
        on remote {
            rename "/hello" "/dir";
        }
        on local {
            write "/hello/file" "helloworld";
        }
        sync { local <=> remote }
        check local  {
            "
                └ dir/file [helloworld]
                "
        }
    );
}

#[test]
fn clock_test() {
    testing!(
        have { local(1) remote(2) }
        on local {
            date 0;
            write "/file" "local";
        }
        sync { local <=> remote }
        on remote {
            date 999;
            write "/file" "remote";
        }
        sync { local <=> remote }
        on local {
            date 0;
            write "/file" "some";
        }
        sync { local <=> remote }
        check local remote {
            // date does not affect sync if there are no conflicts
            "
                └ file [some]
                "
        }
    );
}

#[test]
fn file_conflict_test() {
    testing!(
        have { local(1) remote(2) }
        on local {
            write "/file" "local";
        }
        on remote {
            write "/file" "remote";
        }
        sync { local <=> remote }
        check local remote {
            // remote id is larger, keep the remote
            "
                └ file [remote]
                "
        }
    );

    testing!(
        have { local(1) remote(2) }
        on local {
            date 2;
            write "/file" "local";
        }
        on remote {
            date 1;
            write "/file" "remote";
        }
        sync { local <=> remote }
        check local remote {
            // local date is larger, keep the local
            "
                └ file [local]
                "
        }
    );
}

#[test]
fn folder_conflict_test() {
    testing!(
        have { local(1) remote(2) }
        on local {
            mkdir "/folder1";
            write "/folder1/foo" "bar";
        }
        on remote {
            mkdir "/folder1";
            write "/folder1/file" "abc";
        }
        sync { local <=> remote }
        on remote {
            mkdir "/folder1";
            write "/folder1/hello" "world";
        }
        sync { local <=> remote }
        check local remote {
            // no rename, we just merge the conflict folder
            "
                └ folder1
                 ├ file [abc]
                 ├ foo [bar]
                 └ hello [world]
                "
        }
    );

    testing!(
        have { local(1) }
        on local {
            mkdir "/folder1";
            write "/folder1/foo" "bar";
        }
        clone { local => remote(2) }
        on remote {
            mkdir "/folder2";
            write "/folder2/hello" "world";
            rename "/folder2" "/folder3";
        }
        on local {
            rename "/folder1" "/folder3";
        }
        sync { local <=> remote }
        check local remote {
            // both version of folder3 has content, we can't merge them
            // remote id is larger, keep the remote
            "
                └ folder3/hello [world]
                "
        }
    );

    testing!(
        have { local(1) }
        on local {
            mkdir "/folder1";
            write "/folder1/foo" "bar";
        }
        clone { local => remote(2) }
        on remote {
            mkdir "/folder2";
            rename "/folder2" "/folder3";
            write "/folder3/hello" "world";
        }
        on local {
            rename "/folder1" "/folder3";
        }
        sync { local <=> remote }
        check local remote {
            // local version folder3 has content, keep the local
            // and writes after the rename on the remote will apply to new folder3
            // its looks like merge
            "
                └ folder3
                 ├ foo [bar]
                 └ hello [world]
                "
        }
    );
}
