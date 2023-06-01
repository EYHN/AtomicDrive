use std::borrow::Cow;

pub struct PathTools;

fn is_path_separator(code: &char) -> bool {
    code == &PathTools::DIRECTORY_SEPARATOR_CHAR
}

impl PathTools {
    pub const DIRECTORY_SEPARATOR_CHAR: char = '/';

    pub fn relative(from: &str, to: &str) -> Cow<'static, str> {
        if from == to {
            return Cow::Borrowed("");
        }

        // Trim leading forward slashes.
        let from = PathTools::resolve_iter([from].into_iter());
        let to = PathTools::resolve_iter([to].into_iter());

        if from == to {
            return Cow::Borrowed("");
        }

        let from_start = 1;
        let from_end = from.len();
        let from_len = from_end - from_start;
        let to_start = 1;
        let to_len = to.len() - to_start;

        // Compare paths to find the longest common path from root
        let length = if from_len < to_len { from_len } else { to_len };
        let mut last_common_sep: Option<usize> = None;
        let mut i = 0;
        loop {
            if i >= length {
                break;
            }
            let from_code = from.chars().nth(from_start + i);
            if from_code != to.chars().nth(to_start + i) {
                break;
            } else if from_code == Some(PathTools::DIRECTORY_SEPARATOR_CHAR) {
                last_common_sep = Some(i);
            }
            i += 1;
        }
        if i == length {
            if to_len > length {
                if to.chars().nth(to_start + i) == Some(PathTools::DIRECTORY_SEPARATOR_CHAR) {
                    // We get here if `from` is the exact base path for `to`.
                    // For example: from='/foo/bar'; to='/foo/bar/baz'
                    return Cow::Owned(to[to_start + i + 1..].to_string());
                }
                if i == 0 {
                    // We get here if `from` is the root
                    // For example: from='/'; to='/foo'
                    return Cow::Owned(to[to_start + i..].to_string());
                }
            } else if from_end > length {
                if from.chars().nth(from_start + i) == Some(PathTools::DIRECTORY_SEPARATOR_CHAR) {
                    // We get here if `to` is the exact base path for `from`.
                    // For example: from='/foo/bar/baz'; to='/foo/bar'
                    last_common_sep = Some(i);
                } else if i == 0 {
                    // We get here if `to` is the root.
                    // For example: from='/foo/bar'; to='/'
                    last_common_sep = Some(0);
                }
            }
        }

        let mut out = String::new();
        // Generate the relative path based on the path difference between `to`
        // and `from`.
        let last_common_sep = last_common_sep.map(|i| i as isize).unwrap_or(-1);
        let mut i = from_start.checked_add_signed(last_common_sep + 1).unwrap();
        loop {
            if i > from_end {
                break;
            }
            if i == from_end || from.chars().nth(i) == Some(PathTools::DIRECTORY_SEPARATOR_CHAR) {
                out.push_str(if out.len() == 0 { ".." } else { "/.." });
            }

            i += 1;
        }

        // Lastly, append the rest of the destination (`to`) path that comes after
        // the common path parts.
        out.push_str(&to[to_start.checked_add_signed(last_common_sep).unwrap()..]);
        return Cow::Owned(out);
    }

    pub fn resolve(segment1: &str, segment2: &str) -> Cow<'static, str> {
        PathTools::resolve_iter([segment1, segment2].into_iter())
    }

    pub fn resolve_iter<'a>(
        paths: impl Iterator<Item = &'a str> + DoubleEndedIterator,
    ) -> Cow<'static, str> {
        let mut resolved_segments = Vec::<&str>::new();

        for path in paths.rev() {
            if path.is_empty() {
                continue;
            }

            resolved_segments.push(path);
            if path.starts_with(Self::DIRECTORY_SEPARATOR_CHAR) {
                break;
            }
        }

        let mut resolved_string = String::from("");
        for path in resolved_segments.into_iter().rev() {
            resolved_string.push_str(path);
            resolved_string.push(Self::DIRECTORY_SEPARATOR_CHAR);
        }

        let mut normalized = PathTools::normalize_string(&resolved_string, false);
        normalized.insert(0, '/');
        Cow::Owned(normalized)
    }

    pub fn join(segment1: &str, segment2: &str) -> Cow<'static, str> {
        PathTools::join_iter([segment1, segment2].into_iter())
    }

    pub fn join_iter<'a>(paths: impl Iterator<Item = &'a str>) -> Cow<'static, str> {
        let mut joined = String::new();
        for path in paths {
            if path.is_empty() {
                continue;
            }

            if joined.is_empty() {
                joined.push_str(path);
            } else {
                joined.push(Self::DIRECTORY_SEPARATOR_CHAR);
                joined.push_str(path);
            }
        }

        if joined.is_empty() {
            Cow::Borrowed(".")
        } else {
            PathTools::normalize(&joined)
        }
    }

    pub fn normalize(path: &str) -> Cow<'static, str> {
        if path.is_empty() {
            return Cow::Borrowed(".");
        }

        let is_absolute = path.starts_with(Self::DIRECTORY_SEPARATOR_CHAR);
        let trailing_separator = path.ends_with(Self::DIRECTORY_SEPARATOR_CHAR);

        let path = PathTools::normalize_string(path, !is_absolute);

        if path.is_empty() {
            if is_absolute {
                return Cow::Borrowed("/");
            }

            return if trailing_separator {
                Cow::Borrowed("./")
            } else {
                Cow::Borrowed(".")
            };
        }

        if !trailing_separator && !is_absolute {
            return Cow::Owned(path);
        }

        let mut new_path = String::new();

        if is_absolute {
            new_path.push('/');
        }

        new_path.push_str(&path);

        if trailing_separator {
            new_path.push('/');
        }

        Cow::Owned(new_path)
    }

    pub fn basename(path: &str) -> &str {
        let mut start = 0;
        let mut end = -1;
        let mut matched_slash = true;
        let mut i = path.len() as i32 - 1;

        for code in path.chars().rev() {
            if code == Self::DIRECTORY_SEPARATOR_CHAR {
                if !matched_slash {
                    start = i + 1;
                    break;
                }
            } else if end == -1 {
                matched_slash = false;
                end = i + 1;
            }

            i -= 1;
        }

        if end == -1 {
            ""
        } else {
            &path[start as usize..end as usize]
        }
    }

    /// The Extname() method returns the extension of the path, from the last occurrence
    /// of the . (period) character to end of string in the last portion of the path.
    /// If there is no . in the last portion of the path, or if there are no . characters
    /// other than the first character of the basename of path (see [`basename`]),
    /// an emptystring is returned.
    pub fn extname(path: &str) -> &str {
        let mut start_dot = -1;
        let mut start_part = 0;
        let mut end = -1;
        let mut matched_slash = true;

        // Track the state of characters (if any) we see before our first dot and
        // after any path separator we find
        let mut pre_dot_state = 0;

        let mut i = path.len() as i32;

        for code in path.chars().rev() {
            i -= 1;

            if code == Self::DIRECTORY_SEPARATOR_CHAR {
                // If we reached a path separator that was not part of a set of path
                // separators at the end of the string, stop now
                if !matched_slash {
                    start_part = i + 1;
                    break;
                }

                continue;
            }

            if end == -1 {
                // We saw the first non-path separator, mark this as the end of our
                // extension
                matched_slash = false;
                end = i + 1;
            }

            if code == '.' {
                // If this is our first dot, mark it as the start of our extension
                if start_dot == -1 {
                    start_dot = i;
                } else if pre_dot_state != 1 {
                    pre_dot_state = 1;
                }
            } else if start_dot != -1 {
                // We saw a non-dot and non-path separator before our dot, so we should
                // have a good chance at having a non-empty extension
                pre_dot_state = -1
            }
        }

        if start_dot == -1
            || end == -1
            // We saw a non-dot character immediately before the dot
            || pre_dot_state == 0
            // The (right-most) trimmed path component is exactly '..'
            || (pre_dot_state == 1 && start_dot == end - 1 && start_dot == start_part + 1)
        {
            ""
        } else {
            &path[start_dot as usize..end as usize]
        }
    }

    /// The method returns the directory name of a path, similar to the Unix dirname command. Trailing directory separators are ignored.
    pub fn dirname(path: &str) -> &str {
        if path.is_empty() {
            return ".";
        }

        let has_root = path.starts_with(Self::DIRECTORY_SEPARATOR_CHAR);
        let mut end = -1;
        let mut matched_slash = true;

        let mut i = path.len() as i32 - 1;

        for code in path.chars().rev() {
            if code == Self::DIRECTORY_SEPARATOR_CHAR {
                if !matched_slash {
                    end = i;
                    break;
                }
            } else {
                matched_slash = false
            }

            i -= 1;
            if i < 1 {
                break;
            }
        }

        if end == -1 {
            return if has_root { "/" } else { "." };
        }

        if has_root && end == 1 {
            return "//";
        }

        &path[0..end as usize]
    }

    /// Resolves . and .. elements in a path with directory names.
    fn normalize_string(path: &str, allow_above_root: bool) -> String {
        let mut res = String::new();
        let mut code = '\0';
        let mut last_segment_length = 0;
        let mut last_slash = -1;
        let mut dots = 0;
        let mut chars = path.chars();
        let mut i: i32 = -1;

        loop {
            i += 1;

            if let Some(next_code) = chars.next() {
                code = next_code
            } else if is_path_separator(&code) {
                break;
            } else {
                code = Self::DIRECTORY_SEPARATOR_CHAR
            }

            if is_path_separator(&code) {
                if last_slash == i - 1 || dots == 1 {
                    // NOOP
                } else if dots == 2 {
                    if res.len() < 2 || last_segment_length != 2 || !res.ends_with("..") {
                        if res.len() > 2 {
                            if let Some(last_slash_index) =
                                res.rfind(Self::DIRECTORY_SEPARATOR_CHAR)
                            {
                                res.truncate(last_slash_index);
                                last_segment_length = (res.len() as i32)
                                    - 1
                                    - res
                                        .rfind(Self::DIRECTORY_SEPARATOR_CHAR)
                                        .map_or(-1, |u| u as i32)
                            } else {
                                res.clear();
                                last_segment_length = 0;
                            }

                            last_slash = i;
                            dots = 0;
                            continue;
                        }

                        if !res.is_empty() {
                            res.clear();
                            last_segment_length = 0;
                            last_slash = i;
                            dots = 0;
                            continue;
                        }
                    }

                    if allow_above_root {
                        if !res.is_empty() {
                            res.push(Self::DIRECTORY_SEPARATOR_CHAR);
                        }

                        res.push_str("..");
                        last_segment_length = 2;
                    }
                } else {
                    if !res.is_empty() {
                        res.push(Self::DIRECTORY_SEPARATOR_CHAR);
                        res.push_str(&path[(last_slash + 1) as usize..i as usize]);
                    } else {
                        res = path[(last_slash + 1) as usize..i as usize].to_string()
                    }

                    last_segment_length = i - last_slash - 1;
                }

                last_slash = i;
                dots = 0;
            } else if code == '.' && dots != -1 {
                dots += 1;
            } else {
                dots = -1;
            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_test() {
        assert_eq!(PathTools::join("foo/x", "./bar"), "foo/x/bar");

        let j = |v: Vec<&str>| PathTools::join_iter(v.into_iter());

        assert_eq!(j(vec![]), ".");
        assert_eq!(j(vec![".", "x/b", "..", "/b/c.js"]), "x/b/c.js");
        assert_eq!(j(vec!["/.", "x/b", "..", "/b/c.js"]), "/x/b/c.js");
        assert_eq!(j(vec!["/foo", "../../../bar"]), "/bar");
        assert_eq!(j(vec!["foo", "../../../bar"]), "../../bar");
        assert_eq!(j(vec!["foo/", "../../../bar"]), "../../bar");
        assert_eq!(j(vec!["foo/x", "../../../bar"]), "../bar");
        assert_eq!(j(vec!["foo/x", "./bar"]), "foo/x/bar");
        assert_eq!(j(vec!["foo/x/", "./bar"]), "foo/x/bar");
        assert_eq!(j(vec!["foo/x/", ".", "bar"]), "foo/x/bar");
        assert_eq!(j(vec!["./"]), "./");
        assert_eq!(j(vec![".", "./"]), "./");
        assert_eq!(j(vec![".", ".", "."]), ".");
        assert_eq!(j(vec![".", "./", "."]), ".");
        assert_eq!(j(vec![".", "/./", "."]), ".");
        assert_eq!(j(vec![".", "/////./", "."]), ".");
        assert_eq!(j(vec!["."]), ".");
        assert_eq!(j(vec!["", "."]), ".");
        assert_eq!(j(vec!["", "foo"]), "foo");
        assert_eq!(j(vec!["foo", "/bar"]), "foo/bar");
        assert_eq!(j(vec!["", "/foo"]), "/foo");
        assert_eq!(j(vec!["", "", "/foo"]), "/foo");
        assert_eq!(j(vec!["", "", "foo"]), "foo");
        assert_eq!(j(vec!["foo", ""]), "foo");
        assert_eq!(j(vec!["foo/", ""]), "foo/");
        assert_eq!(j(vec!["foo", "", "/bar"]), "foo/bar");
        assert_eq!(j(vec!["./", "..", "/foo"]), "../foo");
        assert_eq!(j(vec!["./", "..", "..", "/foo"]), "../../foo");
        assert_eq!(j(vec![".", "..", "..", "/foo"]), "../../foo");
        assert_eq!(j(vec!["", "..", "..", "/foo"]), "../../foo");
        assert_eq!(j(vec!["/"]), "/");
        assert_eq!(j(vec!["/", "."]), "/");
        assert_eq!(j(vec!["/", ".."]), "/");
        assert_eq!(j(vec!["/", "..", ".."]), "/");
        assert_eq!(j(vec![""]), ".");
        assert_eq!(j(vec!["", ""]), ".");
        assert_eq!(j(vec![" /foo"]), " /foo");
        assert_eq!(j(vec![" ", "foo"]), " /foo");
        assert_eq!(j(vec![" ", "."]), " ");
        assert_eq!(j(vec![" ", "/"]), " /");
        assert_eq!(j(vec![" ", ""]), " ");
        assert_eq!(j(vec!["/", "foo"]), "/foo");
        assert_eq!(j(vec!["/", "/foo"]), "/foo");
        assert_eq!(j(vec!["/", "//foo"]), "/foo");
        assert_eq!(j(vec!["/", "", "/foo"]), "/foo");
        assert_eq!(j(vec!["", "/", "foo"]), "/foo");
        assert_eq!(j(vec!["", "/", "/foo"]), "/foo");
    }

    #[test]
    fn resolve_test() {
        let r = |v: Vec<&str>| PathTools::resolve_iter(v.into_iter());

        assert_eq!("/var/file", r(vec!["/var/lib", "../", "file/"]));
        assert_eq!("/var/file", r(vec!["/var/lib", "../", "", "file/"]));
        assert_eq!("/file", r(vec!["/var/lib", "/../", "file/"]));
        assert_eq!("/", r(vec!["a/b/c/", "../../.."]));
        assert_eq!("/", r(vec!["."]));
        assert_eq!("/absolute", r(vec!["/some/dir", ".", "/absolute/"]));
        assert_eq!(
            "/foo/tmp.3/cycles/root.js",
            r(vec!["/foo/tmp.3/", "../tmp.3/cycles/root.js"])
        );
        assert_eq!("/foo", r(vec!["../foo"]));
    }

    #[test]
    fn normalize_test() {
        assert_eq!(
            "fixtures/b/c.js",
            PathTools::normalize("./fixtures///b/../b/c.js")
        );
        assert_eq!("/bar", PathTools::normalize("/foo/../../../bar"));
        assert_eq!("a/b", PathTools::normalize("a//b//../b"));
        assert_eq!("a/b/c", PathTools::normalize("a//b//./c"));
        assert_eq!("a/b", PathTools::normalize("a//b//."));
        assert_eq!("/x/y/z", PathTools::normalize("/a/b/c/../../../x/y/z"));
        assert_eq!("/foo/bar", PathTools::normalize("///..//./foo/.//bar"));
        assert_eq!("bar/", PathTools::normalize("bar/foo../../"));
        assert_eq!("bar", PathTools::normalize("bar/foo../.."));
        assert_eq!("bar/baz", PathTools::normalize("bar/foo../../baz"));
        assert_eq!("bar/foo../", PathTools::normalize("bar/foo../"));
        assert_eq!("bar/foo..", PathTools::normalize("bar/foo.."));
        assert_eq!("../../bar", PathTools::normalize("../foo../../../bar"));
        assert_eq!(
            "../../bar",
            PathTools::normalize("../.../.././.../../../bar")
        );
        assert_eq!(
            "../../../../../bar",
            PathTools::normalize("../../../foo/../../../bar")
        );
        assert_eq!(
            "../../../../../../",
            PathTools::normalize("../../../foo/../../../bar/../../")
        );
        assert_eq!(
            "../../",
            PathTools::normalize("../foobar/barfoo/foo/../../../bar/../../")
        );
        assert_eq!(
            "../../../../baz",
            PathTools::normalize("../.../../foobar/../../../bar/../../baz")
        );
        assert_eq!("foo/bar\\baz", PathTools::normalize("foo/bar\\baz"));
    }

    #[test]
    fn basename_test() {
        assert_eq!(
            "test-path-basename.js",
            PathTools::basename("/fixtures/test/test-path-basename.js")
        );
        assert_eq!(".js", PathTools::basename(".js"));
        assert_eq!("", PathTools::basename(""));
        assert_eq!("basename.ext", PathTools::basename("/dir/basename.ext"));
        assert_eq!("basename.ext", PathTools::basename("/basename.ext"));
        assert_eq!("basename.ext", PathTools::basename("basename.ext"));
        assert_eq!("basename.ext", PathTools::basename("basename.ext/"));
        assert_eq!("basename.ext", PathTools::basename("basename.ext//"));
        assert_eq!("bbb", PathTools::basename("/aaa/bbb"));
        assert_eq!("aaa", PathTools::basename("/aaa/"));
        assert_eq!("b", PathTools::basename("/aaa/b"));
        assert_eq!("b", PathTools::basename("/a/b"));
        assert_eq!("a", PathTools::basename("//a"));

        assert_eq!(
            "\\dir\\basename.ext",
            PathTools::basename("\\dir\\basename.ext")
        );
        assert_eq!("\\basename.ext", PathTools::basename("\\basename.ext"));
        assert_eq!("basename.ext", PathTools::basename("basename.ext"));
        assert_eq!("basename.ext\\", PathTools::basename("basename.ext\\"));
        assert_eq!("basename.ext\\\\", PathTools::basename("basename.ext\\\\"));
        assert_eq!("foo", PathTools::basename("foo"));
    }

    #[test]
    fn extname_test() {
        assert_eq!("", PathTools::extname(""));

        assert_eq!(PathTools::extname(""), "");
        assert_eq!(PathTools::extname("/path/to/file"), "");
        assert_eq!(PathTools::extname("/path/to/file.ext"), ".ext");
        assert_eq!(PathTools::extname("/path.to/file.ext"), ".ext");
        assert_eq!(PathTools::extname("/path.to/file"), "");
        assert_eq!(PathTools::extname("/path.to/.file"), "");
        assert_eq!(PathTools::extname("/path.to/.file.ext"), ".ext");
        assert_eq!(PathTools::extname("/path/to/f.ext"), ".ext");
        assert_eq!(PathTools::extname("/path/to/..ext"), ".ext");
        assert_eq!(PathTools::extname("/path/to/.."), "");
        assert_eq!(PathTools::extname("file"), "");
        assert_eq!(PathTools::extname("file.ext"), ".ext");
        assert_eq!(PathTools::extname(".file"), "");
        assert_eq!(PathTools::extname(".file.ext"), ".ext");
        assert_eq!(PathTools::extname("/file"), "");
        assert_eq!(PathTools::extname("/file.ext"), ".ext");
        assert_eq!(PathTools::extname("/.file"), "");
        assert_eq!(PathTools::extname("/.file.ext"), ".ext");
        assert_eq!(PathTools::extname(".path/file.ext"), ".ext");
        assert_eq!(PathTools::extname("file.ext.ext"), ".ext");
        assert_eq!(PathTools::extname("file."), ".");
        assert_eq!(PathTools::extname("."), "");
        assert_eq!(PathTools::extname("./"), "");
        assert_eq!(PathTools::extname(".file.ext"), ".ext");
        assert_eq!(PathTools::extname(".file"), "");
        assert_eq!(PathTools::extname(".file."), ".");
        assert_eq!(PathTools::extname(".file.."), ".");
        assert_eq!(PathTools::extname(".."), "");
        assert_eq!(PathTools::extname("../"), "");
        assert_eq!(PathTools::extname("..file.ext"), ".ext");
        assert_eq!(PathTools::extname("..file"), ".file");
        assert_eq!(PathTools::extname("..file."), ".");
        assert_eq!(PathTools::extname("..file.."), ".");
        assert_eq!(PathTools::extname("..."), ".");
        assert_eq!(PathTools::extname("...ext"), ".ext");
        assert_eq!(PathTools::extname("...."), ".");
        assert_eq!(PathTools::extname("file.ext/"), ".ext");
        assert_eq!(PathTools::extname("file.ext//"), ".ext");
        assert_eq!(PathTools::extname("file/"), "");
        assert_eq!(PathTools::extname("file//"), "");
        assert_eq!(PathTools::extname("file./"), ".");
        assert_eq!(PathTools::extname("file.//"), ".");
        assert_eq!(PathTools::extname(".\\"), "");
        assert_eq!(PathTools::extname("..\\"), ".\\");
        assert_eq!(PathTools::extname("file.ext\\"), ".ext\\");
        assert_eq!(PathTools::extname("file.ext\\\\"), ".ext\\\\");
        assert_eq!(PathTools::extname("file\\"), "");
        assert_eq!(PathTools::extname("file.\\"), ".\\");
        assert_eq!(PathTools::extname("file.\\\\"), ".\\\\");
    }

    #[test]
    fn dirname_test() {
        assert_eq!("/a", PathTools::dirname("/a/b/"));
        assert_eq!("/a", PathTools::dirname("/a/b"));
        assert_eq!("/", PathTools::dirname("/a"));
        assert_eq!(".", PathTools::dirname(""));
        assert_eq!("/", PathTools::dirname("/"));
        assert_eq!("/", PathTools::dirname("////"));
        assert_eq!("//", PathTools::dirname("//a"));
        assert_eq!(".", PathTools::dirname("foo"));
    }

    #[test]
    fn relative_test() {
        assert_eq!("..", PathTools::relative("/var/lib", "/var"));
        assert_eq!("../../bin", PathTools::relative("/var/lib", "/bin"));
        assert_eq!("", PathTools::relative("/var/lib", "/var/lib"));
        assert_eq!("../apache", PathTools::relative("/var/lib", "/var/apache"));
        assert_eq!("lib", PathTools::relative("/var/", "/var/lib"));
        assert_eq!("var/lib", PathTools::relative("/", "/var/lib",));
        assert_eq!(
            "bar/package.json",
            PathTools::relative("/foo/test", "/foo/test/bar/package.json")
        );
        assert_eq!(
            "../..",
            PathTools::relative("/Users/a/web/b/test/mails", "/Users/a/web/b")
        );
        assert_eq!(
            "../baz",
            PathTools::relative("/foo/bar/baz-quux", "/foo/bar/baz")
        );
        assert_eq!(
            "../baz-quux",
            PathTools::relative("/foo/bar/baz", "/foo/bar/baz-quux")
        );
        assert_eq!("../baz", PathTools::relative("/baz-quux", "/baz"));
        assert_eq!("../baz-quux", PathTools::relative("/baz", "/baz-quux"));
        assert_eq!("../../..", PathTools::relative("/page1/page2/foo", "/"));
    }
}
