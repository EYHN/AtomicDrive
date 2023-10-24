/// store file system level file identifier, e.g. inode number in linux, file_id
/// in windows.
///
/// https://man7.org/linux/man-pages/man7/inode.7.html
/// https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info
///
/// # Behavior
/// If the tracker finds a change in the file marker at the same location,
/// the existing file is deemed deleted, and a new file is established.
/// If the file marker already exists at another location,
/// the file is moved to the current location instead of being established.
/// This is the main way the tracker detects file movement.
/// 
/// If the file marker is supplied as empty, the tracker makes no judgment about
/// the file marker.
/// 
/// The tracker treats the file marker as a unique identifier for the node in
/// the file tree, and since the file may be hardlinked, the file marker should
/// be empty for the file.
pub type FileMarker = Vec<u8>;

/// A marker used to identify the file type.
///
/// # Behavior
/// If the tracker detects a change in the file type identifier at the same
/// location, the existing file is deemed deleted, and a new file is established.
///
/// Since the tracker is indifferent to the actual file type, it's represented
/// as Vec<u8>, allowing it to store any value.
pub type FileTypeMarker = Vec<u8>;

/// Store information about whether the file is updated.
/// Usually is a combination of file mtime and size.
pub type FileUpdateMarker = Vec<u8>;
