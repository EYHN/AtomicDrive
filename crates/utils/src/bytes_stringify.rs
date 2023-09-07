pub fn bytes_stringify(bytes: &[u8]) -> String {
    let mut data = String::new();
    for char in bytes {
        let char = *char;
        if !(0x20..=0x80).contains(&char) {
            data.push_str(&format!("\\u{char:02X?}"));
        } else if char == b'"' {
            data.push_str("\\\"")
        } else if char == b'\\' {
            data.push_str("\\\\")
        } else {
            data.push(char::from_u32(char as u32).unwrap())
        }
    }
    data
}
