use std::io;

use crate::mem::InMemory;
use crate::storage::FileExt;

#[test]
fn test_in_memory() -> io::Result<()> {
    let mem = InMemory::new();
    assert_eq!(mem.write_at(b"hello", 0)?, 5);
    let mut buf = [0; 6];
    assert_eq!(mem.read_at(&mut buf, 0)?, 5);
    assert_eq!(&buf, b"hello\0");
    Ok(())
}
