use nsql_storage_engine::StorageEngine;

use crate::*;

#[test]
fn test() -> Result<()> {
    let db = LmdbStorageEngine::create("test.mdb")?;
    let tx = db.begin_write()?;
    let mut tree = db.open_write_tree(&tx, "test")?;
    tree.insert(b"hello", b"world")?;
    tree.insert(b"hello2", b"world2")?;
    assert_eq!(tree.get(b"hello")?, Some(&b"world"[..]));
    Ok(())
}
