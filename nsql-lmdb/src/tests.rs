use crate::*;

#[test]
fn test() -> Result<()> {
    let db = LmdbStorageEngine::open("test.mdb")?;
    let mut tx = db.begin()?;
    tx.put(b"hello", b"world")?;
    tx.put(b"hello2", b"world2")?;
    assert_eq!(tx.get(b"hello")?, Some(&b"world"[..]));
    Ok(())
}
