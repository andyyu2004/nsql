#[test]
fn smoke() -> memkv::Result<()> {
    let db = memkv::Env::open()?;
    let tx = db.begin_write();
    let table = tx.open("foo")?;
    table.insert(b"foo", b"bar");
    // assert_eq!(table.get(b"foo"), Some(&b"bar"[..]));
    Ok(())
}
