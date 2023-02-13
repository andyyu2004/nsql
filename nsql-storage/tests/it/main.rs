use std::path::Path;

use nsql_storage::{Result, Storage};

const SIZE: usize = 4096;

macro_rules! tmp {
    () => {
        tempfile::tempdir()?.path().join("test.db")
    };
}

macro_rules! new {
    () => {
        Storage::new(tmp!()).await?
    };
}

async fn new(path: impl AsRef<Path>) -> Result<Storage> {
    let storage = Storage::new(path).await?;
    Ok(storage)
}

#[test]
fn test_new_storage() -> Result<()> {
    tokio_uring::start(async {
        let storage = new!();
        let mut buf = [0; SIZE];
        buf[0] = 1;
        storage.write_at(0, &buf).await?;

        let data = storage.read_at::<SIZE>(0).await?;
        assert_eq!(data, buf);
        Ok(())
    })
}
