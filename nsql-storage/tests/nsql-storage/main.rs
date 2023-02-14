use nsql_storage::{Result, Storage};
use proptest::prelude::*;

const SIZE: usize = 4096;

macro_rules! tmp {
    () => {
        tempfile::tempdir()?.path().join("test.db")
    };
}

macro_rules! create {
    () => {
        Storage::create(tmp!()).await?
    };
}

#[test]
fn test_new_storage() -> Result<()> {
    tokio_uring::start(async {
        let storage = create!();
        let mut buf = [0; SIZE];
        buf[0] = 1;
        storage.write_at(0, &buf).await?;

        let data = storage.read_at::<SIZE>(0).await?;
        assert_eq!(data, buf);
        Ok(())
    })
}

proptest! {
    #[test]
    fn test_read_after_write_consistency(
        page_indices in prop::collection::vec(0..10000u64, 1..100),
        buf in prop::collection::vec(0..u8::MAX, SIZE),
    ) {
        let result: Result<()> = tokio_uring::start(async {
            let storage = create!();
            let mut buf: [u8; SIZE] = buf.try_into().unwrap();

            for page_index in page_indices {
                buf[0] = page_index as u8;
                let pos = page_index * SIZE as u64;
                storage.write_at::<SIZE>(pos, &buf).await?;
                let data = storage.read_at::<SIZE>(pos).await?;
                assert_eq!(data, buf);
            }

            Ok(())
        });
        result.unwrap()
    }
}
