use nsql_storage::Result;
use nsql_test::mk_storage;
use proptest::collection::vec;
use test_strategy::proptest;

const SIZE: usize = 4096;

#[test]
fn test_new_storage() -> Result<()> {
    tokio_uring::start(async {
        let storage = mk_storage!();
        let mut buf = [0; SIZE];
        buf[0] = 1;
        storage.write_at(0, &buf).await?;

        let data = storage.read_at::<SIZE>(0).await?;
        assert_eq!(data, buf);
        Ok(())
    })
}

#[proptest]
fn test_read_after_write_consistency(
    #[strategy(vec(0..10000u64, 1..100))] page_indices: Vec<u64>,
    #[strategy(vec(0..u8::MAX, SIZE))] buf: Vec<u8>,
) {
    let result: Result<()> = tokio_uring::start(async {
        let storage = mk_storage!();
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
