use std::io;

use nsql_btree::BTree;
use nsql_test::mk_mem_buffer_pool;

#[test]
fn test() -> io::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::create(pool).await?;
        btree.insert(1, 2).await?;
        assert_eq!(btree.get(&1).await?, Some(2));
        Ok(())
    })
}
