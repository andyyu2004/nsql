#![allow(dead_code)]
use crate::{FallibleIterator, ReadTree, StorageEngine, WriteTransaction, WriteTree};

fn example_usage<S: StorageEngine>() -> Result<(), S::Error> {
    let s = S::open("path")?;
    let txn = s.begin_write()?;
    {
        let mut tree = s.open_write_tree(&txn, "tree")?;
        tree.insert(b"hello", b"world")?.unwrap();
        tree.insert(b"hello2", b"world2")?.unwrap();
    }

    txn.commit()?;

    let txn = s.begin()?;
    let tree = s.open_tree(&txn, "tree")?.unwrap();
    assert_eq!(tree.get(b"hello")?.as_deref(), Some(&b"world"[..]));

    let _data = tree.range(..)?.map(|(_, v)| Ok(v.to_vec())).collect::<Vec<Vec<u8>>>()?;

    use_tree(&tree)?;

    let _tree = ContainsTree::<S> { tree };

    Ok(())
}

fn use_tree<'env, 'txn, S: StorageEngine>(
    tree: &impl ReadTree<'env, 'txn, S>,
) -> Result<(), S::Error> {
    let _iter = tree.range(..)?;
    Ok(())
}

struct ContainsTree<'env: 'txn, 'txn, S: StorageEngine> {
    tree: S::ReadTree<'env, 'txn>,
}

// impl<'env: 'txn, 'txn, S: StorageEngine> ContainsTree<'env, 'txn, S> {
//     fn use_tree<'a>(
//         &'a self,
//     ) -> Result<
//         impl FallibleIterator<Item = (S::Bytes<'a>, S::Bytes<'a>), Error = S::Error> + 'env + 'a,
//         S::Error,
//     > {
//         self.tree.range(..)
//     }
// }
