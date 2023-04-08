use super::BTree;

// #[test]
// fn test_mem_insert() {
//     let btree = BTree::<u32, u32, 3>::default();
//     assert!(btree.insert(1, 2).is_none());
//     assert_eq!(btree.get(&1), Some(2));
// }
//
// #[test]
// fn test_mem_insert_split() {
//     cov_mark::check!(test_mem_leaf_split);
//     let btree = BTree::<u32, u32, 3>::default();
//
//     for i in 0..=3 {
//         assert!(btree.insert(i, i).is_none());
//         assert_eq!(btree.get(&i), Some(i));
//     }
// }

// #[proptest]
// fn test_mem_insert_prop(entries: Vec<(u32, u32)>) {
//     let btree = BTree::default();
//     for (key, value) in entries {
//         assert!(btree.insert(key, value).is_none());
//         assert_eq!(btree.get(&key), Some(value));
//     }
// }
