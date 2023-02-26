use super::*;
use crate::tuple::{Literal, Value};

// #[test]
// fn serde_tuple_page() -> Result<()> {
//     nsql_test::start(async {
//         let mut page = HeapTuplePage::default();
//         page.insert(Tuple::from(vec![
//             Value::Literal(Literal::Bool(true)),
//             Value::Literal(Literal::Bool(false)),
//         ]))
//         .await?;

//         let mut buf = vec![];
//         page.serialize(&mut buf).await?;

//         let actual = HeapTuplePage::deserialize(&mut &buf[..]).await?;
//         assert_eq!(page, actual);

//         Ok(())
//     })
// }
