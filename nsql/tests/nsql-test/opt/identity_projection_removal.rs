use expect_test::expect;

use crate::explain::check_explain;

// #[test]
// fn test_identity_projection_removal_simple() -> nsql::Result<()> {
//     check_explain(
//         ["CREATE TABLE t (id int PRIMARY KEY)", "SET explain_output = 'all'"],
//         "EXPLAIN SELECT * FROM t JOIN t",
//         expect![[r#"
//             nested loop join (INNER JOIN)
//               scan t (id)
//               scan t (id)
//         "#]],
//     )
// }
//
// #[test]
// fn test_identity_projection_removal_nested() -> nsql::Result<()> {
//     check_explain(
//         ["CREATE TABLE t (id int PRIMARY KEY)"],
//         "EXPLAIN SELECT * FROM (SELECT * FROM t JOIN t)",
//         expect![[r#"
//             nested loop join (INNER JOIN)
//               scan t (id)
//               scan t (id)
//         "#]],
//     )
// }
//
// #[test]
// fn test_identity_projection_removal_very_nested() -> nsql::Result<()> {
//     check_explain(
//         ["CREATE TABLE t (id int PRIMARY KEY)"],
//         "EXPLAIN SELECT * FROM (SELECT * FROM (SELECT * FROM t) JOIN (SELECT * FROM t))",
//         expect![[r#"
//             nested loop join (INNER JOIN)
//               scan t (id)
//               scan t (id)
//         "#]],
//     )
// }
