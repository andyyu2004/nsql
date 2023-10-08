use expect_test::expect;

use crate::explain::check_explain;

#[test]
fn test_explain() -> nsql::Result<()> {
    check_explain(
        ["CREATE TABLE integers (id int PRIMARY KEY, i int)"],
        "EXPLAIN ANALYZE SELECT * FROM integers",
        expect![[r#"
            scan integers (id, i)
        "#]],
    )?;

    Ok(())
}
