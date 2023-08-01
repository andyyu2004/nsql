use expect_test::expect;

use crate::explain::check_explain;

#[test]
fn test_aggregates_are_deduplicated() -> nsql::Result<()> {
    check_explain(
        [],
        "EXPLAIN SELECT SUM(a), SUM(a), SUM(a) FROM (VALUES (1), (2), (3)) AS t(a) ORDER BY SUM(a)",
        expect![[r#"
            projection (agg.sum(a), agg.sum(a), agg.sum(a))
              order
                ungrouped aggregate (sum(t.a))
                  scan values
        "#]],
    )?;

    check_explain(
        [],
        "EXPLAIN SELECT SUM(a), SUM(a), SUM(a) FROM (VALUES (1), (2), (3)) AS t(a) GROUP BY a ORDER BY SUM(a)",
        expect![[r#"
            projection (agg.sum(a), agg.sum(a), agg.sum(a))
              order
                hash aggregate (sum(t.a)) by t.a
                  scan values
        "#]],
    )
}
