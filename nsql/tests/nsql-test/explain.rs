use expect_test::{expect, Expect};
use nsql::Nsql;
use nsql_storage::tuple::TupleIndex;

async fn check_explain(
    setup: impl IntoIterator<Item = &str>,
    query: &str,
    expect: Expect,
) -> nsql::Result<()> {
    let nsql = Nsql::mem().await?;
    let conn = nsql.connect();

    for sql in setup {
        conn.query(sql).await?;
    }

    let result = conn.query(query).await?;
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0].len(), 1);

    expect.assert_eq(&result.tuples[0][TupleIndex::new(0)].to_string());
    Ok(())
}

#[tokio::test]
async fn test_explain() -> nsql::Result<()> {
    check_explain(
        vec!["CREATE TABLE t (b boolean)"],
        "EXPLAIN UPDATE t SET b = true WHERE b",
        expect![[r#"
            update t
              projection (true, tid)
                filter
                  scan t
        "#]],
    )
    .await?;

    check_explain(
        vec!["CREATE TABLE t (b boolean)"],
        "EXPLAIN VERBOSE UPDATE t SET b = true WHERE b",
        expect![[r#"
            metapipeline #0
              pipeline #0
                output
                update t

                  metapipeline #1
                    pipeline #1
                      update t
                      projection (true, tid)
                      filter
                      scan t
        "#]],
    )
    .await?;

    Ok(())
}
