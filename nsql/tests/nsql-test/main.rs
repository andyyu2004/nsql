use expect_test::{expect, Expect};
use nsql::Nsql;

async fn test(query: &str, expect: Expect) -> nsql::Result<()> {
    let nsql = Nsql::mem().await?;
    let conn = nsql.connect();
    let result = conn.query(query).await?;
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0].len(), 1);

    expect.assert_eq(&result.tuples[0].to_string());
    Ok(())
}

#[tokio::test]
async fn test_explain() -> nsql::Result<()> {
    test(
        "EXPLAIN SELECT 1",
        expect![[r#"
        +-----------------+
        | plan            |
        |-----------------|
        | PhysicalProject |
        |   projection:   |
        |     1           |
        |   source:       |
        |     PhysicalOne |
        +-----------------+"#]],
    )
    .await
}
