use expect_test::{expect, Expect};
use nsql::Nsql;
use nsql_storage::tuple::TupleIndex;

pub fn check_explain<'a>(
    setup: impl IntoIterator<Item = &'a str>,
    query: &str,
    expect: Expect,
) -> nsql::Result<()> {
    let db_path = nsql_test::tempfile::NamedTempFile::new()?.into_temp_path();
    let nsql = Nsql::<nsql_redb::RedbStorageEngine>::create(db_path)?;
    let (conn, state) = nsql.connect();

    for sql in setup {
        conn.query(&state, sql)?;
    }

    let result = conn.query(&state, query)?;
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0].len(), 1);

    expect.assert_eq(&result.tuples[0][TupleIndex::new(0)].to_string());
    Ok(())
}

#[test]
fn test_explain() -> nsql::Result<()> {
    check_explain(
        vec!["CREATE TABLE t (id int PRIMARY KEY, b boolean)"],
        "EXPLAIN UPDATE t SET b = true WHERE b",
        expect![[r#"
            update t
              projection (t.id, true)
                filter t.b
                  scan t (id, b)
        "#]],
    )?;

    check_explain(
        vec!["CREATE TABLE t (id int PRIMARY KEY, b boolean)"],
        "EXPLAIN VERBOSE UPDATE t SET b = true WHERE b",
        expect![[r#"
            metapipeline #0
              pipeline #0
                output
                update t

                  metapipeline #1
                    pipeline #1
                      update t
                      projection (t.id, true)
                      filter t.b
                      scan t (id, b)
        "#]],
    )?;

    Ok(())
}
