use expect_test::{expect, Expect};
use ir::Value;
use nsql::Nsql;
use nsql_storage::tuple::TupleIndex;

#[track_caller]
pub fn check_explain<'a>(
    setup: impl IntoIterator<Item = &'a str>,
    query: &str,
    expect: Expect,
) -> nsql::Result<()> {
    let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
    let nsql = Nsql::<nsql_redb::RedbStorageEngine>::create(db_path)?;
    let (conn, state) = nsql.connect();

    for sql in setup {
        conn.query(&state, sql)?;
    }

    let result = conn.query(&state, query)?;
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0].width(), 1);

    match &result.tuples[0][TupleIndex::new(0)] {
        Value::Text(plan) => expect.assert_eq(plan),
        _ => panic!("expected text output from explain"),
    }
    Ok(())
}

#[test]
fn test_explain() -> nsql::Result<()> {
    check_explain(
        ["CREATE TABLE t (id int PRIMARY KEY, b boolean)"],
        "EXPLAIN UPDATE t SET b = true WHERE b",
        expect![[r#"
            update t
              projection (t.id, true)
                filter t.b
                  scan t (id, b)
        "#]],
    )?;

    check_explain(
        ["SET LOCAL explain_output = 'pipeline'", "CREATE TABLE t (id int PRIMARY KEY, b boolean)"],
        "EXPLAIN UPDATE t SET b = true WHERE b",
        expect![[r#"
            metapipeline #0
              pipeline #0
                 node #4 output
                 node #3 update t

                  metapipeline #1
                    pipeline #1
                       node #3 update t
                       node #2 projection (t.id, true)
                       node #1 filter t.b
                       node #0 scan t (id, b)
        "#]],
    )?;

    Ok(())
}
