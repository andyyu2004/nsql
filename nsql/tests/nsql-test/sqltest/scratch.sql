CREATE TABLE test (
    a integer PRIMARY KEY,
    b integer
);

INSERT INTO test
    VALUES (11, 22);

INSERT INTO test
    VALUES (12, 21);

INSERT INTO test
    VALUES (13, 22);

-- the bug is that the cte scope should be checked first here, I think we're only checking for ctes when
-- nothing else can be found
SELECT
    *
FROM
    test
WHERE
    a = ( WITH cte AS NOT MATERIALIZED (
            SELECT
                a
            FROM
                test t
            WHERE
                t.b = test.b
)
            SELECT
                min(a)
            FROM
                cte);

