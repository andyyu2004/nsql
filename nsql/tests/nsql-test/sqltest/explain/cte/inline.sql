EXPLAIN WITH t AS NOT MATERIALIZED (
    SELECT
        1
)
SELECT
    *
FROM
    t;
