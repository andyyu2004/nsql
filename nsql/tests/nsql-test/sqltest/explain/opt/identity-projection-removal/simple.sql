CREATE TABLE t (
    id int PRIMARY KEY
);

EXPLAIN
SELECT
    *
FROM
    t
    JOIN t
