-- ensure nodes that aren't run still have profiling information
EXPLAIN ANALYZE TIMING OFF
SELECT
    1
WHERE
    1 = 2
