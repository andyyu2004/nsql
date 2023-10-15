EXPLAIN ANALYZE TIMING OFF
SELECT
    *
FROM (
    VALUES (1),
        (2))
    CROSS JOIN (
        VALUES (3), (4))
