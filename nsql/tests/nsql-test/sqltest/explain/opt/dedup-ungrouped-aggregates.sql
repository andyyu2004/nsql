EXPLAIN
SELECT
    SUM(a),
    SUM(a),
    SUM(a)
FROM (
    VALUES (1),
        (2),
        (3)) AS t (a)
ORDER BY
    SUM(a)
