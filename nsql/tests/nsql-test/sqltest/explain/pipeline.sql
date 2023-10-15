SET explain_output = 'pipeline';

CREATE TABLE t (
    id int PRIMARY KEY,
    b boolean
);

EXPLAIN UPDATE
    t
SET
    b = TRUE
WHERE
    b;
