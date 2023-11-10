CREATE TABLE integers (
    i integer,
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY
);

INSERT INTO integers
    VALUES (0),
    (1),
    (2),
    (NULL);

SELECT
    i,
    EXISTS (
        SELECT
            i
        FROM
            integers
        WHERE
            i = i1.i)
FROM
    integers i1
