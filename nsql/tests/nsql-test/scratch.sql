CREATE TABLE integers(i integer, id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY);
INSERT INTO integers VALUES (0), (1), (2), (NULL);
SELECT i, (SELECT COUNT(*) FROM integers WHERE i = i1.i) AS j FROM integers i1;
