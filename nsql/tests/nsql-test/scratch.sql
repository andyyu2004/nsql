CREATE TABLE integers(i integer, id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY);
INSERT INTO integers VALUES (0), (1), (2), (NULL);
SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>2) FROM integers i1 ORDER BY i;
