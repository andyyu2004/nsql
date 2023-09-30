CREATE TABLE t1 (
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY, -- nsql-only
    a integer,
    b integer,
    c integer,
    d integer,
    e integer
);

INSERT INTO t1 (e, c, b, d, a)
    VALUES (NULL, 102, NULL, 101, 104);

INSERT INTO t1 (a, c, d, e, b)
    VALUES (107, 106, 108, 109, 105);

INSERT INTO t1 (e, d, b, a, c)
    VALUES (110, 114, 112, NULL, 113);

INSERT INTO t1 (d, c, e, a, b)
    VALUES (116, 119, 117, 115, NULL);

INSERT INTO t1 (c, d, b, e, a)
    VALUES (123, 122, 124, NULL, 121);

INSERT INTO t1 (a, d, b, e, c)
    VALUES (127, 128, 129, 126, 125);

INSERT INTO t1 (e, c, a, d, b)
    VALUES (132, 134, 131, 133, 130);

INSERT INTO t1 (a, d, b, e, c)
    VALUES (138, 136, 139, 135, 137);

INSERT INTO t1 (e, c, d, a, b)
    VALUES (144, 141, 140, 142, 143);

INSERT INTO t1 (b, a, e, d, c)
    VALUES (145, 149, 146, NULL, 147);

INSERT INTO t1 (b, c, a, d, e)
    VALUES (151, 150, 153, NULL, NULL);

INSERT INTO t1 (c, e, a, d, b)
    VALUES (155, 157, 159, NULL, 158);

INSERT INTO t1 (c, b, a, d, e)
    VALUES (161, 160, 163, 164, 162);

INSERT INTO t1 (b, d, a, e, c)
    VALUES (167, NULL, 168, 165, 166);

INSERT INTO t1 (d, b, c, e, a)
    VALUES (171, 170, 172, 173, 174);

INSERT INTO t1 (e, c, a, d, b)
    VALUES (177, 176, 179, NULL, 175);

INSERT INTO t1 (b, e, a, d, c)
    VALUES (181, 180, 182, 183, 184);

INSERT INTO t1 (c, a, b, e, d)
    VALUES (187, 188, 186, 189, 185);

INSERT INTO t1 (d, b, c, e, a)
    VALUES (190, 194, 193, 192, 191);

INSERT INTO t1 (a, e, b, d, c)
    VALUES (199, 197, 198, 196, 195);

INSERT INTO t1 (b, c, d, a, e)
    VALUES (NULL, 202, 203, 201, 204);

INSERT INTO t1 (c, e, a, b, d)
    VALUES (208, NULL, NULL, 206, 207);

INSERT INTO t1 (c, e, a, d, b)
    VALUES (214, 210, 213, 212, 211);

INSERT INTO t1 (b, c, a, d, e)
    VALUES (218, 215, 216, 217, 219);

INSERT INTO t1 (b, e, d, a, c)
    VALUES (223, 221, 222, 220, 224);

INSERT INTO t1 (d, e, b, a, c)
    VALUES (226, 227, 228, 229, 225);

INSERT INTO t1 (a, c, b, e, d)
    VALUES (234, 231, 232, 230, 233);

INSERT INTO t1 (e, b, a, c, d)
    VALUES (237, 236, 239, NULL, 238);

INSERT INTO t1 (e, c, b, a, d)
    VALUES (NULL, 244, 240, 243, NULL);

INSERT INTO t1 (e, d, c, b, a)
    VALUES (246, 248, 247, 249, 245);

COPY (
    SELECT
        a,
        b,
        c,
        d,
        e,
        (
            SELECT
                1),
            (
                SELECT
                    count(*)
                FROM
                    t1 AS x
                WHERE
                    x.b < t1.b)
            FROM
                t1
            ORDER BY
                1,
                2,
                3,
                4,
                5)
    TO '/tmp/nsql.csv';

CREATE TABLE integers (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY, -- nsql-only
    i int
);

INSERT INTO integers (i)
    VALUES (0),
    (1),
    (2),
    (NULL);

-- COPY (
--     SELECT
--         (
--             SELECT
--                 1),
--             (
--                 SELECT
--                     count(*)
--                 FROM
--                     integers i1
--                 WHERE
--                     i1.i < i)
--             FROM
--                 integers TO '/tmp/nsql.csv';
