SELECT * FROM ( VALUES (1), (2)) AS t(x) JOIN (SELECT 2 AS k) WHERE k = x;
-- SELECT * FROM (VALUES (1), (2)) AS t(x) JOIN (SELECT 2 AS k);


