CREATE TABLE t (id int PRIMARY KEY);
EXPLAIN SELECT * FROM (SELECT * FROM (SELECT * FROM t) JOIN (SELECT * FROM t))