create table t(id int primary key);
explain select * from (select * from t join t)
