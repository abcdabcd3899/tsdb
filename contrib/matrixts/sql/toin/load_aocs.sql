--
-- load data
--
-- the toin/setup.sql must be executed before us
--

set search_path to test_toin;

-- start_ignore
drop table if exists t_data;
-- end_ignore

create table t_data (c1 int, c2 int)
  with (appendonly=true, orientation=column, compresstype=zlib, parallel_workers=2)
  distributed by (c1);

insert into t_data select * from t_data_template;
