--
-- jsonb values are auto merged during mergescan
--

\set tname test_mars_jsonb_t1

-- start_ignore
drop table if exists :tname;
-- end_ignore

-- a trick is played that all the rows are inserted to the same segment
create table :tname (tag int, kv_jsonb jsonb, kv_text text, seg int default 0)
 using mars
  with (tagkey = tag)
 distributed by (seg);

-- jsonb values can be merged during INSERT when they are in the same block
insert into :tname values
  (0, '{"a": 10, "b": 20}', '{"a": 10, "b": 20}')
, (0, '{"a": 40, "c": 30}', '{"a": 40, "c": 30}')
, (0, '{"a": 60, "b": 50}', '{"a": 60, "b": 50}')
;

-- jsonb values can be merged during SELECT
insert into :tname values (1, '{"a": 10, "b": 20}', '{"a": 10, "b": 20}');
insert into :tname values (1, '{"a": 40, "c": 30}', '{"a": 40, "c": 30}');
insert into :tname values (1, '{"a": 60, "b": 50}', '{"a": 60, "b": 50}');

-- jsonb values won't be merged if they are in different blocks
insert into :tname values (2, '{"a": 10, "b": 20}', '{"a": 10, "b": 20}');
insert into :tname values (3, '{"a": 40, "c": 30}', '{"a": 40, "c": 30}');
insert into :tname values (4, '{"a": 60, "b": 50}', '{"a": 60, "b": 50}');

-- verify the effect
select ctid, tag, kv_jsonb, kv_text from :tname;
