\set tname test_mars_mergescan_plain
 \set tagtype int
 \set with
 \i sql/mergescan.load.template

\set tname test_mars_mergescan_group_tag
 \set tagtype int
 \set with 'with (group_col_ = "{tag}")'
 \i sql/mergescan.load.template

\set tname test_mars_mergescan_group_tag_ts
 \set tagtype int
 \set with 'with (group_col_ = "{tag}", group_param_col_ = "{ts in 20s}")'
 \i sql/mergescan.load.template

-- bfv: filters on non-groupkeys should not affect the mergescan
\set tname test_mars_mergescan_bfv_t1
create table :tname (tag int, c1 int, c2 int)
 using mars with (group_col_ = '{tag}')
 distributed masteronly;
insert into :tname values (1, 10, 100);
insert into :tname values (1, 11, null);
select ctid, * from :tname;
-- should be empty
select ctid, * from :tname where c1 = 10;
-- should be the merged result
select ctid, * from :tname where c1 = 11;

-- vi: et :
