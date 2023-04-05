--
-- cleanup
--

-- start_ignore
drop operator class if exists int4_test_ops using btree;
drop schema if exists test_reorder;
-- end_ignore

--
-- setup
--

-- start_ignore
create extension if not exists gp_debug_numsegments;
-- end_ignore
select gp_debug_set_create_table_default_numsegments(1);

create schema test_reorder;
set search_path to test_reorder,public;

-- an alternative btree int opclass, for testing purpose
create operator class int4_test_ops for type int using btree
    as function 1 (int,int) btint4cmp(int,int);

--
-- reorder tests
--
-- some tricks:
-- - tables are created on seg0 only, so SELECT output are unrelative to the
--   number of segments;
-- - SELECT is used to verify the data order, an "order by true" clause is
--   appended to let the regress framework do not sort the output implicitly;
-- - SELECT with both seqscan and indexscan to verify the order;
--

-- common setting
\set nrows 10

-- heap + btree
\set tname 'heap_btree'
\set topts
\set tparts
\set itype 'btree'
\set iopts
\i sql/reorder.template

-- negative: reorder does not support expression index keys
create index negative_expr_idx on :tname (abs(c1));
select * from reorder_by_index('negative_expr_idx');

-- negative: reorder does not support non-default operator classes
create index negative_opclass_idx on :tname (c1 int4_test_ops);
select * from reorder_by_index('negative_opclass_idx');

-- ao table + btree
\set tname 'ao_toin'
\set topts 'with (appendonly=true, orientation=row, compresstype=zlib)'
\set tparts
\set itype 'btree'
\set iopts
\i sql/reorder.template

-- aocs table + btree
\set tname 'aocs_toin'
\set topts 'with (appendonly=true, orientation=column, compresstype=zlib)'
\set tparts
\set itype 'btree'
\set iopts
\i sql/reorder.template

-- partitioned ao table + btree
\set tname 'parted_ao_toin'
\set topts 'with (appendonly=true, orientation=row, compresstype=zlib)'
\set tparts 'partition by range(c1) (start(0) end(' :nrows ') every(1), default partition extra)'
\set itype 'btree'
\set iopts
\i sql/reorder.template

-- partitioned aocs table + btree
\set tname 'parted_aocs_toin'
\set topts 'with (appendonly=true, orientation=column, compresstype=zlib)'
\set tparts 'partition by range(c1) (start(0) end(' :nrows ') every(1), default partition extra)'
\set itype 'btree'
\set iopts
\i sql/reorder.template

-- heap + toin
\set tname 'heap_toin'
\set topts
\set tparts
\set itype 'toin'
\set iopts 'with (pages_per_range=1)'
\i sql/reorder.template

-- partitioned heap table + toin
\set tname 'parted_heap_toin'
\set topts
\set tparts 'partition by range(c1) (start(0) end(' :nrows ') every(1), default partition extra)'
\set itype 'toin'
\set iopts 'with (pages_per_range=1)'
\i sql/reorder.template

-- extra test for randomly distributed table
create table heap_random (like heap_btree) distributed randomly;
insert into heap_random select * from heap_btree;
create index heap_random_idx on heap_random using btree
     ( c1
     , c2 desc
     , c3 asc nulls first
     , c4 asc nulls last
     , c5 desc nulls first
     , c6 desc nulls last
     )
;
select * from reorder_by_index('heap_random_idx');
select * from heap_random;

-- extra test for replicated distributed table
create table heap_replicated (like heap_btree) distributed replicated;
insert into heap_replicated select * from heap_btree;
create index heap_replicated_idx on heap_replicated using btree
     ( c1
     , c2 desc
     , c3 asc nulls first
     , c4 asc nulls last
     , c5 desc nulls first
     , c6 desc nulls last
     )
;
select * from reorder_by_index('heap_replicated_idx');
select * from heap_replicated;

-- non-seq scans are disabled during the reorder function, but they should be
-- restored automatically.
show enable_seqscan;
show enable_indexscan;
show enable_indexonlyscan;
show enable_bitmapscan;
-- they should be restored even if on exceptions.
select reorder_by_index('pg_class_oid_index');
show enable_seqscan;
show enable_indexscan;
show enable_indexonlyscan;
show enable_bitmapscan;
