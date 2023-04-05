-- start_ignore
CREATE EXTENSION IF NOT EXISTS mars;

CREATE extension IF NOT EXISTS gp_debug_numsegments;

SELECT gp_debug_set_create_table_default_numsegments(1);
-- end_ignore

CREATE TABLE foo(ts timestamp encoding (compresstype=lz4),
                 tag int encoding (compresstype=lz4),
                 c1 text encoding (compresstype=lz4),
                 c2 float encoding (compresstype=lz4))
       using mars with (group_param_col_ = "{ts in 1 min}")
       distributed by (tag);

INSERT INTO foo SELECT '2010-01-01 00:00:00'::TIMESTAMP + ('' || i * 20)::interval, i, 'text' || i, i * 0.6 FROM generate_series(1, 10) i;

SELECT * FROM foo;

DROP TABLE foo;

CREATE TABLE foo(ts timestamp encoding (compresstype=zstd),
                 tag int encoding (compresstype=zstd),
                 c1 text encoding (compresstype=zstd),
                 c2 float encoding (compresstype=zstd))
       using mars with (group_param_col_ = "{ts in 1 min}")
       distributed by (tag);;

INSERT INTO foo SELECT '2010-01-01 00:00:00'::TIMESTAMP + ('' || i * 20)::interval, i, 'text' || i, i * 0.6 FROM generate_series(1, 10) i;

SELECT * FROM foo;

DROP TABLE foo;
