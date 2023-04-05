-- Tests for delta_delta.
-- start_ignore
create extension if not exists matrixts;
create extension if not exists mars;
-- end_ignore

-- group col
-- int4
CREATE TABLE dd_mars_int4_1col (i4 int4, ts timestamp)
USING mars WITH (group_col_ = "{i4}", group_param_col_ = "{ts in 1 day}")
DISTRIBUTED masteronly;

INSERT INTO dd_mars_int4_1col SELECT i/100, '2020-10-10 09:01:02'::timestamp + i * interval '1 second' from generate_series(1, 100000, 3)i;
SELECT SUM(i4) FROM dd_mars_int4_1col ;
SET mars.enable_customscan TO off;
SELECT SUM(i4) FROM dd_mars_int4_1col ;
SET mars.enable_customscan TO on;

--int8
CREATE TABLE dd_mars_int8_1col (i8 int8, ts timestamp)
USING mars WITH (group_col_ = "{i8}", group_param_col_ = "{ts in 1 day}")
DISTRIBUTED masteronly;

INSERT INTO dd_mars_int8_1col SELECT i/100, '2020-10-10 09:01:02'::timestamp + i * interval '1 second' from generate_series(1, 100000, 3)i;
SELECT SUM(i8) FROM dd_mars_int8_1col ;
SET mars.enable_customscan TO off;
SELECT SUM(i8) FROM dd_mars_int8_1col ;
SET mars.enable_customscan TO on;

-- group bucket col
-- int4
CREATE TABLE dd_mars_int4_bucket_1col (i4 int4, ts timestamp)
USING mars WITH (group_param_col_ = "{i4 in 10, ts in 1 day}")
DISTRIBUTED masteronly;

INSERT INTO dd_mars_int4_bucket_1col SELECT i/100, '2020-10-10 09:01:02'::timestamp + i * interval '1 second' from generate_series(1, 100000, 3)i;
SELECT SUM(i4) FROM dd_mars_int4_bucket_1col ;
SET mars.enable_customscan TO off;
SELECT SUM(i4) FROM dd_mars_int4_bucket_1col ;
SET mars.enable_customscan TO on;

-- int8
CREATE TABLE dd_mars_int8_bucket_1col (i8 int, ts timestamp)
USING mars WITH (group_param_col_ = "{i8 in 10, ts in 1 day}")
DISTRIBUTED masteronly;

INSERT INTO dd_mars_int8_bucket_1col SELECT i/100, '2020-10-10 09:01:02'::timestamp + i * interval '1 second' from generate_series(1, 100000, 3)i;
SELECT SUM(i8) FROM dd_mars_int8_bucket_1col ;
SET mars.enable_customscan TO off;
SELECT SUM(i8) FROM dd_mars_int8_bucket_1col ;
SET mars.enable_customscan TO on;


\set int4_min 'x''80000000''::int4'
\set int4_max 'x''7fffffff''::int4'

\set int8_min 'x''8000000000000000''::int8'
\set int8_max 'x''7fffffffffffffff''::int8'
SET mars.enable_customscan TO off;

-- int4 min max.
CREATE TABLE int4_min_max(c1 int4);
INSERT INTO int4_min_max
    SELECT
        CASE
            WHEN i%2=1 THEN :int4_min
            ELSE :int4_max
        END
    FROM
        generate_series(1, 100000)i;
SELECT SUM(c1) FROM int4_min_max;

-- int8 min max.
CREATE TABLE int8_min_max(c1 int8);
INSERT INTO int8_min_max
    SELECT
        CASE
            WHEN i%2=1 THEN :int8_min
            ELSE :int8_max
        END
    FROM
        generate_series(1, 100000)i;
SELECT SUM(c1) FROM int8_min_max;

-- compare with heap
CREATE TABLE heap_correct_64_result(
    c64 int8,
    c256 int8,
    c2048 int8,
    cint4 int8,
    cint8 int8)
 DISTRIBUTED masteronly;

CREATE TABLE ddelta_mars_result(
    c64 int8,
    c256 int8,
    c2048 int8,
    cint4 int8,
    cint8 int8)
USING mars
DISTRIBUTED masteronly;

CREATE TABLE ddelta_compress_mars_result(
    c64 int8 ENCODING (compresstype = lz4),
    c256 int8 ENCODING (compresstype = lz4),
    c2048 int8 ENCODING (compresstype = lz4),
    cint4 int8 ENCODING (compresstype = lz4),
    cint8 int8 ENCODING (compresstype = lz4))
USING mars
DISTRIBUTED masteronly;

INSERT INTO heap_correct_64_result
    SELECT
        64 * (random() - 0.5),
        256 * (random() - 0.5),
        2048 * (random() - 0.5),
        :int4_max * (random() - 0.5) * 2,
        :int8_max * (random() - 0.5) * 2
    FROM generate_series(1, 1000000);

INSERT INTO ddelta_mars_result
    SELECT * from heap_correct_64_result;

INSERT INTO ddelta_compress_mars_result
    SELECT * from heap_correct_64_result;

-- start_ignore
\dt+ heap_correct_64_result
\dt+ ddelta_mars_result
\dt+ ddelta_compress_mars_result
-- end_ignore
SELECT
    (SELECT sum(c64) FROM ddelta_mars_result)
    =
    (SELECT sum(c64) FROM heap_correct_64_result),
    (SELECT sum(c256) FROM ddelta_mars_result)
    =
    (SELECT sum(c256) FROM heap_correct_64_result),
    (SELECT sum(c2048) FROM ddelta_mars_result)
    =
    (SELECT sum(c2048) FROM heap_correct_64_result),
    (SELECT sum(cint4) FROM ddelta_mars_result)
    =
    (SELECT sum(cint4) FROM heap_correct_64_result),
    (SELECT sum(cint8) FROM ddelta_mars_result)
    =
    (SELECT sum(cint8) FROM heap_correct_64_result);

-- timestamp
CREATE TABLE heap_time ( c1 timestamp)
distributed masteronly;

CREATE TABLE dd_time ( c1 timestamp)
USING mars
distributed masteronly;

-- timestamptz
CREATE TABLE dd_timetz(c1 timestamptz)
USING mars
distributed masteronly;

CREATE TABLE dd_compress_time ( c1 timestamp ENCODING (compresstype=lz4))
USING mars
distributed masteronly;

-- second gap
INSERT INTO heap_time
SELECT '2021-08-09 14:09:05+08'::timestamp + (((-1^i) * i) || 'second') :: INTERVAL
FROM generate_series(1, 100000)i;
INSERT INTO dd_time SELECT * FROM heap_time;
INSERT INTO dd_timetz SELECT * FROM heap_time;
INSERT INTO dd_compress_time SELECT * FROM heap_time;

SELECT * FROM dd_time LIMIT 20;
SELECT * FROM dd_timetz LIMIT 20;
SELECT * FROM dd_compress_time LIMIT 20;

TRUNCATE heap_time;
TRUNCATE dd_time;
TRUNCATE dd_timetz;
TRUNCATE dd_compress_time;

-- minute gap
INSERT INTO heap_time
SELECT '2021-08-09 14:09:05+08'::timestamp + (((-1^i) * i) || 'minutes') :: INTERVAL
FROM generate_series(1, 100000)i;
INSERT INTO dd_time SELECT * FROM heap_time;
INSERT INTO dd_timetz SELECT * FROM heap_time;
INSERT INTO dd_compress_time SELECT * FROM heap_time;

SELECT * FROM dd_time LIMIT 20;
SELECT * FROM dd_timetz LIMIT 20;
SELECT * FROM dd_compress_time LIMIT 20;

TRUNCATE heap_time;
TRUNCATE dd_time;
TRUNCATE dd_timetz;
TRUNCATE dd_compress_time;


-- hours gap
INSERT INTO heap_time
SELECT '2021-08-09 14:09:05+08'::timestamp + (((-1^i) * i) || 'hours') :: INTERVAL
FROM generate_series(1, 100000)i;
INSERT INTO dd_time SELECT * FROM heap_time;
INSERT INTO dd_timetz SELECT * FROM heap_time;
INSERT INTO dd_compress_time SELECT * FROM heap_time;

SELECT * FROM dd_time LIMIT 20;
SELECT * FROM dd_timetz LIMIT 20;
SELECT * FROM dd_compress_time LIMIT 20;

TRUNCATE heap_time;
TRUNCATE dd_time;
TRUNCATE dd_timetz;
TRUNCATE dd_compress_time;
