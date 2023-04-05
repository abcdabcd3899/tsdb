-- start_ignore
CREATE EXTENSION matrixts;
SET enable_mergejoin TO off;
SET enable_nestloop TO off;
SET enable_parallel_mode TO off;
SET gp_enable_multiphase_agg TO off;
SET optimizer TO off;
-- end_ignore

-- ****************************************************
-- UDF Params Type test
-- Without 'GROUP BY' clause, 'time_bucket_gapfill()' as same as 'time_bucket()'
-- ****************************************************
-- should fail
EXPLAIN (costs off) SELECT time_bucket_gapfill(1);
SELECT time_bucket_gapfill(1);

EXPLAIN (costs off) SELECT time_bucket_gapfill(1, 1);
SELECT time_bucket_gapfill(1, 1);

EXPLAIN (costs off) SELECT time_bucket_gapfill(1, 1, 1);
SELECT time_bucket_gapfill(1, 1, 1);

EXPLAIN (costs off) SELECT time_bucket_gapfill(1, 1, 1, 1);
SELECT time_bucket_gapfill(1, 1, 1, 1);

-- should fail
EXPLAIN (costs off) SELECT time_bucket_gapfill(1, 1, 1, 1, 1);
SELECT time_bucket_gapfill(1, 1, 1, 1, 1);

-- ****************************************************
-- UDF time_bucket_gapfill only support three params struct,
-- 1. (period, table.column)
-- 2. (period, table.column, start_at)
-- 3. (period, table.column, start_at, finish_at)
-- ****************************************************

SELECT time_bucket_gapfill(
    '1 hour'::INTERVAL,
    '2020-01-01 19:35'::TIMESTAMP);

-- should fail
SELECT time_bucket_gapfill(
    '1 hour'::INTERVAL,
    '2020-01-01 19:35'::TIMESTAMP,
    '10 minutes'::INTERVAL );

SELECT time_bucket_gapfill(
    '1 hour'::INTERVAL,
    '2020-01-01 19:35'::TIMESTAMP,
    '2020-01-01 19:00'::TIMESTAMP);

SELECT time_bucket_gapfill(
    '1 hour'::INTERVAL,
    '2020-01-01 19:35'::TIMESTAMP,
    '2020-01-01 19:00'::TIMESTAMP,
    '2020-02-02 21:00'::TIMESTAMP);

CREATE TABLE tmbk (c1 INT, c2 VARCHAR, c3 TIMESTAMP ) DISTRIBUTED BY (c2);
\d+ tmbk
-- ****************************************************
-- For HASH, GROUP, PARTIAL GROUP aggregate,
-- Sort Path and CustomScan are at different place.
-- Projection order should be considered as well.
-- ****************************************************

-- ****************************************************
-- use hash agg
-- ****************************************************

SET optimizer_enable_hashagg TO ON;
SET optimizer_enable_groupagg TO OFF;
SET gp_enable_multiphase_agg TO OFF;

EXPLAIN (costs off)
SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3) AS bucket, locf(avg(c1))
FROM tmbk
GROUP BY bucket;

-- ****************************************************
-- use group agg
-- ****************************************************

SET optimizer_enable_hashagg TO OFF;
SET optimizer_enable_groupagg TO ON;
SET gp_enable_multiphase_agg TO OFF;

EXPLAIN (costs off)
SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3) AS bucket, locf(avg(c1))
FROM tmbk
GROUP BY bucket;

-- ****************************************************
-- use group partial agg
-- ****************************************************

SET optimizer_enable_hashagg TO OFF;
SET optimizer_enable_groupagg TO ON;
SET gp_enable_multiphase_agg TO ON;

EXPLAIN (costs off)
SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3) AS bucket, locf(avg(c1))
FROM tmbk
GROUP BY bucket;

-- ****************************************************
-- use group partial agg
-- ****************************************************

SET optimizer_enable_hashagg TO ON;
SET optimizer_enable_groupagg TO OFF;
SET gp_enable_multiphase_agg TO ON;

EXPLAIN (costs off)
SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3) AS bucket, locf(avg(c1))
FROM tmbk
GROUP BY bucket;

INSERT INTO tmbk VALUES ('10', '2', '2021-02-04 19:11:00');
INSERT INTO tmbk VALUES ('5', '1', '2021-02-04 19:23:00');
INSERT INTO tmbk VALUES ('3', '1', '2021-02-04 20:21:00');
INSERT INTO tmbk VALUES ('7', '3', '2021-02-04 21:13:05');
INSERT INTO tmbk VALUES ('-4', '2', '2021-02-04 21:30:00');
INSERT INTO tmbk VALUES ('4', '2', '2021-02-04 22:00:00');


SET optimizer_enable_hashagg TO ON;
SET optimizer_enable_groupagg TO ON;
SET gp_enable_multiphase_agg TO ON;

-- ****************************************************
-- test only time_bucket_gapfill
-- ****************************************************

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3) AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT avg(c1), time_bucket_gapfill('1 hour', c3, '2021-02-04 19:00', '2021-02-04 21:00') AS bucket
FROM tmbk
GROUP BY bucket;

EXPLAIN (costs off)
SELECT time_bucket_gapfill('1 hour', c3, '2021-02-04 19:00', '2021-02-04 21:00') AS bucket, avg(c1)
FROM tmbk
GROUP BY bucket;

-- ****************************************************
-- test projection order
-- ****************************************************

SELECT interpolate(avg(c1)), locf(avg(c1)), time_bucket_gapfill('10 minutes', c3) AS bucket
FROM tmbk
WHERE c3 > '2021-02-04 19:10' AND c3 < '2021-02-04 22:10' GROUP BY bucket;

SELECT locf(avg(c1)), interpolate(avg(c1)), time_bucket_gapfill('10 minutes', c3) AS bucket
FROM tmbk
WHERE c3 > '2021-02-04 19:10' AND c3 < '2021-02-04 22:10' GROUP BY bucket;

SELECT time_bucket_gapfill('10 minutes', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 > '2021-02-04 19:10' AND c3 < '2021-02-04 22:10' GROUP BY bucket;

-- ****************************************************
-- test interval scale
-- ****************************************************

SELECT time_bucket_gapfill('1 minutes', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket;

SELECT time_bucket_gapfill('1 day', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket;

SELECT time_bucket_gapfill('1 hour', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket;

SELECT time_bucket_gapfill('30 minutes', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket;

SELECT time_bucket_gapfill('3 seconds', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket;

-- ****************************************************
-- test subquery
-- ****************************************************

SELECT tb.bucket, 2*tb.i FROM (
SELECT time_bucket_gapfill('10 minutes', c3) AS bucket, locf(avg(c1)), interpolate(avg(c1)) AS i
FROM tmbk
WHERE c3 >= '2021-02-04 19:11' AND c3 <= '2021-02-04 22:00' GROUP BY bucket) AS tb;

-- ****************************************************
-- test order by multi columns
-- ****************************************************

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('1 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2  order by c2;

SELECT c2, time_bucket_gapfill('1 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2  order by c2;

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by bucket;

SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by bucket;

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by c2;

SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by c2;

INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 19:04:00');
INSERT INTO tmbk VALUES (3, '4', '2021-02-04 19:32:00');
INSERT INTO tmbk VALUES (3, '4', '2021-02-04 19:53:00');
INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 20:04:00');
INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 20:06:00');
INSERT INTO tmbk VALUES (5, '4', '2021-02-04 20:18:00');
INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 20:19:00');
INSERT INTO tmbk VALUES (5, '4', '2021-02-04 20:44:00');
INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 21:04:00');
INSERT INTO tmbk VALUES (NULL, '4', '2021-02-04 22:04:00');

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('1 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:00' and c3 <= '2021-02-04 22:00' and c2 = '4' GROUP BY bucket, c2  order by c2;

SELECT c2, time_bucket_gapfill('1 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:00' and c3 <= '2021-02-04 22:00' and c2 = '4' GROUP BY bucket, c2  ;

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by bucket;

SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by bucket;

EXPLAIN (costs off)
SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by c2;

SELECT c2, time_bucket_gapfill('30 minutes', c3) as bucket, locf(avg(c1)), interpolate(avg(c1)), interpolate(avg(c1))
FROM tmbk
where c3 >= '2021-02-04 19:11' and c3 <= '2021-02-04 22:00' GROUP BY bucket, c2 order by c2;

EXPLAIN (costs off)
SELECT c2, interpolate(avg(c1)), time_bucket_gapfill('30 minutes', c3) as bucket
FROM tmbk
where c3 >= '2021-02-04 18:00' and c3 <= '2021-02-05 01:00' and c2 = '4'
GROUP BY bucket, c2 order by c2;

SELECT c2, interpolate(avg(c1)), time_bucket_gapfill('30 minutes', c3) as bucket
FROM tmbk
where c3 >= '2021-02-04 18:00' and c3 <= '2021-02-05 01:00' and c2 = '4'
GROUP BY bucket, c2 order by c2;

CREATE TABLE tmbk_expand (int_d INT, bigint_d BIGINT, smallint_d SMALLINT, float_d FLOAT, double_d DOUBLE PRECISION, c2_1 VARCHAR, c2_2 VARCHAR, c3 TIMESTAMP ) DISTRIBUTED BY (c2_1, c2_2);
\d+ tmbk_expand

INSERT INTO tmbk_expand VALUES ('2147483647', '9223372036854775807', '127', '3.40282e+038', '1.79769e+308', '1', '2', '2021-02-04 22:00:00');
INSERT INTO tmbk_expand VALUES ('2147483647', '9223372036854775807', '127', '3.40282e+038', '1.79769e+308', '1', '2', '2021-02-04 19:00:00');
INSERT INTO tmbk_expand VALUES ('-2147483648', '-9223372036854775808', '-128', '1.17549e-038', '2.22507e-308', '1', '2', '2021-02-04 21:00:00');
INSERT INTO tmbk_expand VALUES ('2147483647', '9223372036854775807', '127', '3.40282e+038', '1.79769e+308', '2', '1', '2021-02-04 22:00:00');
INSERT INTO tmbk_expand VALUES ('2147483647', '9223372036854775807', '127', '3.40282e+038', '1.79769e+308', '2', '2', '2021-02-04 19:00:00');
INSERT INTO tmbk_expand VALUES ('2147483647', '9223372036854775807', '127', '3.40282e+038', '1.79769e+308', '2', '2', '2021-02-04 21:00:00');
INSERT INTO tmbk_expand VALUES ('-2147483648', '-9223372036854775808', '-128', '1.17549e-038', '2.22507e-308', '3', '2', '2021-02-04 21:00:00');

-- FIXME: double datatype!!
SELECT time_bucket_gapfill('30 minutes', c3) as bucket,
       locf(avg(int_d)), interpolate(avg(int_d)),
       locf(avg(bigint_d)), interpolate(avg(bigint_d)),
       locf(avg(smallint_d)), interpolate(avg(smallint_d)),
       locf(avg(float_d)), interpolate(avg(float_d)),
       locf(avg(double_d)), interpolate(avg(double_d))
FROM tmbk_expand
GROUP BY bucket;

SELECT c2_1, c2_2,
       time_bucket_gapfill('30 minutes', c3) as bucket,
       locf(avg(int_d)), interpolate(avg(int_d)),
       locf(avg(bigint_d)), interpolate(avg(bigint_d)),
       locf(avg(smallint_d)), interpolate(avg(smallint_d)),
       locf(avg(float_d)), interpolate(avg(float_d)),
       locf(avg(double_d)), interpolate(avg(double_d))
FROM tmbk_expand
GROUP BY bucket, c2_1, c2_2
ORDER BY c2_1, c2_2;

SELECT c2_1, c2_2,
       time_bucket_gapfill('30 minutes', c3) as bucket,
       locf(avg(double_d)), interpolate(avg(double_d))
FROM tmbk_expand
GROUP BY bucket, c2_1, c2_2
ORDER BY c2_1, c2_2;

-- It is correct without double datatype.

SELECT time_bucket_gapfill('30 minutes', c3) as bucket,
       locf(avg(float_d)), interpolate(avg(float_d)),
       locf(avg(float_d)), interpolate(avg(float_d)),
       locf(avg(int_d)), interpolate(avg(int_d)),
       locf(avg(bigint_d)), interpolate(avg(bigint_d))
FROM tmbk_expand
GROUP BY bucket;

-- ****************************************************
-- test parallel plan
-- ****************************************************
CREATE TABLE parallel_tmbk (c1 INT, c2 VARCHAR, c3 TIMESTAMP ) DISTRIBUTED BY (c2);
CREATE TABLE no_parallel_tmbk (c1 INT, c2 VARCHAR, c3 TIMESTAMP ) DISTRIBUTED BY (c2);
ALTER TABLE parallel_tmbk SET (parallel_workers = 2);
ALTER TABLE no_parallel_tmbk SET (parallel_workers = 0);

INSERT INTO parallel_tmbk VALUES ('10', '2', '2021-02-04 19:11:00');
INSERT INTO parallel_tmbk VALUES ('5', '1', '2021-02-04 19:23:00');
INSERT INTO parallel_tmbk VALUES ('3', '1', '2021-02-04 20:21:00');
INSERT INTO parallel_tmbk VALUES ('7', '3', '2021-02-04 21:13:05');
INSERT INTO parallel_tmbk VALUES ('-4', '2', '2021-02-04 21:30:00');
INSERT INTO parallel_tmbk VALUES ('4', '2', '2021-02-04 22:00:00');

INSERT INTO no_parallel_tmbk VALUES ('10', '2', '2021-02-04 19:11:00');
INSERT INTO no_parallel_tmbk VALUES ('5', '1', '2021-02-04 19:23:00');
INSERT INTO no_parallel_tmbk VALUES ('3', '1', '2021-02-04 20:21:00');
INSERT INTO no_parallel_tmbk VALUES ('7', '3', '2021-02-04 21:13:05');
INSERT INTO no_parallel_tmbk VALUES ('-4', '2', '2021-02-04 21:30:00');
INSERT INTO no_parallel_tmbk VALUES ('4', '2', '2021-02-04 22:00:00');
SET enable_parallel_mode TO off;

EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM parallel_tmbk
GROUP BY bucket;
SELECT locf(avg(c1)), interpolate(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM parallel_tmbk
GROUP BY bucket;

EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM no_parallel_tmbk
GROUP BY bucket;
SELECT locf(avg(c1)), interpolate(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM no_parallel_tmbk
GROUP BY bucket;

SET enable_parallel_mode TO on;

EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM parallel_tmbk
GROUP BY bucket;
SELECT locf(avg(c1)), interpolate(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM parallel_tmbk
GROUP BY bucket;

EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM no_parallel_tmbk
GROUP BY bucket;
SELECT locf(avg(c1)), interpolate(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM no_parallel_tmbk
GROUP BY bucket;

INSERT INTO parallel_tmbk SELECT i % 10000, i % 100, CURRENT_DATE + i FROM generate_series(1, 1000) i;
INSERT INTO no_parallel_tmbk SELECT i % 10000, i % 100, CURRENT_DATE + i FROM generate_series(1, 1000) i;

EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM parallel_tmbk
GROUP BY bucket;
EXPLAIN (costs off) SELECT locf(avg(c1)), time_bucket_gapfill('1 hour', c3) AS bucket
FROM no_parallel_tmbk
GROUP BY bucket;
