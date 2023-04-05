-- start_ignore
create extension if not exists matrixts;
-- end_ignore

-----------------------------------------------------------------------
-- Create Table
-----------------------------------------------------------------------
create table sheap (segid int, tagid int, tagname text,
					ts timestamp without time zone,
					i1 double precision) using sortheap;
-- sortheap_btree is a must.
insert into sheap select i, i, 'device' || i, now(), i % 100.0 from generate_series(1, 100) i;
-- Only sortheap_btree index is allowed
create index idx_sheap on sheap using btree(tagid, time_bucket('1 hour', ts));
-- Only one index is allowed
begin;
create index idx_sheap on sheap using sortheap_btree(tagid, time_bucket('1 hour', ts));
create index idx1_sheap on sheap using sortheap_btree(tagname, time_bucket('1 hour', ts));
abort;
-- Combination of sort keys
-- order by text: tagname
begin;
create index idx_sheap on sheap using sortheap_btree(tagname);
insert into sheap values (0, 2, 'device2', '2020-06-15 12:00:00', 90),
						 (0, 1, 'device1', '2020-06-15 16:00:00', 70);
select * from sheap; 
abort;
-- order by tagid, tagname
begin;
create index idx_sheap on sheap using sortheap_btree(tagid, tagname);
insert into sheap values (0, 2, 'device2', '2020-06-15 12:00:00', 90),
						 (0, 1, 'device1', '2020-06-15 16:00:00', 70);
select * from sheap; 
abort;
-- order by time_bucket(1 hour), tagid, tagname
begin;
create index idx_sheap on sheap using sortheap_btree(time_bucket('1 hour', ts), tagid, tagname);
insert into sheap values (0, 2, 'devie2', '2020-06-15 12:00:00', 80),
                         (0, 1, 'devie1', '2020-06-15 16:00:00', 60);
select * from sheap; 
abort;
-- order by time_bucket(12 hour), tagid, tagname
begin;
create index idx_sheap on sheap using sortheap_btree(time_bucket('12 hour', ts), tagid, tagname);
insert into sheap values (0, 2, 'device2', '2020-06-15 12:00:00', 50),
                         (0, 1, 'device1', '2020-06-15 16:00:00', 540);
select * from sheap; 
abort;
-- order by mod() 
begin;
create index idx_sheap on sheap using sortheap_btree(mod(tagid, 10));
insert into sheap values (0, 8, 'device2', '2020-06-15 12:00:00', 333),
                         (0, 2, 'device1', '2020-06-15 16:00:00', 444);
select * from sheap; 
abort;
-- order by four attrs (the max cached sort key is three) 
begin;
create index idx_sheap on sheap using sortheap_btree(ts, tagid, tagname, i1);
insert into sheap values (0, 1, 'device1', '2020-06-15 12:00:00', 888),
                         (0, 1, 'device1', '2020-06-15 12:00:00', 444);
select * from sheap; 
abort;

-----------------------------------------------------------------------
-- Index scan
-----------------------------------------------------------------------
drop table sheap;
create table sheap
(
	tagid int, ts timestamp without time zone,
	i1 double precision, i2 double precision, i3 double precision
) using sortheap;

create index idx_sheap on sheap using sortheap_btree(tagid, time_bucket('1 hour', ts));
insert into sheap
select
	i % 20,
	'2020-06-15 12:00:00'::timestamp + (i || ' mins')::interval,
	i / 100.0,
	i / 10.0,
	i / 1000.0
from generate_series(1, 200) i;
ANALYZE sheap;

-- Btree index scan
set enable_bitmapscan to off;
set enable_seqscan to off;
select * from sheap where tagid = 10;
explain (costs off) select * from sheap where tagid = 10;
select * from sheap where tagid in (10, 19);
explain (costs off) select * from sheap where tagid in (10, 19);
select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
explain (costs off) select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
select count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
select tagid, count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
-- Btree index scan used as auxiliary estate
set enable_seqscan to on;
set random_page_cost to 10000;
select * from sheap where tagid = 10;
explain (costs off) select * from sheap where tagid = 10;
select * from sheap where tagid in (10, 19);
explain (costs off) select * from sheap where tagid in (10, 19);
select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
explain (costs off) select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
select * from sheap where tagid = 10 and i3 = 0.07;
explain (costs off) select * from sheap where tagid = 10 and i3 = 0.07;
select count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
select tagid, count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;

reset random_page_cost;
reset enable_bitmapscan;
reset enable_seqscan;

-- Bitmap index scan
set enable_indexscan to off;
set enable_seqscan to off;
select * from sheap where tagid = 10;
explain (costs off) select * from sheap where tagid = 10;
select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
explain select * from sheap where tagid = 10 and time_bucket('1 hour', ts) = '2020-06-15 13:00:00';
select * from sheap where tagid in (10, 19);
explain (costs off) select * from sheap where tagid in (10, 19);
select * from sheap where tagid = 10 or tagid = 19;
explain (costs off) select * from sheap where tagid = 10 or tagid = 19;
select count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
select tagid, count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;

-- Bitmap index scan cannot used as auxiliary estate
set enable_seqscan to on;
set random_page_cost to 10000;
select * from sheap where tagid = 10;
explain (costs off) select * from sheap where tagid = 10;
select * from sheap where tagid in (10, 19);
explain (costs off) select * from sheap where tagid in (10, 19);
select * from sheap where tagid = 10 or tagid = 19;
explain (costs off) select * from sheap where tagid = 10 or tagid = 19;
select count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
select tagid, count(*), min(i1), max(i2) from sheap where tagid IN (10, 19) and ts >= '2020-06-15 13:00:00' group by tagid;
reset random_page_cost;
reset enable_indexscan;
reset enable_seqscan;

-- Secondary filter query using Brin
select count(*) from sheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00';
explain (costs off) select count(*) from sheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00';
select * from sheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00' and i3 = 0.001;
explain (costs off) select * from sheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00' and i3 = 0.001;
select count(*), min(i1), max(i2), time_bucket('1 hour', ts) as hour from sheap where time_bucket('1 hour', ts) = '2020-06-15 13:00:00' group by hour;
explain select count(*), min(i1), max(i2), time_bucket('1 hour', ts) as hour from sheap where time_bucket('1 hour', ts) = '2020-06-15 13:00:00' group by hour;

-----------------------------------------------------------------------
-- Merge & Vacuum & Vacuum Full
-----------------------------------------------------------------------
VACUUM sheap;
ANALYZE sheap;
VACUUM FULL sheap;
ANALYZE sheap;
select * from sheap where tagid = 10;

----------------------------------------------------------------------
-- Custom Scan
----------------------------------------------------------------------
-- Group Aggregate without sort
set enable_hashagg to off;
select count(*) from sheap where tagid = 10 group by tagid, time_bucket('1 hour', ts);
explain (costs off) select count(*) from sheap where tagid = 10 group by tagid, time_bucket('1 hour', ts);
select count(*) from sheap group by time_bucket('1 hour', ts), tagid;
explain (costs off) select count(*) from sheap group by time_bucket('1 hour', ts), tagid;
-- Order by limit without sort
select * from sheap order by tagid limit 1;
explain (costs off) select * from sheap order by tagid limit 1;
select * from sheap order by tagid, time_bucket('1 hour', ts) limit 1;
explain (costs off) select * from sheap order by tagid, time_bucket('1 hour', ts) limit 1;
-- Plain query use the original seqscan
select count(*) from sheap;
explain (costs off) select count(*) from sheap;
-- Index path has no order
select time_bucket('1 hour', ts) as hour from sheap where tagid = 10 order by hour limit 1;
explain (costs off) select time_bucket('1 hour', ts) as hour from sheap where tagid = 10 order by hour limit 1;

reset enable_hashagg;

-- NestLoop
set enable_bitmapscan to off;
set enable_mergejoin to off;
set enable_hashjoin to off;
set enable_seqscan to off;

-- nestloop with index scan
select * from sheap inner join generate_series(0, 1) i on (sheap.tagid = i);
explain (costs off)
select * from sheap inner join generate_series(0, 1) i on (sheap.tagid = i);
-- nestloop with seqscan with auxiliary index scan
set enable_seqscan to on;
set enable_indexscan to off;
set enable_material to off;
select count(*) from sheap inner join generate_series(0, 1000) i on (sheap.tagid = i);
explain (costs off)
select count(*) from sheap inner join generate_series(0, 1000) i on (sheap.tagid = i);

reset enable_indexscan;
reset enable_material;
reset enable_bitmapscan;
reset enable_mergejoin;
reset enable_seqscan;
reset enable_hashjoin;

-----------------------------------------------------------------------
-- Compression & Column Store
-----------------------------------------------------------------------
set sortheap_sort_mem to '64kB';
drop table sheap;
create table sheap
(
	tagid int, ts timestamp without time zone,
	i1 double precision, i2 double precision, i3 double precision
) using sortheap;

create index idx_sheap on sheap using sortheap_btree(tagid);
insert into sheap
select
	i % 20,
	'2020-06-15 12:00:00'::timestamp + (i % 7200 || ' mins')::interval,
	i / 100.0,
	i / 10.0,
	i / 1000.0
from generate_series(1, 30000) i;
ANALYZE sheap;

-- BitmapIndexScan
set enable_indexscan to off;
set enable_seqscan to off;
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
reset enable_indexscan;
reset enable_seqscan;

-- IndexScan
set enable_bitmapscan to off;
set enable_seqscan to off;
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
reset enable_bitmapscan;
reset enable_seqscan;

-- SeqScan with auxiliary indexscan
set enable_bitmapscan to off;
set enable_indexscan to off;
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00';
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
explain (costs off)
select * from sheap where tagid = 10 and ts = '2020-06-15 12:30:00' and i3 > 21;
reset enable_bitmapscan;
reset enable_indexscan;

-- Compress complex type or NULL
create table complexsheap (
	tagid int,
	c1 text,
	c2 varchar,
	c3 int[],
	c4 text[],
	c5 float8,
	c6 float8[],
	c7 bool,
	c8 json,
	c9 point,
	c10 money,
	c11 inet,
	c12 int[][],
	cnull int
) using sortheap;

create index idx_complexsheap on complexsheap using sortheap_btree(tagid);

-- cnull is null
insert into complexsheap select i % 2, 'texttype', 'varchartype', '{1, 2, 3}', '{"text1", "text2"}', 1.1, '{1.1, 2.2}', true, '{"bar": "baz", "balance":7.77, "active":false}'::json, point(1, 2), 12.34::money, '192.168.0.1/32', '{{1, 2}, {2, 3}}' from generate_series(1, 100) i;
-- cnull is not null 
insert into complexsheap select i % 2, 'texttype', 'varchartype', '{1, 2, 3}', '{"text1", "text2"}', 1.1, '{1.1, 2.2}', true, '{"bar": "baz", "balance":7.77, "active":false}'::json, point(1, 2), 12.34::money, '192.168.0.1/32', '{{1, 2}, {2, 3}}', 1 from generate_series(1, 100) i;
-- tuples should in a row-oriented format
select * from complexsheap where tagid = 0 limit 1;
select * from complexsheap where tagid = 1 limit 1;
select * from complexsheap limit 1;
insert into complexsheap select i % 2, 'texttype', 'varchartype', '{1, 2, 3}', '{"text1", "text2"}', 1.1, '{1.1, 2.2}', true, '{"bar": "baz", "balance":7.77, "active":false}'::json, point(1, 2), 12.34::money, '192.168.0.1/32', '{{1, 2}, {2, 3}}' from generate_series(1, 1000) i;
select * from complexsheap where tagid = 0 limit 1;
select * from complexsheap where tagid = 1 limit 1;
select * from complexsheap limit 1;
vacuum complexsheap;
-- tuples might be in a column-oriented format
select * from complexsheap where tagid = 0 limit 1;
select * from complexsheap where tagid = 1 limit 1;
select * from complexsheap limit 1;
-- tuples must be in a column-oriented format
vacuum FULL complexsheap;
select * from complexsheap where tagid = 0 limit 1;
select * from complexsheap where tagid = 1 limit 1;
select * from complexsheap limit 1;

-- Maximum 1600 attributes 
create table hugesheap (tagid int, ts timestamp without time zone,
c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, c31 int, c32 int, c33 int, c34 int, c35 int, c36 int, c37 int, c38 int, c39 int, c40 int, c41 int, c42 int, c43 int, c44 int, c45 int, c46 int, c47 int, c48 int, c49 int, c50 int, c51 int, c52 int, c53 int, c54 int, c55 int, c56 int, c57 int, c58 int, c59 int, c60 int, c61 int, c62 int, c63 int, c64 int, c65 int, c66 int, c67 int, c68 int, c69 int, c70 int, c71 int, c72 int, c73 int, c74 int, c75 int, c76 int, c77 int, c78 int, c79 int, c80 int, c81 int, c82 int, c83 int, c84 int, c85 int, c86 int, c87 int, c88 int, c89 int, c90 int, c91 int, c92 int, c93 int, c94 int, c95 int, c96 int, c97 int, c98 int, c99 int, c100 int, c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int, c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int, c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int, c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int, c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int, c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int, c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int, c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int, c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int, c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int, c201 int, c202 int, c203 int, c204 int, c205 int, c206 int, c207 int, c208 int, c209 int, c210 int, c211 int, c212 int, c213 int, c214 int, c215 int, c216 int, c217 int, c218 int, c219 int, c220 int, c221 int, c222 int, c223 int, c224 int, c225 int, c226 int, c227 int, c228 int, c229 int, c230 int, c231 int, c232 int, c233 int, c234 int, c235 int, c236 int, c237 int, c238 int, c239 int, c240 int, c241 int, c242 int, c243 int, c244 int, c245 int, c246 int, c247 int, c248 int, c249 int, c250 int, c251 int, c252 int, c253 int, c254 int, c255 int, c256 int, c257 int, c258 int, c259 int, c260 int, c261 int, c262 int, c263 int, c264 int, c265 int, c266 int, c267 int, c268 int, c269 int, c270 int, c271 int, c272 int, c273 int, c274 int, c275 int, c276 int, c277 int, c278 int, c279 int, c280 int, c281 int, c282 int, c283 int, c284 int, c285 int, c286 int, c287 int, c288 int, c289 int, c290 int, c291 int, c292 int, c293 int, c294 int, c295 int, c296 int, c297 int, c298 int, c299 int, c300 int, c301 int, c302 int, c303 int, c304 int, c305 int, c306 int, c307 int, c308 int, c309 int, c310 int, c311 int, c312 int, c313 int, c314 int, c315 int, c316 int, c317 int, c318 int, c319 int, c320 int, c321 int, c322 int, c323 int, c324 int, c325 int, c326 int, c327 int, c328 int, c329 int, c330 int, c331 int, c332 int, c333 int, c334 int, c335 int, c336 int, c337 int, c338 int, c339 int, c340 int, c341 int, c342 int, c343 int, c344 int, c345 int, c346 int, c347 int, c348 int, c349 int, c350 int, c351 int, c352 int, c353 int, c354 int, c355 int, c356 int, c357 int, c358 int, c359 int, c360 int, c361 int, c362 int, c363 int, c364 int, c365 int, c366 int, c367 int, c368 int, c369 int, c370 int, c371 int, c372 int, c373 int, c374 int, c375 int, c376 int, c377 int, c378 int, c379 int, c380 int, c381 int, c382 int, c383 int, c384 int, c385 int, c386 int, c387 int, c388 int, c389 int, c390 int, c391 int, c392 int, c393 int, c394 int, c395 int, c396 int, c397 int, c398 int, c399 int, c400 int, c401 int, c402 int, c403 int, c404 int, c405 int, c406 int, c407 int, c408 int, c409 int, c410 int, c411 int, c412 int, c413 int, c414 int, c415 int, c416 int, c417 int, c418 int, c419 int, c420 int, c421 int, c422 int, c423 int, c424 int, c425 int, c426 int, c427 int, c428 int, c429 int, c430 int, c431 int, c432 int, c433 int, c434 int, c435 int, c436 int, c437 int, c438 int, c439 int, c440 int, c441 int, c442 int, c443 int, c444 int, c445 int, c446 int, c447 int, c448 int, c449 int, c450 int, c451 int, c452 int, c453 int, c454 int, c455 int, c456 int, c457 int, c458 int, c459 int, c460 int, c461 int, c462 int, c463 int, c464 int, c465 int, c466 int, c467 int, c468 int, c469 int, c470 int, c471 int, c472 int, c473 int, c474 int, c475 int, c476 int, c477 int, c478 int, c479 int, c480 int, c481 int, c482 int, c483 int, c484 int, c485 int, c486 int, c487 int, c488 int, c489 int, c490 int, c491 int, c492 int, c493 int, c494 int, c495 int, c496 int, c497 int, c498 int, c499 int, c500 int, c501 int, c502 int, c503 int, c504 int, c505 int, c506 int, c507 int, c508 int, c509 int, c510 int, c511 int, c512 int, c513 int, c514 int, c515 int, c516 int, c517 int, c518 int, c519 int, c520 int, c521 int, c522 int, c523 int, c524 int, c525 int, c526 int, c527 int, c528 int, c529 int, c530 int, c531 int, c532 int, c533 int, c534 int, c535 int, c536 int, c537 int, c538 int, c539 int, c540 int, c541 int, c542 int, c543 int, c544 int, c545 int, c546 int, c547 int, c548 int, c549 int, c550 int, c551 int, c552 int, c553 int, c554 int, c555 int, c556 int, c557 int, c558 int, c559 int, c560 int, c561 int, c562 int, c563 int, c564 int, c565 int, c566 int, c567 int, c568 int, c569 int, c570 int, c571 int, c572 int, c573 int, c574 int, c575 int, c576 int, c577 int, c578 int, c579 int, c580 int, c581 int, c582 int, c583 int, c584 int, c585 int, c586 int, c587 int, c588 int, c589 int, c590 int, c591 int, c592 int, c593 int, c594 int, c595 int, c596 int, c597 int, c598 int, c599 int, c600 int, c601 int, c602 int, c603 int, c604 int, c605 int, c606 int, c607 int, c608 int, c609 int, c610 int, c611 int, c612 int, c613 int, c614 int, c615 int, c616 int, c617 int, c618 int, c619 int, c620 int, c621 int, c622 int, c623 int, c624 int, c625 int, c626 int, c627 int, c628 int, c629 int, c630 int, c631 int, c632 int, c633 int, c634 int, c635 int, c636 int, c637 int, c638 int, c639 int, c640 int, c641 int, c642 int, c643 int, c644 int, c645 int, c646 int, c647 int, c648 int, c649 int, c650 int, c651 int, c652 int, c653 int, c654 int, c655 int, c656 int, c657 int, c658 int, c659 int, c660 int, c661 int, c662 int, c663 int, c664 int, c665 int, c666 int, c667 int, c668 int, c669 int, c670 int, c671 int, c672 int, c673 int, c674 int, c675 int, c676 int, c677 int, c678 int, c679 int, c680 int, c681 int, c682 int, c683 int, c684 int, c685 int, c686 int, c687 int, c688 int, c689 int, c690 int, c691 int, c692 int, c693 int, c694 int, c695 int, c696 int, c697 int, c698 int, c699 int, c700 int, c701 int, c702 int, c703 int, c704 int, c705 int, c706 int, c707 int, c708 int, c709 int, c710 int, c711 int, c712 int, c713 int, c714 int, c715 int, c716 int, c717 int, c718 int, c719 int, c720 int, c721 int, c722 int, c723 int, c724 int, c725 int, c726 int, c727 int, c728 int, c729 int, c730 int, c731 int, c732 int, c733 int, c734 int, c735 int, c736 int, c737 int, c738 int, c739 int, c740 int, c741 int, c742 int, c743 int, c744 int, c745 int, c746 int, c747 int, c748 int, c749 int, c750 int, c751 int, c752 int, c753 int, c754 int, c755 int, c756 int, c757 int, c758 int, c759 int, c760 int, c761 int, c762 int, c763 int, c764 int, c765 int, c766 int, c767 int, c768 int, c769 int, c770 int, c771 int, c772 int, c773 int, c774 int, c775 int, c776 int, c777 int, c778 int, c779 int, c780 int, c781 int, c782 int, c783 int, c784 int, c785 int, c786 int, c787 int, c788 int, c789 int, c790 int, c791 int, c792 int, c793 int, c794 int, c795 int, c796 int, c797 int, c798 int, c799 int, c800 int, c801 int, c802 int, c803 int, c804 int, c805 int, c806 int, c807 int, c808 int, c809 int, c810 int, c811 int, c812 int, c813 int, c814 int, c815 int, c816 int, c817 int, c818 int, c819 int, c820 int, c821 int, c822 int, c823 int, c824 int, c825 int, c826 int, c827 int, c828 int, c829 int, c830 int, c831 int, c832 int, c833 int, c834 int, c835 int, c836 int, c837 int, c838 int, c839 int, c840 int, c841 int, c842 int, c843 int, c844 int, c845 int, c846 int, c847 int, c848 int, c849 int, c850 int, c851 int, c852 int, c853 int, c854 int, c855 int, c856 int, c857 int, c858 int, c859 int, c860 int, c861 int, c862 int, c863 int, c864 int, c865 int, c866 int, c867 int, c868 int, c869 int, c870 int, c871 int, c872 int, c873 int, c874 int, c875 int, c876 int, c877 int, c878 int, c879 int, c880 int, c881 int, c882 int, c883 int, c884 int, c885 int, c886 int, c887 int, c888 int, c889 int, c890 int, c891 int, c892 int, c893 int, c894 int, c895 int, c896 int, c897 int, c898 int, c899 int, c900 int, c901 int, c902 int, c903 int, c904 int, c905 int, c906 int, c907 int, c908 int, c909 int, c910 int, c911 int, c912 int, c913 int, c914 int, c915 int, c916 int, c917 int, c918 int, c919 int, c920 int, c921 int, c922 int, c923 int, c924 int, c925 int, c926 int, c927 int, c928 int, c929 int, c930 int, c931 int, c932 int, c933 int, c934 int, c935 int, c936 int, c937 int, c938 int, c939 int, c940 int, c941 int, c942 int, c943 int, c944 int, c945 int, c946 int, c947 int, c948 int, c949 int, c950 int, c951 int, c952 int, c953 int, c954 int, c955 int, c956 int, c957 int, c958 int, c959 int, c960 int, c961 int, c962 int, c963 int, c964 int, c965 int, c966 int, c967 int, c968 int, c969 int, c970 int, c971 int, c972 int, c973 int, c974 int, c975 int, c976 int, c977 int, c978 int, c979 int, c980 int, c981 int, c982 int, c983 int, c984 int, c985 int, c986 int, c987 int, c988 int, c989 int, c990 int, c991 int, c992 int, c993 int, c994 int, c995 int, c996 int, c997 int, c998 int, c999 int, c1000 int, c1001 int, c1002 int, c1003 int, c1004 int, c1005 int, c1006 int, c1007 int, c1008 int, c1009 int, c1010 int, c1011 int, c1012 int, c1013 int, c1014 int, c1015 int, c1016 int, c1017 int, c1018 int, c1019 int, c1020 int, c1021 int, c1022 int, c1023 int, c1024 int, c1025 int, c1026 int, c1027 int, c1028 int, c1029 int, c1030 int, c1031 int, c1032 int, c1033 int, c1034 int, c1035 int, c1036 int, c1037 int, c1038 int, c1039 int, c1040 int, c1041 int, c1042 int, c1043 int, c1044 int, c1045 int, c1046 int, c1047 int, c1048 int, c1049 int, c1050 int, c1051 int, c1052 int, c1053 int, c1054 int, c1055 int, c1056 int, c1057 int, c1058 int, c1059 int, c1060 int, c1061 int, c1062 int, c1063 int, c1064 int, c1065 int, c1066 int, c1067 int, c1068 int, c1069 int, c1070 int, c1071 int, c1072 int, c1073 int, c1074 int, c1075 int, c1076 int, c1077 int, c1078 int, c1079 int, c1080 int, c1081 int, c1082 int, c1083 int, c1084 int, c1085 int, c1086 int, c1087 int, c1088 int, c1089 int, c1090 int, c1091 int, c1092 int, c1093 int, c1094 int, c1095 int, c1096 int, c1097 int, c1098 int, c1099 int, c1100 int, c1101 int, c1102 int, c1103 int, c1104 int, c1105 int, c1106 int, c1107 int, c1108 int, c1109 int, c1110 int, c1111 int, c1112 int, c1113 int, c1114 int, c1115 int, c1116 int, c1117 int, c1118 int, c1119 int, c1120 int, c1121 int, c1122 int, c1123 int, c1124 int, c1125 int, c1126 int, c1127 int, c1128 int, c1129 int, c1130 int, c1131 int, c1132 int, c1133 int, c1134 int, c1135 int, c1136 int, c1137 int, c1138 int, c1139 int, c1140 int, c1141 int, c1142 int, c1143 int, c1144 int, c1145 int, c1146 int, c1147 int, c1148 int, c1149 int, c1150 int, c1151 int, c1152 int, c1153 int, c1154 int, c1155 int, c1156 int, c1157 int, c1158 int, c1159 int, c1160 int, c1161 int, c1162 int, c1163 int, c1164 int, c1165 int, c1166 int, c1167 int, c1168 int, c1169 int, c1170 int, c1171 int, c1172 int, c1173 int, c1174 int, c1175 int, c1176 int, c1177 int, c1178 int, c1179 int, c1180 int, c1181 int, c1182 int, c1183 int, c1184 int, c1185 int, c1186 int, c1187 int, c1188 int, c1189 int, c1190 int, c1191 int, c1192 int, c1193 int, c1194 int, c1195 int, c1196 int, c1197 int, c1198 int, c1199 int, c1200 int, c1201 int, c1202 int, c1203 int, c1204 int, c1205 int, c1206 int, c1207 int, c1208 int, c1209 int, c1210 int, c1211 int, c1212 int, c1213 int, c1214 int, c1215 int, c1216 int, c1217 int, c1218 int, c1219 int, c1220 int, c1221 int, c1222 int, c1223 int, c1224 int, c1225 int, c1226 int, c1227 int, c1228 int, c1229 int, c1230 int, c1231 int, c1232 int, c1233 int, c1234 int, c1235 int, c1236 int, c1237 int, c1238 int, c1239 int, c1240 int, c1241 int, c1242 int, c1243 int, c1244 int, c1245 int, c1246 int, c1247 int, c1248 int, c1249 int, c1250 int, c1251 int, c1252 int, c1253 int, c1254 int, c1255 int, c1256 int, c1257 int, c1258 int, c1259 int, c1260 int, c1261 int, c1262 int, c1263 int, c1264 int, c1265 int, c1266 int, c1267 int, c1268 int, c1269 int, c1270 int, c1271 int, c1272 int, c1273 int, c1274 int, c1275 int, c1276 int, c1277 int, c1278 int, c1279 int, c1280 int, c1281 int, c1282 int, c1283 int, c1284 int, c1285 int, c1286 int, c1287 int, c1288 int, c1289 int, c1290 int, c1291 int, c1292 int, c1293 int, c1294 int, c1295 int, c1296 int, c1297 int, c1298 int, c1299 int, c1300 int, c1301 int, c1302 int, c1303 int, c1304 int, c1305 int, c1306 int, c1307 int, c1308 int, c1309 int, c1310 int, c1311 int, c1312 int, c1313 int, c1314 int, c1315 int, c1316 int, c1317 int, c1318 int, c1319 int, c1320 int, c1321 int, c1322 int, c1323 int, c1324 int, c1325 int, c1326 int, c1327 int, c1328 int, c1329 int, c1330 int, c1331 int, c1332 int, c1333 int, c1334 int, c1335 int, c1336 int, c1337 int, c1338 int, c1339 int, c1340 int, c1341 int, c1342 int, c1343 int, c1344 int, c1345 int, c1346 int, c1347 int, c1348 int, c1349 int, c1350 int, c1351 int, c1352 int, c1353 int, c1354 int, c1355 int, c1356 int, c1357 int, c1358 int, c1359 int, c1360 int, c1361 int, c1362 int, c1363 int, c1364 int, c1365 int, c1366 int, c1367 int, c1368 int, c1369 int, c1370 int, c1371 int, c1372 int, c1373 int, c1374 int, c1375 int, c1376 int, c1377 int, c1378 int, c1379 int, c1380 int, c1381 int, c1382 int, c1383 int, c1384 int, c1385 int, c1386 int, c1387 int, c1388 int, c1389 int, c1390 int, c1391 int, c1392 int, c1393 int, c1394 int, c1395 int, c1396 int, c1397 int, c1398 int, c1399 int, c1400 int, c1401 int, c1402 int, c1403 int, c1404 int, c1405 int, c1406 int, c1407 int, c1408 int, c1409 int, c1410 int, c1411 int, c1412 int, c1413 int, c1414 int, c1415 int, c1416 int, c1417 int, c1418 int, c1419 int, c1420 int, c1421 int, c1422 int, c1423 int, c1424 int, c1425 int, c1426 int, c1427 int, c1428 int, c1429 int, c1430 int, c1431 int, c1432 int, c1433 int, c1434 int, c1435 int, c1436 int, c1437 int, c1438 int, c1439 int, c1440 int, c1441 int, c1442 int, c1443 int, c1444 int, c1445 int, c1446 int, c1447 int, c1448 int, c1449 int, c1450 int, c1451 int, c1452 int, c1453 int, c1454 int, c1455 int, c1456 int, c1457 int, c1458 int, c1459 int, c1460 int, c1461 int, c1462 int, c1463 int, c1464 int, c1465 int, c1466 int, c1467 int, c1468 int, c1469 int, c1470 int, c1471 int, c1472 int, c1473 int, c1474 int, c1475 int, c1476 int, c1477 int, c1478 int, c1479 int, c1480 int, c1481 int, c1482 int, c1483 int, c1484 int, c1485 int, c1486 int, c1487 int, c1488 int, c1489 int, c1490 int, c1491 int, c1492 int, c1493 int, c1494 int, c1495 int, c1496 int, c1497 int, c1498 int, c1499 int, c1500 int, c1501 int, c1502 int, c1503 int, c1504 int, c1505 int, c1506 int, c1507 int, c1508 int, c1509 int, c1510 int, c1511 int, c1512 int, c1513 int, c1514 int, c1515 int, c1516 int, c1517 int, c1518 int, c1519 int, c1520 int, c1521 int, c1522 int, c1523 int, c1524 int, c1525 int, c1526 int, c1527 int, c1528 int, c1529 int, c1530 int, c1531 int, c1532 int, c1533 int, c1534 int, c1535 int, c1536 int, c1537 int, c1538 int, c1539 int, c1540 int, c1541 int, c1542 int, c1543 int, c1544 int, c1545 int, c1546 int, c1547 int, c1548 int, c1549 int, c1550 int, c1551 int, c1552 int, c1553 int, c1554 int, c1555 int, c1556 int, c1557 int, c1558 int, c1559 int, c1560 int, c1561 int, c1562 int, c1563 int, c1564 int, c1565 int, c1566 int, c1567 int, c1568 int, c1569 int, c1570 int, c1571 int, c1572 int, c1573 int, c1574 int, c1575 int, c1576 int, c1577 int, c1578 int, c1579 int, c1580 int, c1581 int, c1582 int, c1583 int, c1584 int, c1585 int, c1586 int, c1587 int, c1588 int, c1589 int, c1590 int, c1591 int, c1592 int, c1593 int, c1594 int, c1595 int, c1596 int, c1597 int, c1598 int)
using sortheap;

create index idx_hugesheap on hugesheap using sortheap_btree(tagid);

insert into hugesheap select i % 2, '2020-06-15 12:00:00', 
1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1
from generate_series(1, 1000) i;

-- column project   
select count(*) from hugesheap where tagid = 0;
select count(*) from hugesheap where tagid = 1;
select count(c10) from hugesheap where tagid = 1;
select count(c1000) from hugesheap where tagid = 1;
select count(c2), count(c3), count(c4), count(c1000), count(c1001)
from hugesheap where tagid = 1;
select count(c1598) from hugesheap;
select distinct(c3, c100, c500, c1500) from hugesheap;
select count(*), min(c5), max(c100), max(c500), max(c1000) from hugesheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00' group by tagid;
select tagid, count(*), min(c5), max(c100), max(c500), max(c1000) from hugesheap where time_bucket('1 hour', ts) = '2020-06-15 12:00:00' group by tagid;

-- start_ignore
drop extension matrixts cascade;
-- end_ignore


