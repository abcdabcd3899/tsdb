-- start_ignore
set timezone to 'Asia/Shanghai';
set enable_partitionwise_aggregate to on;
set enable_partitionwise_join to on;
create extension matrixts;
create extension mars;
-- end_ignore

CREATE TABLE tags(id SERIAL PRIMARY KEY,
 hostname TEXT,
 region TEXT,
 datacenter TEXT,
 rack TEXT,
 os TEXT,
 arch TEXT,
 team TEXT,
 service TEXT,
 service_version TEXT,
 service_environment TEXT)
DISTRIBUTED REPLICATED;

CREATE TABLE cpu_heap (time timestamptz,
 tags_id integer,
 hostname TEXT,
 usage_user DOUBLE PRECISION,
 usage_system DOUBLE PRECISION,
 usage_idle DOUBLE PRECISION,
 usage_nice DOUBLE PRECISION,
 usage_iowait DOUBLE PRECISION,
 usage_irq DOUBLE PRECISION,
 usage_softirq DOUBLE PRECISION,
 usage_steal DOUBLE PRECISION,
 usage_guest DOUBLE PRECISION,
 usage_guest_nice DOUBLE PRECISION,
 additional_tags JSONB DEFAULT NULL)
DISTRIBUTED BY(tags_id);

CREATE TABLE cpu (time timestamptz,
 tags_id integer,
 hostname TEXT,
 usage_user DOUBLE PRECISION,
 usage_system DOUBLE PRECISION,
 usage_idle DOUBLE PRECISION,
 usage_nice DOUBLE PRECISION,
 usage_iowait DOUBLE PRECISION,
 usage_irq DOUBLE PRECISION,
 usage_softirq DOUBLE PRECISION,
 usage_steal DOUBLE PRECISION,
 usage_guest DOUBLE PRECISION,
 usage_guest_nice DOUBLE PRECISION,
 additional_tags JSONB DEFAULT NULL)
using mars with (tagkey = "tags_id", timekey = "time", timebucket="1 hour")
DISTRIBUTED BY(tags_id);

COPY tags FROM stdin;
1	host_208	sa-east-1	sa-east-1b	41	Ubuntu16.10	x64	NYC	6	0	test
2	host_240	eu-west-1	eu-west-1c	77	Ubuntu16.10	x64	CHI	9	0	staging
3	host_334	ap-northeast-1	ap-northeast-1c	87	Ubuntu16.10	x64	NYC	1	1	test
4	host_99	us-east-1	us-east-1a	31	Ubuntu16.10	x86	SF	19	1	staging
5	host_132	sa-east-1	sa-east-1c	15	Ubuntu16.04LTS	x64	SF	2	0	production
\.

COPY cpu_heap FROM stdin;
2016-01-01 08:00:00+08	1	host_208	11	47	14	21	97	10	61	31	48	87	\N
2016-01-01 09:00:00+08	1	host_208	12	39	22	19	92	24	48	35	55	72	\N
2016-01-01 10:00:00+08	1	host_208	0	10	1	31	73	26	85	23	86	71	\N
2016-01-01 11:00:00+08	1	host_208	0	13	32	41	50	1	81	20	70	52	\N
2016-01-01 12:00:00+08	1	host_208	7	23	59	100	42	22	30	17	56	61	\N
2016-01-01 13:00:00+08	1	host_208	24	1	60	76	47	13	10	33	80	72	\N
2016-01-01 14:00:00+08	1	host_208	10	11	34	77	50	0	13	15	22	64	\N
2016-01-01 15:00:00+08	1	host_208	14	18	3	95	73	5	38	0	8	90	\N
2016-01-01 16:00:00+08	1	host_208	17	42	22	83	61	6	25	2	5	100	\N
2016-01-01 17:00:00+08	1	host_208	33	42	34	93	66	28	44	19	2	78	\N
2016-01-01 18:00:00+08	1	host_208	37	30	46	87	46	44	37	44	15	65	\N
2016-01-01 19:00:00+08	1	host_208	34	27	42	89	59	49	48	43	35	66	\N
2016-01-01 20:00:00+08	1	host_208	6	34	67	60	60	57	20	14	52	41	\N
2016-01-01 21:00:00+08	1	host_208	0	56	86	55	50	89	25	34	60	49	\N
2016-01-01 22:00:00+08	1	host_208	16	47	98	75	33	93	26	54	37	7	\N
2016-01-01 23:00:00+08	1	host_208	6	45	84	46	5	77	24	71	29	8	\N
2016-01-01 08:00:00+08	2	host_240	97	12	88	42	28	96	71	91	84	27	\N
2016-01-01 09:00:00+08	2	host_240	84	15	91	38	19	98	71	75	85	56	\N
2016-01-01 10:00:00+08	2	host_240	75	24	82	36	9	96	54	75	80	41	\N
2016-01-01 11:00:00+08	2	host_240	61	29	91	30	6	98	34	72	49	68	\N
2016-01-01 12:00:00+08	2	host_240	75	61	95	14	10	91	15	43	33	75	\N
2016-01-01 13:00:00+08	2	host_240	82	58	97	20	6	66	56	30	58	94	\N
2016-01-01 14:00:00+08	2	host_240	81	70	100	6	11	60	71	37	84	77	\N
2016-01-01 15:00:00+08	2	host_240	79	59	80	31	17	47	61	59	86	88	\N
2016-01-01 16:00:00+08	2	host_240	72	82	84	63	5	69	51	36	84	94	\N
2016-01-01 17:00:00+08	2	host_240	83	74	96	70	39	69	63	13	93	96	\N
2016-01-01 18:00:00+08	2	host_240	91	82	84	57	35	95	64	0	91	83	\N
2016-01-01 19:00:00+08	2	host_240	85	93	75	98	17	84	48	16	64	96	\N
2016-01-01 20:00:00+08	2	host_240	77	86	91	72	37	75	37	11	47	98	\N
2016-01-01 21:00:00+08	2	host_240	62	76	99	62	61	58	50	43	27	84	\N
2016-01-01 22:00:00+08	2	host_240	43	85	86	50	66	32	53	49	13	66	\N
2016-01-01 23:00:00+08	2	host_240	64	89	77	39	77	33	51	67	4	74	\N
2016-01-01 08:00:00+08	3	host_334	72	60	30	8	78	100	59	35	31	3	\N
2016-01-01 09:00:00+08	3	host_334	46	36	55	0	99	83	49	44	0	3	\N
2016-01-01 10:00:00+08	3	host_334	76	24	58	18	73	91	94	64	21	19	\N
2016-01-01 11:00:00+08	3	host_334	97	26	65	15	68	95	92	67	9	3	\N
2016-01-01 12:00:00+08	3	host_334	60	29	43	21	57	88	77	57	9	9	\N
2016-01-01 13:00:00+08	3	host_334	47	43	43	22	49	100	85	74	14	3	\N
2016-01-01 14:00:00+08	3	host_334	36	20	22	11	50	98	30	56	7	44	\N
2016-01-01 15:00:00+08	3	host_334	25	36	54	15	76	82	57	65	29	56	\N
2016-01-01 16:00:00+08	3	host_334	23	44	75	0	81	86	63	58	51	22	\N
2016-01-01 17:00:00+08	3	host_334	5	31	93	35	93	71	99	81	23	14	\N
2016-01-01 18:00:00+08	3	host_334	13	29	92	15	82	54	45	84	19	51	\N
2016-01-01 19:00:00+08	3	host_334	33	15	91	10	85	47	45	84	14	32	\N
2016-01-01 20:00:00+08	3	host_334	54	14	89	17	71	73	54	88	9	19	\N
2016-01-01 21:00:00+08	3	host_334	73	7	47	20	85	54	61	100	13	25	\N
2016-01-01 22:00:00+08	3	host_334	73	19	54	11	68	45	56	80	8	20	\N
2016-01-01 23:00:00+08	3	host_334	63	24	47	5	55	53	62	83	4	33	\N
2016-01-01 08:00:00+08	4	host_99	12	52	71	44	42	87	60	35	67	35	\N
2016-01-01 09:00:00+08	4	host_99	56	65	88	48	29	70	59	12	16	40	\N
2016-01-01 10:00:00+08	4	host_99	81	50	76	62	52	86	98	2	37	55	\N
2016-01-01 11:00:00+08	4	host_99	98	51	97	71	57	91	92	3	61	49	\N
2016-01-01 12:00:00+08	4	host_99	49	74	80	69	25	92	97	22	76	70	\N
2016-01-01 13:00:00+08	4	host_99	62	98	100	71	59	82	74	13	70	52	\N
2016-01-01 14:00:00+08	4	host_99	72	86	94	86	56	81	87	23	51	28	\N
2016-01-01 15:00:00+08	4	host_99	29	40	99	68	23	58	50	36	70	36	\N
2016-01-01 16:00:00+08	4	host_99	10	32	63	70	62	48	47	38	51	47	\N
2016-01-01 17:00:00+08	4	host_99	4	68	69	77	46	54	81	32	15	46	\N
2016-01-01 18:00:00+08	4	host_99	1	68	72	82	53	52	80	33	13	52	\N
2016-01-01 19:00:00+08	4	host_99	23	57	93	63	4	52	83	52	21	54	\N
2016-01-01 20:00:00+08	4	host_99	17	61	69	64	31	71	95	58	34	49	\N
2016-01-01 21:00:00+08	4	host_99	17	41	79	76	40	57	81	36	33	65	\N
2016-01-01 22:00:00+08	4	host_99	0	74	35	89	39	82	94	53	41	86	\N
2016-01-01 23:00:00+08	4	host_99	8	54	23	86	42	79	76	34	35	92	\N
2016-01-01 08:00:00+08	5	host_132	62	71	10	70	5	77	44	38	23	44	\N
2016-01-01 09:00:00+08	5	host_132	82	48	1	55	16	78	49	23	10	14	\N
2016-01-01 10:00:00+08	5	host_132	96	22	5	67	0	63	47	33	11	21	\N
2016-01-01 11:00:00+08	5	host_132	81	12	30	68	39	72	42	52	16	19	\N
2016-01-01 12:00:00+08	5	host_132	98	13	24	73	10	68	31	60	15	68	\N
2016-01-01 13:00:00+08	5	host_132	100	4	9	94	1	39	83	85	44	60	\N
2016-01-01 14:00:00+08	5	host_132	99	27	11	90	3	10	80	69	1	82	\N
2016-01-01 15:00:00+08	5	host_132	89	5	17	78	7	38	97	58	15	65	\N
2016-01-01 16:00:00+08	5	host_132	49	15	19	25	17	30	100	53	57	52	\N
2016-01-01 17:00:00+08	5	host_132	69	9	0	35	15	34	94	72	63	61	\N
2016-01-01 18:00:00+08	5	host_132	99	26	9	2	37	46	87	66	63	66	\N
2016-01-01 19:00:00+08	5	host_132	72	30	12	14	34	58	86	99	90	90	\N
2016-01-01 20:00:00+08	5	host_132	74	19	7	31	49	91	66	75	96	48	\N
2016-01-01 21:00:00+08	5	host_132	39	23	19	34	49	42	76	58	76	42	\N
2016-01-01 22:00:00+08	5	host_132	64	22	18	11	45	21	94	87	42	37	\N
2016-01-01 23:00:00+08	5	host_132	36	18	1	5	77	1	93	82	48	39	\N
\.

insert into cpu select * from cpu_heap order by tags_id, time;

-- cpu_max_all_1
select time_bucket('3600 seconds', time) as hour
     , max(usage_user) as max_usage_user
     , max(usage_system) as max_usage_system
     , max(usage_idle) as max_usage_idle
     , max(usage_nice) as max_usage_nice
     , max(usage_iowait) as max_usage_iowait
     , max(usage_irq) as max_usage_irq
     , max(usage_softirq) as max_usage_softirq
     , max(usage_steal) as max_usage_steal
     , max(usage_guest) as max_usage_guest
     , max(usage_guest_nice) as max_usage_guest_nice
  from cpu
 where tags_id in (select id from tags where hostname in ('host_132'))
   and time >= '2016-01-01 12:21:08.743349'
   and time <  '2016-01-01 20:21:08.743349'
 group by hour
 order by hour
;

-- double_groupby_1
with cpu_avg as (
  select time_bucket('3600 seconds', time) as hour
       , tags_id
       , count(usage_user) as mean_usage_user
    from cpu
   where time >= '2016-01-01 11:16:29.378777'
     and time <  '2016-01-01 23:16:29.378777'
   group by hour, tags_id
)
select hour
     , tags.hostname
     , mean_usage_user
  from cpu_avg
  join tags on cpu_avg.tags_id = tags.id
 order by hour, tags.hostname
;

-- groupby_orderby_limit
select time_bucket('3600 seconds', time) as minute
     , max(usage_user)
  from cpu
 where time < '2016-01-01 13:26:46.646325'
 group by minute
 order by minute
 limit 5
;

-- high_cpu_1
select *
  from cpu
 where usage_user > 10.0
   and time >= '2016-01-01 09:39:11.640097'
   and time <  '2016-01-01 21:39:11.640097'
   and tags_id in (select id
                   from tags
                   where hostname in ('host_99'))
;

-- high_cpu_all
select *
  from cpu
  where usage_user > 10.0
   and time >= '2016-01-01 05:19:02.079405'
   and time <  '2016-01-01 17:19:02.079405'
;

--single_groupby_5_1_1
select time_bucket('60 seconds', time) as minute
     , max(usage_user) as max_usage_user
     , max(usage_system) as max_usage_system
     , max(usage_idle) as max_usage_idle
     , max(usage_nice) as max_usage_nice
     , max(usage_iowait) as max_usage_iowait
  from cpu
 where tags_id in (select id
                   from tags
                   where hostname in ('host_208'))
   and time >= '2016-01-01 8:34:45.774719'
   and time <  '2016-01-01 11:34:45.774719'
 group by minute
 order by minute asc
;

-- query plan

-- cpu_max_all_1
explain (costs off) select time_bucket('3600 seconds', time) as hour
     , max(usage_user) as max_usage_user
     , max(usage_system) as max_usage_system
     , max(usage_idle) as max_usage_idle
     , max(usage_nice) as max_usage_nice
     , max(usage_iowait) as max_usage_iowait
     , max(usage_irq) as max_usage_irq
     , max(usage_softirq) as max_usage_softirq
     , max(usage_steal) as max_usage_steal
     , max(usage_guest) as max_usage_guest
     , max(usage_guest_nice) as max_usage_guest_nice
  from cpu
 where tags_id in (select id from tags where hostname in ('host_132'))
   and time >= '2016-01-01 12:21:08.743349'
   and time <  '2016-01-01 20:21:08.743349'
 group by hour
 order by hour
;

-- double_groupby_1
explain (costs off) with cpu_avg as (
  select time_bucket('3600 seconds', time) as hour
       , tags_id
       , count(usage_user) as mean_usage_user
    from cpu
   where time >= '2016-01-01 11:16:29.378777'
     and time <  '2016-01-01 23:16:29.378777'
   group by hour, tags_id
)
select hour
     , tags.hostname
     , mean_usage_user
  from cpu_avg
  join tags on cpu_avg.tags_id = tags.id
 order by hour, tags.hostname
;

-- groupby_orderby_limit
explain (costs off) select time_bucket('3600 seconds', time) as minute
     , max(usage_user)
  from cpu
 where time < '2016-01-01 13:26:46.646325'
 group by minute
 order by minute
 limit 5
;

-- high_cpu_1
explain (costs off) select *
  from cpu
 where usage_user > 10.0
   and time >= '2016-01-01 09:39:11.640097'
   and time <  '2016-01-01 21:39:11.640097'
   and tags_id in (select id
                   from tags
                   where hostname in ('host_99'))
;

-- high_cpu_all
explain (costs off) select *
  from cpu
  where usage_user > 10.0
   and time >= '2016-01-01 05:19:02.079405'
   and time <  '2016-01-01 17:19:02.079405'
;

--single_groupby_5_1_1
explain (costs off) select time_bucket('60 seconds', time) as minute
     , max(usage_user) as max_usage_user
     , max(usage_system) as max_usage_system
     , max(usage_idle) as max_usage_idle
     , max(usage_nice) as max_usage_nice
     , max(usage_iowait) as max_usage_iowait
  from cpu
 where tags_id in (select id
                   from tags
                   where hostname in ('host_208'))
   and time >= '2016-01-01 8:34:45.774719'
   and time <  '2016-01-01 11:34:45.774719'
 group by minute
 order by minute asc
;

-- start_ignore
set timezone to 'PST8PDT';
drop table cpu;
drop table tags;
drop table cpu_heap;
-- end_ignore
