--
-- Routines for sortheap
--
-- Create the sortheap access method
CREATE FUNCTION matrixts_internal.sortheap_tableam_handler(INTERNAL) RETURNS table_am_handler
AS '$libdir/matrixts' LANGUAGE C STRICT;

CREATE ACCESS METHOD sortheap
TYPE TABLE
HANDLER matrixts_internal.sortheap_tableam_handler;

-- Create help function to get the info of the sortheap storage
CREATE OR REPLACE FUNCTION matrixts_internal.info_sortheap(r1 REGCLASS) RETURNS CSTRING
    AS '$libdir/matrixts', 'info_sortheap'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON ALL SEGMENTS;

--
-- sortheap_btree indexes, used by sortheap
--
CREATE OR REPLACE FUNCTION sortheap_bthandler(INTERNAL) RETURNS INDEX_AM_HANDLER
    AS '$libdir/matrixts' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE ACCESS METHOD sortheap_btree TYPE INDEX HANDLER sortheap_bthandler;

--
-- operator classes to support btree operations
--

CREATE OPERATOR CLASS array_ops DEFAULT FOR TYPE anyarray USING sortheap_btree
    AS OPERATOR 1 <(anyarray,anyarray)
     , OPERATOR 2 <=(anyarray,anyarray)
     , OPERATOR 3 =(anyarray,anyarray)
     , OPERATOR 4 >=(anyarray,anyarray)
     , OPERATOR 5 >(anyarray,anyarray)
     , FUNCTION 1 (anyarray,anyarray) btarraycmp(anyarray,anyarray)
;

CREATE OPERATOR CLASS bit_ops DEFAULT FOR TYPE bit USING sortheap_btree
    AS OPERATOR 1 <(bit,bit)
     , OPERATOR 2 <=(bit,bit)
     , OPERATOR 3 =(bit,bit)
     , OPERATOR 4 >=(bit,bit)
     , OPERATOR 5 >(bit,bit)
     , FUNCTION 1 (bit,bit) bitcmp(bit,bit)
;

CREATE OPERATOR CLASS bool_ops DEFAULT FOR TYPE boolean USING sortheap_btree
    AS OPERATOR 1 <(boolean,boolean)
     , OPERATOR 2 <=(boolean,boolean)
     , OPERATOR 3 =(boolean,boolean)
     , OPERATOR 4 >=(boolean,boolean)
     , OPERATOR 5 >(boolean,boolean)
     , FUNCTION 1 (boolean,boolean) btboolcmp(boolean,boolean)
;

CREATE OPERATOR CLASS bpchar_ops DEFAULT FOR TYPE character USING sortheap_btree
    AS OPERATOR 1 <(character,character)
     , OPERATOR 2 <=(character,character)
     , OPERATOR 3 =(character,character)
     , OPERATOR 4 >=(character,character)
     , OPERATOR 5 >(character,character)
     , FUNCTION 1 (character,character) bpcharcmp(character,character)
;

CREATE OPERATOR CLASS bytea_ops DEFAULT FOR TYPE bytea USING sortheap_btree
    AS OPERATOR 1 <(bytea,bytea)
     , OPERATOR 2 <=(bytea,bytea)
     , OPERATOR 3 =(bytea,bytea)
     , OPERATOR 4 >=(bytea,bytea)
     , OPERATOR 5 >(bytea,bytea)
     , FUNCTION 1 (bytea,bytea) byteacmp(bytea,bytea)
;

CREATE OPERATOR CLASS char_ops DEFAULT FOR TYPE "char" USING sortheap_btree
    AS OPERATOR 1 <("char","char")
     , OPERATOR 2 <=("char","char")
     , OPERATOR 3 =("char","char")
     , OPERATOR 4 >=("char","char")
     , OPERATOR 5 >("char","char")
     , FUNCTION 1 ("char","char") btcharcmp("char","char")
;

CREATE OPERATOR CLASS date_ops DEFAULT FOR TYPE date USING sortheap_btree
    AS OPERATOR 1 <(date,date)
     , OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 2 <=(date,date)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 3 =(date,date)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 4 >=(date,date)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 5 >(date,date)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp with time zone,date)
     , FUNCTION 1 (date,date) date_cmp(date,date)
     , FUNCTION 1 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 1 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 1 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 1 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
;

CREATE OPERATOR CLASS float4_ops DEFAULT FOR TYPE real USING sortheap_btree
    AS OPERATOR 1 <(real,real)
     , OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 2 <=(real,real)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 3 =(real,real)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 4 >=(real,real)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 5 >(real,real)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , FUNCTION 1 (real,real) btfloat4cmp(real,real)
     , FUNCTION 1 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 1 (double precision,real) btfloat84cmp(double precision,real)
;

CREATE OPERATOR CLASS float8_ops DEFAULT FOR TYPE double precision USING sortheap_btree
    AS OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 1 <(double precision,double precision)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 2 <=(double precision,double precision)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 3 =(double precision,double precision)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 4 >=(double precision,double precision)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , OPERATOR 5 >(double precision,double precision)
     , FUNCTION 1 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 1 (double precision,real) btfloat84cmp(double precision,real)
     , FUNCTION 1 (double precision,double precision) btfloat8cmp(double precision,double precision)
;

CREATE OPERATOR CLASS inet_ops DEFAULT FOR TYPE inet USING sortheap_btree
    AS OPERATOR 1 <(inet,inet)
     , OPERATOR 2 <=(inet,inet)
     , OPERATOR 3 =(inet,inet)
     , OPERATOR 4 >=(inet,inet)
     , OPERATOR 5 >(inet,inet)
     , FUNCTION 1 (inet,inet) network_cmp(inet,inet)
;

CREATE OPERATOR CLASS int2_ops DEFAULT FOR TYPE smallint USING sortheap_btree
    AS OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(smallint,smallint)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(smallint,smallint)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(smallint,smallint)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(smallint,smallint)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(smallint,smallint)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,smallint)
     , FUNCTION 1 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 1 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 1 (smallint,smallint) btint2cmp(smallint,smallint)
     , FUNCTION 1 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 1 (integer,smallint) btint42cmp(integer,smallint)
;

CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE integer USING sortheap_btree
    AS OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 1 <(integer,integer)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 2 <=(integer,integer)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 3 =(integer,integer)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 4 >=(integer,integer)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,bigint)
     , OPERATOR 5 >(integer,smallint)
     , OPERATOR 5 >(integer,integer)
     , FUNCTION 1 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 1 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 1 (integer,bigint) btint48cmp(integer,bigint)
     , FUNCTION 1 (integer,smallint) btint42cmp(integer,smallint)
     , FUNCTION 1 (integer,integer) btint4cmp(integer,integer)
;

CREATE OPERATOR CLASS int8_ops DEFAULT FOR TYPE bigint USING sortheap_btree
    AS OPERATOR 1 <(bigint,bigint)
     , OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 2 <=(bigint,bigint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 3 =(bigint,bigint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 4 >=(bigint,bigint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 5 >(bigint,bigint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(integer,bigint)
     , FUNCTION 1 (bigint,bigint) btint8cmp(bigint,bigint)
     , FUNCTION 1 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 1 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 1 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 1 (integer,bigint) btint48cmp(integer,bigint)
;

CREATE OPERATOR CLASS interval_ops DEFAULT FOR TYPE interval USING sortheap_btree
    AS OPERATOR 1 <(interval,interval)
     , OPERATOR 2 <=(interval,interval)
     , OPERATOR 3 =(interval,interval)
     , OPERATOR 4 >=(interval,interval)
     , OPERATOR 5 >(interval,interval)
     , FUNCTION 1 (interval,interval) interval_cmp(interval,interval)
;

CREATE OPERATOR CLASS macaddr_ops DEFAULT FOR TYPE macaddr USING sortheap_btree
    AS OPERATOR 1 <(macaddr,macaddr)
     , OPERATOR 2 <=(macaddr,macaddr)
     , OPERATOR 3 =(macaddr,macaddr)
     , OPERATOR 4 >=(macaddr,macaddr)
     , OPERATOR 5 >(macaddr,macaddr)
     , FUNCTION 1 (macaddr,macaddr) macaddr_cmp(macaddr,macaddr)
;

CREATE OPERATOR CLASS macaddr8_ops DEFAULT FOR TYPE macaddr8 USING sortheap_btree
    AS OPERATOR 1 <(macaddr8,macaddr8)
     , OPERATOR 2 <=(macaddr8,macaddr8)
     , OPERATOR 3 =(macaddr8,macaddr8)
     , OPERATOR 4 >=(macaddr8,macaddr8)
     , OPERATOR 5 >(macaddr8,macaddr8)
     , FUNCTION 1 (macaddr8,macaddr8) macaddr8_cmp(macaddr8,macaddr8)
;

CREATE OPERATOR CLASS name_ops DEFAULT FOR TYPE name USING sortheap_btree
    AS OPERATOR 1 <(name,name)
     , OPERATOR 1 <(name,text)
     , OPERATOR 1 <(text,name)
     , OPERATOR 2 <=(name,name)
     , OPERATOR 2 <=(name,text)
     , OPERATOR 2 <=(text,name)
     , OPERATOR 3 =(name,name)
     , OPERATOR 3 =(name,text)
     , OPERATOR 3 =(text,name)
     , OPERATOR 4 >=(name,name)
     , OPERATOR 4 >=(name,text)
     , OPERATOR 4 >=(text,name)
     , OPERATOR 5 >(name,name)
     , OPERATOR 5 >(name,text)
     , OPERATOR 5 >(text,name)
     , FUNCTION 1 (name,name) btnamecmp(name,name)
     , FUNCTION 1 (name,text) btnametextcmp(name,text)
     , FUNCTION 1 (text,name) bttextnamecmp(text,name)
;

CREATE OPERATOR CLASS numeric_ops DEFAULT FOR TYPE numeric USING sortheap_btree
    AS OPERATOR 1 <(numeric,numeric)
     , OPERATOR 2 <=(numeric,numeric)
     , OPERATOR 3 =(numeric,numeric)
     , OPERATOR 4 >=(numeric,numeric)
     , OPERATOR 5 >(numeric,numeric)
     , FUNCTION 1 (numeric,numeric) numeric_cmp(numeric,numeric)
;

CREATE OPERATOR CLASS complex_ops DEFAULT FOR TYPE complex USING sortheap_btree
    AS OPERATOR 1 <<(complex,complex)
     , OPERATOR 2 <<=(complex,complex)
     , OPERATOR 3 =(complex,complex)
     , OPERATOR 4 >>=(complex,complex)
     , OPERATOR 5 >>(complex,complex)
     , FUNCTION 1 (complex,complex) complex_cmp(complex,complex)
;

CREATE OPERATOR CLASS oid_ops DEFAULT FOR TYPE oid USING sortheap_btree
    AS OPERATOR 1 <(oid,oid)
     , OPERATOR 2 <=(oid,oid)
     , OPERATOR 3 =(oid,oid)
     , OPERATOR 4 >=(oid,oid)
     , OPERATOR 5 >(oid,oid)
     , FUNCTION 1 (oid,oid) btoidcmp(oid,oid)
;

CREATE OPERATOR CLASS oidvector_ops DEFAULT FOR TYPE oidvector USING sortheap_btree
    AS OPERATOR 1 <(oidvector,oidvector)
     , OPERATOR 2 <=(oidvector,oidvector)
     , OPERATOR 3 =(oidvector,oidvector)
     , OPERATOR 4 >=(oidvector,oidvector)
     , OPERATOR 5 >(oidvector,oidvector)
     , FUNCTION 1 (oidvector,oidvector) btoidvectorcmp(oidvector,oidvector)
;

CREATE OPERATOR CLASS record_ops DEFAULT FOR TYPE record USING sortheap_btree
    AS OPERATOR 1 <(record,record)
     , OPERATOR 2 <=(record,record)
     , OPERATOR 3 =(record,record)
     , OPERATOR 4 >=(record,record)
     , OPERATOR 5 >(record,record)
     , FUNCTION 1 (record,record) btrecordcmp(record,record)
;

CREATE OPERATOR CLASS text_ops DEFAULT FOR TYPE text USING sortheap_btree
    AS OPERATOR 1 <(name,text)
     , OPERATOR 1 <(text,name)
     , OPERATOR 1 <(text,text)
     , OPERATOR 2 <=(name,text)
     , OPERATOR 2 <=(text,name)
     , OPERATOR 2 <=(text,text)
     , OPERATOR 3 =(name,text)
     , OPERATOR 3 =(text,name)
     , OPERATOR 3 =(text,text)
     , OPERATOR 4 >=(name,text)
     , OPERATOR 4 >=(text,name)
     , OPERATOR 4 >=(text,text)
     , OPERATOR 5 >(name,text)
     , OPERATOR 5 >(text,name)
     , OPERATOR 5 >(text,text)
     , FUNCTION 1 (name,text) btnametextcmp(name,text)
     , FUNCTION 1 (text,name) bttextnamecmp(text,name)
     , FUNCTION 1 (text,text) bttextcmp(text,text)
;

CREATE OPERATOR CLASS time_ops DEFAULT FOR TYPE time without time zone USING sortheap_btree
    AS OPERATOR 1 <(time without time zone,time without time zone)
     , OPERATOR 2 <=(time without time zone,time without time zone)
     , OPERATOR 3 =(time without time zone,time without time zone)
     , OPERATOR 4 >=(time without time zone,time without time zone)
     , OPERATOR 5 >(time without time zone,time without time zone)
     , FUNCTION 1 (time without time zone,time without time zone) time_cmp(time without time zone,time without time zone)
;

CREATE OPERATOR CLASS timestamptz_ops DEFAULT FOR TYPE timestamp with time zone USING sortheap_btree
    AS OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp with time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp with time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,date)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp with time zone)
     , FUNCTION 1 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 1 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
     , FUNCTION 1 (timestamp with time zone,timestamp with time zone) timestamptz_cmp(timestamp with time zone,timestamp with time zone)
;

CREATE OPERATOR CLASS timetz_ops DEFAULT FOR TYPE time with time zone USING sortheap_btree
    AS OPERATOR 1 <(time with time zone,time with time zone)
     , OPERATOR 2 <=(time with time zone,time with time zone)
     , OPERATOR 3 =(time with time zone,time with time zone)
     , OPERATOR 4 >=(time with time zone,time with time zone)
     , OPERATOR 5 >(time with time zone,time with time zone)
     , FUNCTION 1 (time with time zone,time with time zone) timetz_cmp(time with time zone,time with time zone)
;

CREATE OPERATOR CLASS varbit_ops DEFAULT FOR TYPE bit varying USING sortheap_btree
    AS OPERATOR 1 <(bit varying,bit varying)
     , OPERATOR 2 <=(bit varying,bit varying)
     , OPERATOR 3 =(bit varying,bit varying)
     , OPERATOR 4 >=(bit varying,bit varying)
     , OPERATOR 5 >(bit varying,bit varying)
     , FUNCTION 1 (bit varying,bit varying) varbitcmp(bit varying,bit varying)
;

CREATE OPERATOR CLASS timestamp_ops DEFAULT FOR TYPE timestamp without time zone USING sortheap_btree
    AS OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp without time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp without time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp without time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , FUNCTION 1 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 1 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 1 (timestamp without time zone,timestamp without time zone) timestamp_cmp(timestamp without time zone,timestamp without time zone)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
;

CREATE OPERATOR CLASS money_ops DEFAULT FOR TYPE money USING sortheap_btree
    AS OPERATOR 1 <(money,money)
     , OPERATOR 2 <=(money,money)
     , OPERATOR 3 =(money,money)
     , OPERATOR 4 >=(money,money)
     , OPERATOR 5 >(money,money)
     , FUNCTION 1 (money,money) cash_cmp(money,money)
;

CREATE OPERATOR CLASS tid_ops DEFAULT FOR TYPE tid USING sortheap_btree
    AS OPERATOR 1 <(tid,tid)
     , OPERATOR 2 <=(tid,tid)
     , OPERATOR 3 =(tid,tid)
     , OPERATOR 4 >=(tid,tid)
     , OPERATOR 5 >(tid,tid)
     , FUNCTION 1 (tid,tid) bttidcmp(tid,tid)
;

CREATE OPERATOR CLASS uuid_ops DEFAULT FOR TYPE uuid USING sortheap_btree
    AS OPERATOR 1 <(uuid,uuid)
     , OPERATOR 2 <=(uuid,uuid)
     , OPERATOR 3 =(uuid,uuid)
     , OPERATOR 4 >=(uuid,uuid)
     , OPERATOR 5 >(uuid,uuid)
     , FUNCTION 1 (uuid,uuid) uuid_cmp(uuid,uuid)
;

CREATE OPERATOR CLASS pg_lsn_ops DEFAULT FOR TYPE pg_lsn USING sortheap_btree
    AS OPERATOR 1 <(pg_lsn,pg_lsn)
     , OPERATOR 2 <=(pg_lsn,pg_lsn)
     , OPERATOR 3 =(pg_lsn,pg_lsn)
     , OPERATOR 4 >=(pg_lsn,pg_lsn)
     , OPERATOR 5 >(pg_lsn,pg_lsn)
     , FUNCTION 1 (pg_lsn,pg_lsn) pg_lsn_cmp(pg_lsn,pg_lsn)
;

CREATE OPERATOR CLASS enum_ops DEFAULT FOR TYPE anyenum USING sortheap_btree
    AS OPERATOR 1 <(anyenum,anyenum)
     , OPERATOR 2 <=(anyenum,anyenum)
     , OPERATOR 3 =(anyenum,anyenum)
     , OPERATOR 4 >=(anyenum,anyenum)
     , OPERATOR 5 >(anyenum,anyenum)
     , FUNCTION 1 (anyenum,anyenum) enum_cmp(anyenum,anyenum)
;

CREATE OPERATOR CLASS tsvector_ops DEFAULT FOR TYPE tsvector USING sortheap_btree
    AS OPERATOR 1 <(tsvector,tsvector)
     , OPERATOR 2 <=(tsvector,tsvector)
     , OPERATOR 3 =(tsvector,tsvector)
     , OPERATOR 4 >=(tsvector,tsvector)
     , OPERATOR 5 >(tsvector,tsvector)
     , FUNCTION 1 (tsvector,tsvector) tsvector_cmp(tsvector,tsvector)
;

CREATE OPERATOR CLASS tsquery_ops DEFAULT FOR TYPE tsquery USING sortheap_btree
    AS OPERATOR 1 <(tsquery,tsquery)
     , OPERATOR 2 <=(tsquery,tsquery)
     , OPERATOR 3 =(tsquery,tsquery)
     , OPERATOR 4 >=(tsquery,tsquery)
     , OPERATOR 5 >(tsquery,tsquery)
     , FUNCTION 1 (tsquery,tsquery) tsquery_cmp(tsquery,tsquery)
;

CREATE OPERATOR CLASS range_ops DEFAULT FOR TYPE anyrange USING sortheap_btree
    AS OPERATOR 1 <(anyrange,anyrange)
     , OPERATOR 2 <=(anyrange,anyrange)
     , OPERATOR 3 =(anyrange,anyrange)
     , OPERATOR 4 >=(anyrange,anyrange)
     , OPERATOR 5 >(anyrange,anyrange)
     , FUNCTION 1 (anyrange,anyrange) range_cmp(anyrange,anyrange)
;

CREATE OPERATOR CLASS jsonb_ops DEFAULT FOR TYPE jsonb USING sortheap_btree
    AS OPERATOR 1 <(jsonb,jsonb)
     , OPERATOR 2 <=(jsonb,jsonb)
     , OPERATOR 3 =(jsonb,jsonb)
     , OPERATOR 4 >=(jsonb,jsonb)
     , OPERATOR 5 >(jsonb,jsonb)
     , FUNCTION 1 (jsonb,jsonb) jsonb_cmp(jsonb,jsonb)
;
