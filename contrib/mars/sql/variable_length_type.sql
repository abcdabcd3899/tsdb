-- start_ignore
create extension if not exists mars;

create extension if not exists gp_debug_numsegments;

select gp_debug_set_create_table_default_numsegments(1);

create or replace function generate_random_string(length integer) returns text as
$$
declare
  chars text[] := '{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z}';
  result text := '';
  i integer := 0;
begin
  if length < 0 then
    raise exception 'given length cannot be less than 0';
  end if;
  for i in 1..length loop
    result := result || chars[1+random()*(array_length(chars, 1)-1)];
  end loop;
  return result;
end;
$$ language plpgsql;

drop table if exists foo;
-- end_ignore

-- text type basic test
create table foo(a int, b text) using mars;

insert into foo select i, 'abcdefghigklmnopqrstuvwxyz' from generate_series(1 , 4096) i;

select max(a), b from foo group by b;

truncate foo;

insert into foo values (1, 'abcdefghigklmnopqrstuvwxyz'), (2, 'abc'), (3, 'abcdef'), (4, 'a');

select a, b from foo;

drop table foo;

-- json type basic test
create table foo(a int, b json) using mars;

insert into foo values (1, '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}');

select * from foo;

drop table foo;

-- jsonb type basic test
create table foo(a int, b jsonb) using mars;

insert into foo values (1, '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}');

select * from foo;

drop table foo;

-- varchar type basic test
create table foo(a int, b varchar(256)) using mars;
insert into foo values (1, '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}');
select * from foo;
drop table foo;

-- numeric type basic test
create table foo(a int, b numeric(10, 2)) using mars;
insert into foo values (1, 123.456);
select * from foo;
drop table foo;


-- text filter
create table foo(a int, b text) using mars;

insert into foo select i, CASE WHEN i = 50 THEN 'abcdefghijklmnopqrstuvwxyz' ELSE generate_random_string(25) END  from generate_series(1 , 4096) i;

select * from foo where b = 'abcdefghijklmnopqrstuvwxyz';

select * from foo where b in ('abcdefghijklmnopqrstuvwxyz');
select * from foo where b in ('abcdefghijklmnopqrstuvwxyz', 'xxx');

select exists (select * from foo where b < 'abcdefghijklmnopqrstuvwxyz');

select exists (select * from foo where b > 'abcdefghijklmnopqrstuvwxyz');

select exists (select * from foo where b like '%fghijklmnopqrst%');

drop table foo;

-- text column in option

create table foo (tag text, ts timestamp, col1 int, col2 float) using mars
with(tagkey = "tag", timekey= "ts", timebucket= "1 min");

drop table if exists foo;

-- verify text and varchar(n)
create table foo (c0 int, c1 text, c2 varchar(100)) using mars
 with (group_col_ = '{c0}') distributed by (c0);

insert into foo select i/5, ((i+3)%5)::text, ((i+2)%5)::varchar(100) from generate_series(0, 12) i;

select ctid, * from foo;

-- verify numeric and interval

drop table if exists foo;

create table foo (c0 int, c1 numeric, c2 interval) using mars
 with (group_col_ = '{c0}') distributed by (c0);

insert into foo select i/5, (i+3)%5, make_interval(mins => ((i+2)%5)) from generate_series(0, 12) i;

select ctid, * from foo;

drop table if exists foo;
