-- start_ignore
select gp_debug_set_create_table_default_numsegments(1);

drop table if exists foo;
-- end_ignore

create or replace function preagg_sum_compare() returns setof record as $$
declare
sum_a numeric;
sum_b numeric;
sum_c numeric;
sum_d numeric;
my_sum text[];
begin
	execute 'select sum(a), sum(b), sum(c), sum(d) from foo' into 
	sum_a, sum_b, sum_c, sum_d; 

	select ARRAY(select sum as my_sum
		from mars.preagg_sum('foo'::regclass)
		where batch = 1
		order by colid ) into my_sum;

	return query select sum_a = my_sum[1]::numeric , sum_b = my_sum[2]::numeric, abs(sum_c - my_sum[3]::numeric) < 1, abs(sum_d - my_sum[4]::numeric) < 1;
	
end; $$ language plpgsql volatile;

CREATE TABLE foo(a int, b bigint, c float4, d float8) using mars;

INSERT INTO foo SELECT i, i * 100, i * random(), i * 100 * random() FROM generate_series(1, 1024) i;

select * from preagg_sum_compare() f(a bool, b bool, c bool, d bool);
