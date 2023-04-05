--
-- BFV, bugfix verification
--

--
-- bfv: runtime keys were not evaluated in non-join prepared statements.
--
-- we used to believe that the runtime keys only occur in joining, so we only
-- evaluate the runtime keys in rescan, however it is also possible in prepared
-- statements, so mars see unevaluated runtime keys and generate empty results.
--
-- it is fixed by evaluating the runtime keys also in non-join statements.
--
-- note: to triggere the issue we has to execute the same prepared statement
-- multiple times, in the first several times the plan cache forces the
-- non-cached plan to be executed several times to evaulate the costs, after
-- this warm-up stage the cached plan gets the chance to be picked.
--

\set tname text_mars_bfv_runtimekeys_of_prepared_stmts_t1
\set pname :tname '_p1'

drop table if exists :tname;
create table :tname (c1 int)
 using mars
 distributed by (c1);

insert into :tname values (10);

prepare :pname (int) as
 select * from :tname where c1 = $1;

execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);
execute :pname (10);

-- vi: et :
