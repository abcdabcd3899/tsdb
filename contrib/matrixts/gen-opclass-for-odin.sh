#!/usr/bin/env bash

: ${PGPORT:=4000}
: ${my_name:=odin}
: ${am_name:=btree}

Q()
{
    PGOPTIONS="-c gp_role=utility" \
	psql -P pager=off -p $PGPORT -d postgres -tqA -F ' ' "$@"
}

am_oid=$(Q -c "select oid from pg_am where amname='$am_name'")

cat <<EOF

--
-- operator classes to support btree operations
--

EOF

Q -c "select opcname, opcfamily, opcintype
        from pg_opclass
       where opcmethod = $am_oid
         and opcdefault" \
| while read -r opcname opcfamily opcintype; do
    typname=$(Q -c "select $opcintype::regtype")

    cat <<EOF
CREATE OPERATOR CLASS $opcname DEFAULT FOR TYPE $typname USING $my_name
EOF

    Q <<EOF
select '    AS OPERATOR ' || amopstrategy || ' ' || amopopr::regoperator
  from pg_amop
 where amopfamily = $opcfamily
   and $opcintype in (amoplefttype, amoprighttype)
 order by amopstrategy, amoplefttype, amoprighttype
 limit 1
;

select '     , OPERATOR ' || amopstrategy || ' ' || amopopr::regoperator
  from pg_amop
 where amopfamily = $opcfamily
   and $opcintype in (amoplefttype, amoprighttype)
 order by amopstrategy, amoplefttype, amoprighttype
offset 1
;
EOF

    # also need the btree order (cmp) functions
    Q <<EOF
select '     , FUNCTION ' || (amprocnum)
    || ' (' || amproclefttype::regtype || ',' || amprocrighttype::regtype || ') '
    || amproc::regprocedure
  from pg_amproc
  join pg_opclass
    on amprocfamily = opcfamily
   and opcdefault
  join pg_am
    on opcmethod = pg_am.oid
   and amname = 'btree'
 where amprocnum = 1
   and opcintype = $opcintype
   and opcintype in (amproclefttype, amprocrighttype)
 order by opcmethod, amproclefttype, amprocrighttype
;
EOF

    echo ";"
    echo
  done

# vi: et :
