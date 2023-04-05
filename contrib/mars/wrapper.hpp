/*-------------------------------------------------------------------------
 *
 * wrapper.hpp
 *	  MARS storage related postgresql header files
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/wrapper.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_WRAPPER_HPP
#define MARS_WRAPPER_HPP

extern "C"
{

#include "postgres.h"

#include "access/amapi.h"
#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_operator.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/toasting.h"
#include "cdb/cdbgroupingpaths.h"
#include "cdb/cdbmarsxlog.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeIndexscan.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "port.h"
#include "storage/backendid.h"
#include "storage/lmgr.h"
#include "storage/lockdefs.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "catalog/mx_mars.h"

#include "coder/gorilla/gorilla.h"
#include "coder/gorilla/delta_delta.h"
#include "coder/lz4/lz4_compression.h"
#include "coder/zstd/zstd_compression.h"
#include "mars_planner.h"

#undef Abs
#undef DAY
#undef IsPowerOf2
#undef NIL
#undef SECOND

    using Oid = ::Oid;
    using TupleTableSlot = ::TupleTableSlot;
    using Relation = ::Relation;
    using AttrNumber = ::AttrNumber;

} // extern "C"

#endif // MARS_WRAPPER_HPP
