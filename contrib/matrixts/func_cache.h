#ifndef __MATRIXTS_FUNC_CACHE_H__
#define __MATRIXTS_FUNC_CACHE_H__ 

#include <postgres.h>
#include <nodes/primnodes.h>

#define FUNC_CACHE_MAX_FUNC_ARGS 10

typedef Expr *(*sort_transform_func)(FuncExpr *func);
typedef double (*group_estimate_func)(PlannerInfo *root, FuncExpr *expr, double path_rows);

typedef struct FuncInfo
{
	const char *funcname;
	bool is_matrixts_func;
	bool is_bucketing_func;
	int nargs;
	Oid arg_types[FUNC_CACHE_MAX_FUNC_ARGS];
	group_estimate_func group_estimate;
	sort_transform_func sort_transform;
} FuncInfo;

extern FuncInfo *ts_func_cache_get(Oid funcid);
extern FuncInfo *ts_func_cache_get_bucketing_func(Oid funcid);

#endif /* __MATRIXTS_FUNC_CACHE_H__ */
