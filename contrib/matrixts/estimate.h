#ifndef __MATRIXTS_ESTIMATE_H__
#define __MATRIXTS_ESTIMATE_H__ 

#include <postgres.h>

#define INVALID_ESTIMATE (-1)
#define IS_VALID_ESTIMATE(est) ((est) >= 0)

extern double ts_estimate_group_expr_interval(PlannerInfo *root, Expr *expr,
											  double interval_period);
extern double ts_estimate_group(PlannerInfo *root, double path_rows);

#endif /* __MATRIXTS_ESTIMATE_H__ */
