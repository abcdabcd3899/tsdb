#ifndef __MATRIXTS_GUC_H__
#define __MATRIXTS_GUC_H__ 

#include <postgres.h>

extern bool ts_guc_disable_optimizations;
extern bool ts_guc_apm_allow_dml;
extern bool ts_guc_apm_allow_ddl;

extern int ts_guc_continuous_view_workmem;
void _guc_init(void);
void _guc_fini(void);

#endif /* __MATRIXTS_GUC_H__ */
