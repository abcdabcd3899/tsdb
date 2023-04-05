#ifndef __TELEMETRY_GUC_H__
#define __TELEMETRY_GUC_H__

#include <postgres.h>

extern bool telemetry_guc_disable_http;
void _telemetry_guc_init(void);
void _telemetry_guc_fini(void);

#endif /* __TELEMETRY_GUC_H__ */
