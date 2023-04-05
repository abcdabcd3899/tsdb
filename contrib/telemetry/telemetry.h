#ifndef MATRIXDB_TELEMETRY_H
#define MATRIXDB_TELEMETRY_H
#include "postgres.h"

#include "conn.h"
#include "http.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"

#ifdef USE_OPENSSL
	#define TELEMETRY_SCHEME "https"
#else
	#define TELEMETRY_SCHEME "http"
#endif
#define TELEMETRY_VERSION "3.0.0-1.0.0"
#define UUID_FILE ".matrix_uuid"
#define UUID_SIZE 36
#define MONITOR_BAGLOG ".monitor_backlog"

#define TELEMETRY_HOST "ymatrix.cn"
#define TELEMETRY_PATH "/telemetry/v1"
#define TELEMETRY_PORT 0
#define TMLOGLEVEL DEBUG4
#define MONITOR_TIME_GAP 12 * 60 * 60 * 1000L // 12hours

/*
 * TELEMETRY HTTP SERVER STATUS CODE
 *
 * [ASSIGN_NEW_CLUSTERID] [HTTP-StatusOK-200]
 * When first create a cluster, TELEMETRY Server will allocate
 * a new cluster ID to it.
 */
#define ASSIGN_NEW_CLUSTERID 200
/*
 * [REPORT_SUCESS] [HTTP-StatusNoContent-204]
 * For clusters that have been assigned Cluster IDs, each success
 * to report, client will receive status code REPORT_SUCESS.
 */
#define REPORT_SUCESS 204

/* params for local test */
//#define TELEMETRY_HOST "127.0.0.1"
//#define TELEMETRY_PATH "/telemetry/v1"
//#define TELEMETRY_PORT 7777
//#define TMLOGLEVEL WARNING
//#define MONITOR_TIME_GAP 10 * 1000L // 10 seconds

extern void _PG_init(void);
extern void _PG_fini(void);

/*
 *  Background worker building functions.
 * */
extern void telemetry_start(void);
extern void telemetry_bgworker_main(Datum main_arg);
extern void telemetry_monitor_wrapper(void);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  MatrixDB via HTTPS.
 */
extern void telemetry_main(const char *host, const char *path, const char *service);
extern void telemetry_main_wrapper(void);

/*
 * Telemetry monitor collect functions.
 * */
extern void telemetry_gpversion(void);
extern bool get_segment_configuration(StringInfo monitor_jtext);
extern void save_monitor_collection_to_local(StringInfo monitor_jtext);
extern bool save_matrix_uuid_local(const char *uuid);
extern Datum telemetry_monitor(PG_FUNCTION_ARGS);

/*
 * Telemetry http connect and request functions.
 * */
extern HttpRequest *build_telemetry_request(const char *host, const char *path, StringInfo monitor_jtext);
extern Connection *telemetry_connect(const char *host, const char *service);

#endif //MATRIXDB_TELEMETRY_H
