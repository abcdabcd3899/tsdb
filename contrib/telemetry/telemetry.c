/*-------------------------------------------------------------------------
 * telemetry.c
 *	  Text search telemetry dictionary
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/telemetry/telemetry.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "postmaster/bgworker.h"
#include "utils/fmgrtab.h"

#include "telemetry.h"
#include "conn_plain.h"
#include "guc.h"

PG_MODULE_MAGIC;

void _PG_init(void)
{
	_telemetry_guc_init();
	/* Launch the warden process on master */
	if (Gp_role == GP_ROLE_DISPATCH)
		telemetry_start();
}

void _PG_fini(void)
{
	_telemetry_guc_fini();
}

void telemetry_main_wrapper()
{
	return telemetry_main(TELEMETRY_HOST, TELEMETRY_PATH, TELEMETRY_SCHEME);
}

void telemetry_main(const char *host, const char *path, const char *service)
{
	HttpError err;
	Connection *conn;
	HttpRequest *req;
	HttpResponseState *rsp;
	const char *volatile json = NULL;
	StringInfo monitor_jtext = makeStringInfo();
	// query all telemetry data

	StartTransactionCommand();

	/* Query all telemetry collection by UDF */
	get_segment_configuration(monitor_jtext);

	/*
	 * When guc disable http, we append collection to local file,
	 * commit transaction immediately.
	 * */
	if (telemetry_guc_disable_http)
	{
		save_monitor_collection_to_local(monitor_jtext);
		CommitTransactionCommand();
		return;
	}
	conn = telemetry_connect(host, service);

	/*
	 * If fail to connect http-server or guc disable http,
	 * append collection to local file and abort current transaction.
	 * */
	if (conn == NULL)
	{
		elog(TMLOGLEVEL, "telemetry fail to http connect");
		save_monitor_collection_to_local(monitor_jtext);
		AbortCurrentTransaction();
		return;
	}

	/* Send http post request and receive response. */
	req = build_telemetry_request(host, path, monitor_jtext);

	rsp = ts_http_response_state_create();

	err = ts_http_send_and_recv(conn, req, rsp);

	/* Destroy http connection. */
	ts_http_request_destroy(req);
	ts_connection_destroy(conn);

	if (err != HTTP_ERROR_NONE)
	{
		elog(TMLOGLEVEL, "telemetry error: %s", ts_http_strerror(err));
		AbortCurrentTransaction();
		return;
	}

	if (!ts_http_response_state_valid_status(rsp))
	{
		elog(TMLOGLEVEL,
			 "telemetry got unexpected HTTP response status: %d",
			 ts_http_response_state_status_code(rsp));
		AbortCurrentTransaction();
		return;
	}

	/*
	 * Response is the body of a well-formed HTTP response,
	 * since otherwise the previous line will throw an error.
	 */
	PG_TRY();
	{
		int status_code;

		status_code = ts_http_response_state_status_code(rsp);
		switch (status_code)
		{
		case ASSIGN_NEW_CLUSTERID:
			/*
			 * [ASSIGN_NEW_CLUSTERID]
			 * When the cluster created first time, TELEMETRY Server will allocate
			 * a new cluster ID to it.
			 */
			json = ts_http_response_state_body_start(rsp);
			elog(TMLOGLEVEL, "(response)<%s>\n", json);

			/*
			 * The response is something like {"data": "xxx"}, in psql we could do
			 * the job as below:
			 *
			 *		SELECT '{"data": "xxx"}'::json->'data';
			 *
			 * The operator "->(json,text)" is implemented via json_object_field(),
			 * we can call it manually to have the job done.
			 */
			if (json && *json)
			{
				Datum jsonDatum;
				Datum keyDatum;
				Datum uuidDatum;
				char *uuid;

				/*
				 * We are in a temporary memory context already, no need to worry
				 * about memory leaks.
				 */
				jsonDatum = CStringGetTextDatum((const char *)json);
				keyDatum = CStringGetTextDatum("data");
				uuidDatum = DirectFunctionCall2(json_object_field_text,
												jsonDatum, keyDatum);
				uuid = TextDatumGetCString(uuidDatum);

				elog(TMLOGLEVEL, "Checking uuid %s\n", uuid);
				save_matrix_uuid_local(uuid);
			}
			break;
		case REPORT_SUCESS:
			/*
			 * [REPORT_SUCESS]
			 * For clusters that have been assigned Cluster IDs, after successful
			 * report, will receive status code REPORT_SUCESS.
			 * Report success, no special handling.
			 * Fallthrough.
			 */
		default:
			/*
			 * Receive an exception response, the server should handle it,
			 * but the client should keep silent.
			 */
			break;
		}
	}
	PG_CATCH();
	{
		/* If the response is malformed, ts_check_version_response() will
		 * throw an error, so we capture the error here and print debugging
		 * information before re-throwing the error. */
		ereport(TMLOGLEVEL,
				(errmsg("malformed telemetry response body"),
				 errdetail("host=%s, service=%s, path=%s: %s",
						   host,
						   service,
						   path,
						   json ? json : "<EMPTY>")));
		PG_RE_THROW();
	}
	PG_END_TRY();
	ts_http_response_state_destroy(rsp);
	CommitTransactionCommand();
	return;
}

Connection *
telemetry_connect(const char *host, const char *service)
{
	int ret;
	Connection *conn = NULL;

	if (strcmp("http", service) == 0)
	{
		_conn_plain_init();
		conn = ts_connection_create(CONNECTION_PLAIN);
	}
	else if (strcmp("https", service) == 0)
	{
		_conn_ssl_init();
		conn = ts_connection_create(CONNECTION_SSL);
	}
	else
		ereport(TMLOGLEVEL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("scheme \"%s\" not supported for telemetry", service)));

	if (conn == NULL)
		return NULL;

	ret = ts_connection_connect(conn, host, service, TELEMETRY_PORT);

	if (ret < 0)
	{
		const char *errstr = ts_connection_get_and_clear_error(conn);

		ts_connection_destroy(conn);
		conn = NULL;

		ereport(TMLOGLEVEL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("telemetry could not connect to \"%s\"", host),
				 errdetail("%s", errstr)));
	}
	return conn;
}

HttpRequest *
build_telemetry_request(const char *host, const char *path, StringInfo monitor_jtext)
{
	char body_len_string[5];
	HttpRequest *req;

	snprintf(body_len_string, 5, "%d", monitor_jtext->len);
	/* Fill in HTTP request */
	req = ts_http_request_create(HTTP_POST);

	ts_http_request_set_uri(req, path);
	ts_http_request_set_version(req, HTTP_VERSION_10);
	ts_http_request_set_header(req, HTTP_CONTENT_TYPE, TIMESCALE_TYPE);
	ts_http_request_set_header(req, HTTP_CONTENT_LENGTH, body_len_string);
	ts_http_request_set_header(req, HTTP_HOST, host);
	ts_http_request_set_body(req, monitor_jtext->data, monitor_jtext->len);

	return req;
}

bool get_segment_configuration(StringInfo monitor_jtext)
{
	int res;
	bool isnull = true;
	int tuple_num;

	if (SPI_OK_CONNECT != SPI_connect_ext(0))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to connect to execute internal query")));
	PushActiveSnapshot(GetTransactionSnapshot());
	res = SPI_execute("select * from collect_gp_segment_configuration();", true, 0);
	PopActiveSnapshot();

	tuple_num = (int)SPI_processed;
	if (res == SPI_OK_SELECT && SPI_tuptable != NULL)
	{

		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		if (tuple_num > 0)
		{
			HeapTuple tuple;
			Datum d;

			tuple = tuptable->vals[0];
			d = heap_getattr(tuple, 1, tupdesc, &isnull);
			char *tmp_result = TextDatumGetCString(d);
			appendStringInfo(monitor_jtext, "%s", tmp_result);
		}
	}

	SPI_finish();
	return isnull;
}

bool save_matrix_uuid_local(const char *uuid)
{
	FILE *file = fopen(UUID_FILE, "w");

	if (file == NULL)
		return false;
	fwrite(uuid, sizeof(char), (size_t)UUID_SIZE, file);
	fclose(file);
	return true;
}

void save_monitor_collection_to_local(StringInfo monitor_jtext)
{
	FILE *file = fopen(MONITOR_BAGLOG, "w");

	if (file == NULL)
	{
		return;
	}
	fwrite(monitor_jtext->data, 1, monitor_jtext->len, file);
	fclose(file);
}
