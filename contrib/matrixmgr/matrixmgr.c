/*-------------------------------------------------------------------------
 *
 * matrixmgr.c
 *	  MatrixManager Extension for MatrixDB
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixmgr/matrixmgr.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdio.h>
#include <string.h>

#include "access/table.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/event_trigger.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "port.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mxmgr_create_meta_table);
PG_FUNCTION_INFO_V1(mxmgr_cmd_run);
PG_FUNCTION_INFO_V1(mxmgr_uninstall);

#define MXMGR_CONFIG_TABLE_NAME "matrix_manager_config"
#define MAX_READ_LINES 100
#define READ_SIZE 512

extern void _PG_init(void);
extern void _PG_fini(void);

/*
 * Function creates master-only table "matrix_manager_config".
 *
 * It has definition on both QD and QE with same relid and type_oid; when insert
 * or selected, planner treat it as local only table; when drop/alter table, it
 * will raise MPP operation to update on all segments.
 */
Datum
mxmgr_create_meta_table(PG_FUNCTION_ARGS)
{
	Oid meta_oid;
	TupleDesc tupdesc;
	ObjectAddress typaddress;
	Oid nspoid = InvalidOid;
	Oid rel_oid = InvalidOid;
	Oid type_oid = InvalidOid;
	Oid arr_oid = InvalidOid;
	Name nspname = PG_GETARG_NAME(0);
	char *arr_type_name = makeArrayTypeName(MXMGR_CONFIG_TABLE_NAME, nspoid);

	if (NameStr(*nspname)[0])
		nspoid = get_namespace_oid(nspname->data, false);

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		OidAssignment *key;
		List *dispatch_oids = NIL;

		/* On segments OIDs are dispatched from master. */
		rel_oid = PG_GETARG_OID(1);
		type_oid = PG_GETARG_OID(2);
		arr_oid = PG_GETARG_OID(3);

		key = makeNode(OidAssignment);
		key->type = T_OidAssignment;
		key->catalog = TypeRelationId;
		key->objname = arr_type_name;
		key->namespaceOid = nspoid;
		key->oid = arr_oid;

		dispatch_oids = lappend(dispatch_oids, key);

		/*
		 * We fill the global variable for holding master dispatched OIDs. Then
		 * following heap_create_with_catalog() can find then to create table
		 * correctly.
		 */
		AddPreassignedOids(dispatch_oids);
	}

	tupdesc = CreateTemplateTupleDesc(4);

	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "category", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "key", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "value", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "desc", TEXTOID, -1, 0);

	meta_oid = heap_create_with_catalog(MXMGR_CONFIG_TABLE_NAME,
										nspoid,
										DEFAULTTABLESPACE_OID,
										rel_oid,
										type_oid,
										InvalidOid,
										MyProc->roleId,
										HEAP_TABLE_AM_OID,
										tupdesc,
										NIL,
										RELKIND_RELATION,
										RELPERSISTENCE_PERMANENT,
										false,
										false,
										ONCOMMIT_NOOP,
										NULL, /* GP Policy */
										(Datum) 0,
										false,
										false,
										false,
										InvalidOid,
										&typaddress,
										false);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char *sql;

		type_oid = typaddress.objectId;

		/* Make created table visible to SysCache. */
		CommandCounterIncrement();

		/* Get the type Oid of the array type just created. */
		arr_oid = GetSysCacheOid2(TYPENAMENSP,
								  Anum_pg_type_oid,
								  CStringGetDatum(arr_type_name),
								  ObjectIdGetDatum(nspoid));

		/*
		 * Raise query to segments, create the talbe with same OIDs. Although we
		 * don't need to store tuple to segments, it is a must to have same
		 * tables on segment, otherwise transaction will fail.
		 */
		sql =
			psprintf("SELECT matrixmgr_internal.mxmgr_create_meta_table('%s', "
					 "%d, %d, %d)",
					 NameStr(*nspname),
					 meta_oid,
					 type_oid,
					 arr_oid);

		CdbDispatchCommand(sql, DF_CANCEL_ON_ERROR | DF_WITH_SNAPSHOT, NULL);

		/*
		 * Clear the global OIDs marked as need dispatching. This is safe
		 * because we have dispatched them to segments to create tables.
		 */
		GetAssignedOidsForDispatch();
	}

	PG_RETURN_BOOL(true);
}

/*
 * mxmgr_cmd_run() executes an external command and print its STDOUT as NOTICE.
 */
Datum
mxmgr_cmd_run(PG_FUNCTION_ARGS)
{
	FILE *file;
	char exec_path[MAXPGPATH];
	StringInfo cmdresult = makeStringInfo();
	StringInfo cmdstr = makeStringInfo();
	char readline[READ_SIZE] = "";
	char *pathname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *args = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *alias = text_to_cstring(PG_GETARG_TEXT_PP(2));

	/* Remove special symbols, pathname can change length here. */
	canonicalize_path(pathname);

	/*
	 * Absolute path is not allowed. Path cannot contain any '../' or '/../'.
	 * Can only execute command below $MatrixDB/bin.
	 */
	if (is_absolute_path(pathname))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("absolute path not allowed")));
	else if (path_contains_parent_reference(pathname))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("path must be in or below the current directory")));

	/* Get execute directory from global var 'my_exec_path'. */
	strlcpy(exec_path, my_exec_path, MAXPGPATH);
	get_parent_directory(exec_path);
	/* Concat exec_path, binary and args. */
	appendStringInfo(cmdstr, "%s/%s %s", exec_path, pathname, args);

	/*
	 * Execute command in shell and redirect STDOUT to a pipe, Only read top
	 * STACK_DEPTH_MAX lines from the pipe. Each line read SYMBOL_SIZE bytes.
	 */
	file = popen(cmdstr->data, "r");
	if (file == NULL)
		PG_RETURN_BOOL(false);
	for (size_t i = 0; i < MAX_READ_LINES; ++i)
	{
		/* Get one line of the result from STDOUT. */
		if (fgets(readline, READ_SIZE, file) == NULL)
			break;
		appendStringInfo(cmdresult, "%s", readline);
	}

	pclose(file);
	/* Print STDOUT of command to NOTICE */
	elog(NOTICE, "(%s)%s", alias, cmdresult->data);

	PG_RETURN_BOOL(true);
}

/*
 * Function for handling DROP EXTENSION
 */
Datum
mxmgr_uninstall(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	/*
	 * Disallow direct call to this UDF on master. This function should only be
	 * triggered by DROP EXTENSION.
	 */
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	trigdata = (EventTriggerData *) fcinfo->context;

	if (IsA(trigdata->parsetree, DropStmt))
	{
		DropStmt *stmt = (DropStmt *) trigdata->parsetree;

		if (stmt->removeType == OBJECT_EXTENSION)
		{
			Node *object = linitial(stmt->objects);

			if (object == NULL)
				goto DONE;

			if (nodeTag(object) != T_String)
				goto DONE;

			if (strncmp(strVal(object), "matrixmgr", NAMEDATALEN) != 0)
				goto DONE;

			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("DROP EXTENSION aborted"),
					 errdetail("The extension matrixmgr cannot be dropped in "
							   "this version."),
					 errhint("If need to stop system metrics collection, run "
							 "\"SELECT public.mxmgr_remove_all('local');\"")));
		}
	}

DONE:
	PG_RETURN_NULL();
}

void
_PG_init(void)
{
	/*
	 * This is reserved in MatrixDB 3.0-GA.
	 *
	 * We register matrixmgr.so to shared_preload_libraries since 3.0. In future
	 * versions we can add actual codes here without letting users manual change
	 * shared_preload_libraries and restart MatrixDB.
	 */
}

void
_PG_fini(void)
{
}
