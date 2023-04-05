/*-------------------------------------------------------------------------
 *
 * mx_paths.h
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	    src/backend/optimizer/mx_paths.h
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MX_PATHS_H
#define MATRIXDB_MX_PATHS_H

#include "postgres.h"

#include "access/attnum.h"

typedef struct mono_proc_info
{
	Oid proc;
	AttrNumber equivalence_arg;
	Datum *equivalence_proc;
	int equivalence_proc_num;
} mono_proc_info;

#endif /* MATRIXDB_MX_PATHS_H */
