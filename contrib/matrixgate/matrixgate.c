/*-------------------------------------------------------------------------
 *
 * matrixgate.c
 *	  Provides supportive functions/workers to MatrixGate.
 *	  However, as of MatrixDB 3.0 GA, mxgate executable does not rely on this
 *	  library to work, means mxgate can still work with open-source GPDB.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixgate/matrixgate.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "fmgr.h"

#include "matrixgate.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);
extern void _PG_fini(void);

void
_PG_init(void)
{
	/* Launch the warden process on master */
	if (Gp_role == GP_ROLE_DISPATCH)
		mxgate_start_warden();
}

void
_PG_fini(void)
{
}
