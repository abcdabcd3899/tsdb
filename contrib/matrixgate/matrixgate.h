/*-------------------------------------------------------------------------
 *
 * matrixgate.h
 *	  Provides supportive functions/workers to MatrixGate.
 *	  However, as of MatrixDB 3.0 GA, mxgate executable does not rely on this
 *	  library to work, means mxgate can still work with open-source GPDB.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixgate/matrixgate.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __MATRIXGATE_C_H__
#define __MATRIXGATE_C_H__

#include "postgres.h"

extern void mxgate_start_warden(void);
extern void mxgate_warden_main(Datum main_arg);

#endif /* __MATRIXGATE_C_H__ */
