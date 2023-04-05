/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/mxkv_in.h
 *	  Convert text to mxkv bytea.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_MXKV_IN_H
#define MXKV_MXKV_IN_H

#include "postgres.h"

#include "mxkv/config.h"
#include "mxkv/mxkv_iter.h"

extern bytea *mxkv_serialize(MxkvIter *iter, const MxkvConfig *config);
extern bytea *mxkv_in(char *json, const MxkvVType *vtype);

#endif /* MXKV_MXKV_IN_H */
