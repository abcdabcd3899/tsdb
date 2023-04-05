/*-------------------------------------------------------------------------
 *
 * mx_compress.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef MX_COMPRESS_H
#define MX_COMPRESS_H

#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_compression.h"
#include "fmgr.h"


extern PGFunction *mx_get_shared_compression_functions(char *compresstype);
extern void mx_unget_shared_compression_functions(PGFunction *funcs);
extern CompressionState *mx_get_shared_compression_state(bool compression,
														 char *compresstype,
														 int16 compresslevel,
														 int32 blocksize,
														 TupleDesc tupledesc);
extern void mx_unget_shared_compression_state(CompressionState *state);

#endif   /* MX_COMPRESS_H */
