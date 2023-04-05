/*-------------------------------------------------------------------------
 *
 * contrib/gorilla/delta_delta.h
 *	  delta delta encoding method.
 *
 * This implements the Facebook Gorilla [1] encoding.
 *
 * [1]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_DELTA_DELTA_H
#define MATRIXDB_DELTA_DELTA_H

#include "postgres.h"

extern int32 delta_delta_compress_data(const char *source, uint32 source_size,
									   char *dest, uint32 dst_size);

extern uint32 delta_delta_decompress_data(const char *source, uint32 source_size,
										  char *dest);

#endif /* MATRIXDB_DELTA_DELTA_H */
