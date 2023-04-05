/*-------------------------------------------------------------------------
 *
 * contrib/gorilla/gorilla.h
 *	  The gorilla encoding method.
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
#ifndef MATRIXDB_GORILLA_H
#define MATRIXDB_GORILLA_H

#include "postgres.h"

extern int32 gorilla_compress_data(const char *source, uint32 source_size,
								   char *dest, uint32 dst_size);

extern uint32 gorilla_decompress_data(const char *source, uint32 source_size,
									  char *dest);

#endif /* MATRIXDB_GORILLA_H */
