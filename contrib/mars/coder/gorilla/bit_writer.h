/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/gorilla/bit_writer.h
 *	  Bit writer.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef GORILLA_BIT_WRITER_H
#define GORILLA_BIT_WRITER_H

#include "postgres.h"

typedef uint8 BufferType;

typedef struct BitWriter
{
	/*
	 * buffer_overflow
	 *  if true : no writable space left.
	 */
	bool buffer_overflow;

	/*
	 * dest is target space address.
	 */
	char *dest_begin;
	char *dest_current;
	char *dest_end;

	/* current writing buffer. */
	BufferType bits_buffer;

	/* valid bit size in current writing buffer. */
	uint8 bits_count;
} BitWriter;

extern void bitwriter_construct(BitWriter *bit_writer, char *begin, char *end);
extern void bitwriter_write_bits(BitWriter *bit_writer,
								 uint8 to_write_bits_size, uint64 value);
extern void bitwriter_flush(BitWriter *bit_writer);

#endif /* GORILLA_BIT_WRITER_H */
