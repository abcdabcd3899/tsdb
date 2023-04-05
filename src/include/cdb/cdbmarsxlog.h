/*-------------------------------------------------------------------------
 *
 * cdbmarsxlog.h
 *
 * Portions Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbmarsxlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_CDBMARSXLOG_H
#define MATRIXDB_CDBMARSXLOG_H

#include "c.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"

#define XLOG_MARS_INSERT 0x00

typedef struct
{
	RelFileNode node;
	uint32 segment_filenum;
	int64 offset;
} xl_mars_target;

#define SizeOfMARSTarget (offsetof(xl_mars_target, offset) + sizeof(int64))

typedef struct
{
	/* meta data about the inserted block of AO data*/
	xl_mars_target target;
	/* BLOCK DATA FOLLOWS AT END OF STRUCT */
} xl_mars_insert;

#define SizeOfMARSInsert (offsetof(xl_mars_insert, target) + SizeOfMARSTarget)

extern void xlog_mars_insert(RelFileNode relFileNode, int32 segmentFileNum,
							 int64 offset, void *buffer, int32 bufferLen);

extern void mars_redo(XLogReaderState *record);

/* in marsdesc.c */
extern void mars_desc(StringInfo buf, XLogReaderState *record);
extern const char *mars_identify(uint8 info);
#endif // MATRIXDB_CDBMARSXLOG_H
