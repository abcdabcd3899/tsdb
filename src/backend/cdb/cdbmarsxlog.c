/*-------------------------------------------------------------------------
 *
 * cdbmarsxlog.c
 *
 * Portions Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbmarsxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/aomd.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "cdb/cdbmarsxlog.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "utils/faultinjector.h"
#include "utils/faultinjector_lists.h"

#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/file.h>

#define register_dirty_segment_mars(rnode, segno, vfd) register_dirty_segment_ao(rnode, segno, vfd)

void xlog_mars_insert(RelFileNode relFileNode, int32 segmentFileNum,
					  int64 offset, void *buffer, int32 bufferLen)
{
	xl_mars_insert xlmarsinsert;

	xlmarsinsert.target.node = relFileNode;
	xlmarsinsert.target.segment_filenum = segmentFileNum;
	xlmarsinsert.target.offset = offset;

	XLogBeginInsert();
	XLogRegisterData((char *)&xlmarsinsert, SizeOfMARSInsert);

	if (bufferLen != 0)
		XLogRegisterData((char *)buffer, bufferLen);

	SIMPLE_FAULT_INJECTOR("xlog_mars_insert");

	XLogInsert(RM_MARS_ID, XLOG_MARS_INSERT);

	wait_to_avoid_large_repl_lag();
}

static void
mars_insert_replay(XLogReaderState *record)
{
	char *dbPath;
	char path[MAXPGPATH];
	int written_len;
	File file;
	int fileFlags;
	xl_mars_insert *xlrec = (xl_mars_insert *)XLogRecGetData(record);
	char *buffer = (char *)xlrec + SizeOfMARSInsert;
	uint32 len = XLogRecGetDataLen(record) - SizeOfMARSInsert;

	dbPath = GetDatabasePath(xlrec->target.node.dbNode,
							 xlrec->target.node.spcNode);

	if (xlrec->target.segment_filenum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, xlrec->target.node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, xlrec->target.node.relNode, xlrec->target.segment_filenum);
	pfree(dbPath);

	fileFlags = O_RDWR | PG_BINARY;

	/* When writing from the beginning of the file, it might not exist yet. Create it. */
	if (xlrec->target.offset == 0)
		fileFlags |= O_CREAT;
	file = PathNameOpenFile(path, fileFlags);

	if (file < 0)
	{
		XLogAOSegmentFile(xlrec->target.node, xlrec->target.segment_filenum);
		return;
	}

	written_len = FileWrite(file, buffer, len, xlrec->target.offset,
							WAIT_EVENT_COPY_FILE_WRITE);
	if (written_len < 0 || written_len != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to write %d bytes in file \"%s\": %m",
						len,
						path)));
	}

	register_dirty_segment_mars(xlrec->target.node,
								xlrec->target.segment_filenum,
								file);

	FileClose(file);
}

void mars_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/*
	 * Perform redo of AO XLOG records only for standby mode. We do
	 * not need to replay AO XLOG records in normal mode because fsync
	 * is performed on file close.
	 */
	if (!IsStandbyMode())
		return;

	switch (info)
	{
	case XLOG_MARS_INSERT:
		mars_insert_replay(record);
		break;
	default:
		elog(PANIC, "appendonly_redo: unknown code %u", info);
	}
}
