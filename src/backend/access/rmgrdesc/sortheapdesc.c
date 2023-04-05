/*-------------------------------------------------------------------------
 *
 * sortheapdesc.c
 *	  rmgr descriptor routines for sort heap access method
 *
 * Copyright (c) 2012-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/rmgrdesc/sortheapdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "cdb/cdbsortheapxlog.h"

void sortheap_desc(StringInfo buf, XLogReaderState *record)
{
	BlockNumber blkno;
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_SORTHEAP_DUMP:
	{
		xl_sortheap_dump *xlrec = (xl_sortheap_dump *)XLogRecGetData(record);
		appendStringInfo(buf, "dump tuples to tape %d", xlrec->destTape);
		break;
	}
	case XLOG_SORTHEAP_TAPEEXTEND:
	{
		XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);
		appendStringInfo(buf, "extend tape with new data block %d", blkno);
		break;
	}
	case XLOG_SORTHEAP_FULLPAGEUPDATE:
	{
		XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);
		appendStringInfo(buf, "full page update on block %d", blkno);
		break;
	}
	case XLOG_SORTHEAP_EXTEND:
	{
		appendStringInfo(buf, "extend the sortheap file to length %ld",
						 *(long *)XLogRecGetData(record));
		break;
	}
	case XLOG_SORTHEAP_NEWTAPESORT:
	{
		XLogRecGetBlockTag(record, 1, NULL, NULL, &blkno);
		appendStringInfo(buf, "new tapesort header %d", blkno);
		break;
	}
	case XLOG_SORTHEAP_NEWHEADER:
	{
		XLogRecGetBlockTag(record, 1, NULL, NULL, &blkno);
		appendStringInfo(buf, "new tape header %d", blkno);
		break;
	}
	case XLOG_SORTHEAP_RECORD_ALLOCATED:
	{
		blkno = *(long *)(XLogRecGetData(record) +
						  2 * sizeof(bool) +
						  sizeof(long));

		appendStringInfo(buf, "record new block %d is allocated by tape", blkno);
		break;
	}
	case XLOG_SORTHEAP_MERGE_DONE:
	{
		appendStringInfo(buf, "merge done");
		break;
	}
	case XLOG_SORTHEAP_VACUUM_DONE:
	{
		appendStringInfo(buf, "vacuum done");
		break;
	}
	case XLOG_SORTHEAP_VACUUMTAPE:
	{
		appendStringInfo(buf, "vacuum tape %d done", *(int *)XLogRecGetData(record));
		break;
	}
	case XLOG_SORTHEAP_RELEASE_AUXBLK:
	{
		appendStringInfo(buf, "recycle data/btree pages");
		break;
	}
	default:
		appendStringInfo(buf, "UNKNOWN");
		break;
	}
}

const char *
sortheap_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
	default:
		id = "sortheap";
	}

	return id;
}

void sortheap_desc2(StringInfo buf, XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_SORTHEAP_FREEPAGE_ALLOC:
	{
		appendStringInfo(buf, "freepage: allocated new freeblock");
		break;
	}
	case XLOG_SORTHEAP_FREEPAGE_RECYC:
	{
		appendStringInfo(buf, "freepage: recycle a block");
		break;
	}
	case XLOG_SORTHEAP_FREEPAGE_NEWROOT:
	{
		appendStringInfo(buf, "freepage: new root block");
		break;
	}
	case XLOG_SORTHEAP_FREEPAGE_NEWLEAF:
	{
		appendStringInfo(buf, "freepage: new leaf block");
		break;
	}
	default:
		appendStringInfo(buf, "UNKNOWN");
		break;
	}
}

const char *
sortheap_identify2(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
	default:
		id = "sortheap2";
	}

	return id;
}
