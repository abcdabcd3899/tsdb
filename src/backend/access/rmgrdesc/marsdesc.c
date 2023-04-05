/*-------------------------------------------------------------------------
 *
 * marsdesc.c
 *	  rmgr descriptor routines for mars
 *
 * Copyright (c) 2012-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/rmgrdesc/marsdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbmarsxlog.h"

void mars_desc(StringInfo buf, XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_MARS_INSERT:
	{
		xl_mars_insert *xlrec = (xl_mars_insert *)XLogRecGetData(record);

		appendStringInfo(
			buf,
			"insert: rel %u/%u/%u seg/offset:%u/" INT64_FORMAT " len:%lu",
			xlrec->target.node.spcNode, xlrec->target.node.dbNode,
			xlrec->target.node.relNode, xlrec->target.segment_filenum,
			xlrec->target.offset, XLogRecGetDataLen(record) - SizeOfMARSInsert);
	}
	break;
	default:
		appendStringInfo(buf, "UNKNOWN");
		break;
	}
}

const char *
mars_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
	case XLOG_MARS_INSERT:
		id = "MARS_INSERT";
		break;
	}

	return id;
}
