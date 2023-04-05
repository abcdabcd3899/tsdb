/*-------------------------------------------------------------------------
 *
 * cdbsortheapxlog.h
 *	  Sort Heap XLog routines.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/include/cdb/cdbsortheapxlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBSORTHEAPXLOG_H
#define CDBSORTHEAPXLOG_H

/*
 *  WAL record definitions for sortheapam.c's WAL operations
 *
 *  XLOG allows to store some information in high 4 bits of log
 *  record xl_info field.  We use 3 for opcode and one for init bit.
 *
 *  4 bits can only store about 16 xlog type and sortheap need more types of
 *  xlog, so we use two RM to record the xlog for sortheap
 */
#define XLOG_SORTHEAP_DUMP 0x00
#define XLOG_SORTHEAP_EXTEND 0x10
#define XLOG_SORTHEAP_TAPEEXTEND 0x20
#define XLOG_SORTHEAP_RUNSMETABLK 0x30
#define XLOG_SORTHEAP_FULLPAGEUPDATE 0x40
#define XLOG_SORTHEAP_NEWHEADER 0x50
#define XLOG_SORTHEAP_NEWTAPESORT 0x60
#define XLOG_SORTHEAP_RECORD_ALLOCATED 0x70
#define XLOG_SORTHEAP_VACUUMTAPE 0x80
#define XLOG_SORTHEAP_MERGE_DONE 0x90
#define XLOG_SORTHEAP_VACUUM_DONE 0xA0
#define XLOG_SORTHEAP_RELEASE_AUXBLK 0xB0

/*
 * Part two
 */
#define XLOG_SORTHEAP_FREEPAGE_ALLOC 0x00
#define XLOG_SORTHEAP_FREEPAGE_RECYC 0x10
#define XLOG_SORTHEAP_FREEPAGE_NEWROOT 0x20
#define XLOG_SORTHEAP_FREEPAGE_NEWLEAF 0x30

/* This is what we need to know about dump */
typedef struct xl_sortheap_dump
{
	TransactionId lastInsertXid; /* xmax of the deleted tuple */
	int destTape;
	int newDestTape;
	int newCurrentRun;
	int newRuns;
	int newDummy;
} xl_sortheap_dump;

#define SizeOfSortHeapDump (offsetof(xl_sortheap_dump, newDummy) + sizeof(int))

typedef struct xl_sortheap_extend_bt
{
	int curRun;
	int level;
	int nprealloc;
	bool newlevel;
} xl_sortheap_extend_bt;

#define SizeOfSortHeapExtendBT (offsetof(xl_sortheap_extend_bt, newlevel) + sizeof(bool))

extern void sortheap_redo(XLogReaderState *record);
extern void sortheap_desc(StringInfo buf, XLogReaderState *record);
extern const char *sortheap_identify(uint8 info);

extern void sortheap_redo2(XLogReaderState *record);
extern void sortheap_desc2(StringInfo buf, XLogReaderState *record);
extern const char *sortheap_identify2(uint8 info);

#endif /* CDBSORTHEAPXLOG_H */
