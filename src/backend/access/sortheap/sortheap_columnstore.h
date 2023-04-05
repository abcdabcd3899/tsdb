/*-------------------------------------------------------------------------
 *
 * sortheap_columnstore.h
 *	  POSTGRES Sort Heap column-oriented store and compression routines.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_columnstore.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORTHEAP_COLUMNSTORE_H
#define SORTHEAP_COLUMNSTORE_H

#define MAXCHUNKSIZE 100

#define CHUNKHEADERSIZE(natts) \
	offsetof(ColumnChunk, attrs) + sizeof(ColumnChunkAttr) * natts

#define CHUNKDATA(chunk, natts) \
	(((char *)chunk) + CHUNKHEADERSIZE(natts))

typedef struct ColumnChunkAttr
{
	bool hasnull;
	uint32 off;
} ColumnChunkAttr;

typedef struct ColumnChunk
{
	int32 len;
	ColumnChunkAttr attrs[FLEXIBLE_ARRAY_MEMBER];
} ColumnChunk;

typedef struct ColumnChunkContext
{
	Size datalen;
	char *dp;
	bool attrnulls[MAXCHUNKSIZE];
} ColumnChunkContext;

int ColumnChunkSize(Relation relation);

TupleDesc build_columnstore_tupledesc(Relation relation, Relation indexrel,
									  TupleDesc **columnchunk_descs_ptr,
									  int *nindexkeys_ptr);
ColumnChunk **formColumnChunks(TapeInsertState *insertstate,
							   SortTuple *tuples,
							   int ntuples);
HeapTuple formCompressedTuple(TapeInsertState *insertstate,
							  SortTuple *first,
							  int ntuples,
							  ColumnChunk **chunks);
void deformColumnChunk(ColumnChunk *chunk,
					   TupleDesc chunkdesc,
					   int attnoinchunk,
					   int ntuples,
					   Datum **datums,
					   bool **isnulls);

#endif /* //SORTHEAP_COLUMNSTORE_H */
