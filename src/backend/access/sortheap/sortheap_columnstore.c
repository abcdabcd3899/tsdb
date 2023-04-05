/*-------------------------------------------------------------------------
 *
 * sortheap_columnstore.c
 *	  Sort heap column store and compression routines.
 *
 * Sort heap provides an automatic mechanism to convert a bunch of sorted
 * tuples from row store format to column store format and compress them.
 *
 * The conversion might occurs WHEN
 *
 * 1) the tuples are sorted and dumped in bunch.
 * 2) the tuples are merged in bunch.
 *
 * IF the number of tuples in the same group:
 * (NOTE: same group means the same index key value)
 *
 * 1) reachs the maxmum of tuples number threadhold (has more tuples in
 * the same group)
 * 2) reachs the minimum of tuples number and minimum of uncompression
 * size (has no more tuples in the same group)
 *
 * It is possible that row-oriented tuples and column-oriented tuples
 * are stored in the same table (even in a same block).
 *
 * The column-oriented tuple is formated as:
 *
 * | key1 | key2 |...| columnchunk1 | columnchunk2 |...| columnchunkN|
 *
 * where key1/key2 is the unique key for the group, columnchunk is a
 * unit of the columns, it may be consist of one column or multiple
 * columns which depends on the number of attributes a table has, if a
 * table has several hundred or thousand columns, a few columns will
 * be combined stored in a single columnchunk, otherwise, a columnchunk
 * stores a attribute.
 *
 * Row-oriented tuples are vertically split into column chunks, we take
 * a column chunk with three columns as an example.
 *
 * the column chunk is firstly constructed to:
 *
 * |c1, c1, c1 ... c2, c2, c2...c3, c3, c3...|
 *
 * then be compressed in line.
 * finally be toast stored externally if necessary.
 *
 * A compressed column-oriented tuple is also stored as a HeapTuple, we
 * use an obsolete HeapTupleHeader flag (HEAP_HASOID_OLD) to signal if
 * it's a column-oriented tuple or a row-oriented tuple.
 *
 * A compressed column-oriented tuple also has an index tuple, obviously
 * the size of index is much smaller and the efficiency of the index is
 * also improved dramaticlly (one index search can get a bunch of tuples
 * at a time).
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_columnstore.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_columnstore.h"
#include "sortheap_tape.h"
#include "sortheap_tapesets.h"

int ColumnChunkSize(Relation relation)
{
	int nattrs;
	int size;

	nattrs = RelationGetDescr(relation)->natts;
	size = (nattrs * TOAST_POINTER_SIZE + TOAST_TUPLE_THRESHOLD) / TOAST_TUPLE_THRESHOLD;

	if (size >= MAXCHUNKSIZE)
		elog(ERROR, "column chunk size exceeded the %d", MAXCHUNKSIZE);

	return size;
}

TupleDesc
build_columnstore_tupledesc(Relation relation, Relation indexrel,
							TupleDesc **columnchunk_descs_ptr,
							int *nindexkeys_ptr)
{
	TupleDesc desc;
	TupleDesc csdesc;
	TupleDesc *columnchunk_descs;
	int i;
	int chunksize;
	int nchunks;
	int nattrs;
	int nindexkeys;

	nindexkeys = IndexRelationGetNumberOfKeyAttributes(indexrel);

	desc = RelationGetDescr(relation);

	nattrs = desc->natts;

	/* Secondly to initialize combine column store tuple descriptor */
	chunksize = ColumnChunkSize(relation);
	nchunks = (nattrs + chunksize - 1) / chunksize;

	/* Initialize the column store tuple descriptor */
	/* ntuples | indexkey1 | indexkey2 | columnchunk1 | columnchunk2 ... */
	csdesc = CreateTemplateTupleDesc(nchunks + nindexkeys + 1);

	TupleDescInitEntry(csdesc, 1, NULL,
					   INT4OID,
					   -1, 0);

	/* First init indexkey attributes */
	for (i = 0; i < nindexkeys; i++)
	{
		TupleDesc inddesc = RelationGetDescr(indexrel);
		Form_pg_attribute attr = TupleDescAttr(inddesc, i);

		TupleDescInitEntry(csdesc, i + 2, NULL,
						   attr->atttypid,
						   attr->atttypmod, attr->attndims);
	}

	/* Then init column chunk attributes */
	for (i = 0; i < nchunks; i++)
	{
		TupleDescInitEntry(csdesc,
						   nindexkeys + i + 2, NULL,
						   BYTEAOID,
						   -1,
						   0);
	}

	columnchunk_descs = (TupleDesc *)palloc(sizeof(TupleDesc) * nchunks);

	/* Init each combine column descriptor */
	for (i = 0; i < nattrs; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (i % chunksize == 0)
		{
			int ncols;

			/* Last chunk may has different size with chunk size */
			if (i / chunksize == nattrs / chunksize)
				ncols = nattrs % chunksize;
			else
				ncols = chunksize;

			columnchunk_descs[i / chunksize] = CreateTemplateTupleDesc(ncols);
		}

		TupleDescInitEntry(columnchunk_descs[i / chunksize],
						   (i % chunksize) + 1,
						   NULL,
						   attr->atttypid,
						   attr->atttypmod,
						   attr->attndims);
	}

	*columnchunk_descs_ptr = columnchunk_descs;

	if (nindexkeys_ptr)
		*nindexkeys_ptr = nindexkeys;

	return csdesc;
}

ColumnChunk **
formColumnChunks(TapeInsertState *insertstate,
				 SortTuple *tuples,
				 int ntuples)
{
	int cidx;
	int tidx;
	int chunksize;
	int nchunks;
	int natts; /* number of atts to extract */
	int attnum;
	int coff;
	bool hasnulls;
	bool slow = false; /* can we use/set attcacheoff? */
	char *tp;		   /* ptr to tuple data */
	uint32 off;		   /* offset in tuple data */
	uint32 *offarr;	   /* offset array for each tuple */
	HeapTuple tuple;
	HeapTupleHeader tup;
	Relation rel;
	TupleDesc desc;
	TupleDesc chunkdesc;

	ColumnChunk **chunks;
	ColumnChunkContext *context;

	rel = insertstate->relation;
	desc = RelationGetDescr(rel);
	natts = desc->natts;

	chunksize = ColumnChunkSize(rel);
	nchunks = (natts + chunksize - 1) / chunksize;

	context = (ColumnChunkContext *)palloc0(sizeof(ColumnChunkContext) * nchunks);

	/* Go through the tuples and caculate the size of column chunks */
	for (tidx = 0; tidx < ntuples; tidx++)
	{
		uint32 saved;
		bits8 *bp; /* ptr to null bitmap in tuple */

		tuple = tuples[tidx].tuple;
		tup = tuple->t_data;
		bp = tup->t_bits;
		hasnulls = HeapTupleHasNulls(tuple);

		tp = (char *)tup + tup->t_hoff;

		off = 0;

		for (attnum = 0; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt = TupleDescAttr(desc, attnum);

			cidx = attnum / chunksize;
			coff = attnum % chunksize;

			if (hasnulls && att_isnull(attnum, bp))
			{
				context[cidx].attrnulls[coff] = true;
				slow = true; /* can't use attcacheoff anymore */
				continue;
			}

			if (!slow && thisatt->attcacheoff >= 0)
				off = thisatt->attcacheoff;
			else if (thisatt->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be
				 * no pad bytes in any case: then the offset will be valid for
				 * either an aligned or unaligned value.
				 */
				if (!slow &&
					off == att_align_nominal(off, thisatt->attalign))
					thisatt->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, thisatt->attalign, -1,
											tp + off);
					slow = true;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, thisatt->attalign);

				if (!slow)
					thisatt->attcacheoff = off;
			}
			if (!slow && thisatt->attlen < 0)
				slow = true;

			saved = off;

			off = att_addlength_pointer(off, thisatt->attlen, tp + off);

			context[cidx].datalen += (off - saved);
		}
	}

	chunks = (ColumnChunk **)palloc(sizeof(ColumnChunk *) * nchunks);

	for (cidx = 0; cidx < nchunks; cidx++)
	{
		int len;
		int i;

		chunkdesc = insertstate->columnchunk_descs[cidx];

		len = CHUNKHEADERSIZE(chunkdesc->natts);

		for (i = 0; i < chunkdesc->natts; i++)
		{
			if (context[cidx].attrnulls[i])
				len += BITMAPLEN(ntuples);
		}

		len += context[cidx].datalen;

		/* Allocated the memory the whole chunk needs */
		chunks[cidx] = (ColumnChunk *)palloc0(len);

		/* Set the start address of data in this chunk */
		context[cidx].dp = CHUNKDATA(chunks[cidx], chunkdesc->natts);

		SET_VARSIZE(chunks[cidx], len);
	}

	/* Go through all tuples again and fill the columns chunk */
	offarr = palloc0(sizeof(uint32) * ntuples);

	for (attnum = 0; attnum < desc->natts; attnum++)
	{
		Form_pg_attribute thisatt;
		ColumnChunk *chunk;
		bits8 *bit = NULL;
		int bitmask = HIGHBIT;

		thisatt = TupleDescAttr(desc, attnum);
		cidx = attnum / chunksize;
		coff = attnum % chunksize;
		chunk = chunks[cidx];
		hasnulls = false;

		/* Initialized the start address of this attr in the chunk */
		chunk->attrs[coff].hasnull = false;
		chunk->attrs[coff].off = context[cidx].dp - (char *)chunk;

		if (context[cidx].attrnulls[coff])
		{
			chunk->attrs[coff].hasnull = true;
			bit = ((bits8 *)context[cidx].dp) - 1;
			context[cidx].dp += BITMAPLEN(ntuples);
			hasnulls = true;
		}

		for (tidx = 0; tidx < ntuples; tidx++)
		{
			uint32 saved;
			bits8 *bp; /* ptr to null bitmap in tuple */

			tuple = tuples[tidx].tuple;
			tup = tuple->t_data;
			bp = tup->t_bits;

			tp = (char *)tup + tup->t_hoff;

			off = offarr[tidx];

			if (hasnulls)
			{
				if (bitmask != HIGHBIT)
					bitmask <<= 1;
				else
				{
					bit += 1;
					*bit = 0x0;
					bitmask = 1;
				}

				if (att_isnull(attnum, bp))
				{
					slow = true; /* can't use attcacheoff anymore */
					continue;
				}
				else
					*bit |= bitmask;
			}

			if (thisatt->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be
				 * no pad bytes in any case: then the offset will be valid for
				 * either an aligned or unaligned value.
				 */
				if (!slow &&
					off == att_align_nominal(off, thisatt->attalign))
					thisatt->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, thisatt->attalign, -1,
											tp + off);
					slow = true;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, thisatt->attalign);

				if (!slow)
					thisatt->attcacheoff = off;
			}
			if (!slow && thisatt->attlen < 0)
				slow = true;

			saved = off;

			off = att_addlength_pointer(off, thisatt->attlen, tp + off);

			memcpy(context[cidx].dp, tp + saved, off - saved);

			context[cidx].dp += (off - saved);
			offarr[tidx] = off;
		}
	}

	pfree(context);
	pfree(offarr);

	return chunks;
}

HeapTuple
formCompressedTuple(TapeInsertState *insertstate,
					SortTuple *first,
					int32 ntuples,
					ColumnChunk **chunks)
{
	int idx;
	int cidx;
	int natts;
	int nchunks;
	int chunksize;
	int nindexkeys;
	Datum *chunkdatums;
	bool *chunkisnulls;
	Relation rel;
	HeapTuple result;
	SortHeapState *shstate = insertstate->parent->parent;

	rel = insertstate->relation;
	natts = RelationGetDescr(rel)->natts;
	chunksize = ColumnChunkSize(rel);
	nchunks = (natts + chunksize - 1) / chunksize;
	nindexkeys = shstate->nkeys;
	chunkdatums = palloc(sizeof(Datum) * (nchunks + nindexkeys + 1));
	chunkisnulls = palloc(sizeof(bool) * (nchunks + nindexkeys + 1));

	chunkdatums[0] = Int32GetDatum(ntuples);
	chunkisnulls[0] = false;

	for (idx = 0; idx < nindexkeys; idx++)
	{
		Datum d;
		bool isnull;

		d = get_sorttuple_cache(shstate,
								first,
								idx,
								&isnull);

		chunkdatums[1 + idx] = d;
		chunkisnulls[1 + idx] = isnull;
	}

	for (cidx = 0; cidx < nchunks; cidx++)
	{
		idx = 1 + nindexkeys + cidx;

		chunkdatums[idx] = toast_compress_datum(PointerGetDatum(chunks[cidx]));

		if (chunkdatums[idx])
		{
			chunkdatums[idx] = sortheap_toast_save_datum(rel, chunkdatums[idx]);
			chunkisnulls[idx] = false;
		}
		else
		{
			chunkdatums[idx] = sortheap_toast_save_datum(rel, PointerGetDatum(chunks[cidx]));
			chunkisnulls[idx] = false;
		}

		pfree(chunks[cidx]);
	}

	result = heap_form_tuple(insertstate->columnstore_desc,
							 chunkdatums,
							 chunkisnulls);

	return result;
}

void deformColumnChunk(ColumnChunk *chunk,
					   TupleDesc chunkdesc,
					   int attnoinchunk,
					   int ntuples,
					   Datum **datums_ptr,
					   bool **isnulls_ptr)
{
	int tidx;
	Datum *datums;
	bool *isnulls;
	bool hasnull;
	uint32 off;
	char *tp;
	bits8 *bp;
	ColumnChunkAttr *chunkattr;
	Form_pg_attribute thisatt;

	datums = palloc(sizeof(Datum) * ntuples);
	isnulls = palloc(sizeof(bool) * ntuples);

	chunkattr = &chunk->attrs[attnoinchunk];
	thisatt = TupleDescAttr(chunkdesc, attnoinchunk);
	hasnull = chunkattr->hasnull;

	if (hasnull)
	{
		bp = (bits8 *)((char *)chunk + chunkattr->off);
		tp = (char *)chunk + chunkattr->off + BITMAPLEN(ntuples);
	}
	else
		tp = (char *)chunk + chunkattr->off;

	off = 0;

	for (tidx = 0; tidx < ntuples; tidx++)
	{
		if (hasnull && att_isnull(tidx, bp))
		{
			datums[tidx] = (Datum)0;
			isnulls[tidx] = true;
			continue;
		}

		isnulls[tidx] = false;

		if (thisatt->attlen == -1)
		{
			off = att_align_pointer(off, thisatt->attalign, -1,
									tp + off);
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);
		}

		datums[tidx] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);
	}

	*datums_ptr = datums;
	*isnulls_ptr = isnulls;
}
