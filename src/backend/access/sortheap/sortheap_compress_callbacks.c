/*-------------------------------------------------------------------------
 *
 * sortheap_compress_callbacks.c
 *  	compression version to implement the external sort framework
 *
 * Compare to the default version in sortheap_default_callbacks.c, this
 * file implementes the callbacks to handle compressed column-oriented
 * stored tuples.
 *
 * Refer the sortheap_columnstore for more info about column-oriented
 * store.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_compress_callbacks.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_tape.h"
#include "sortheap_tapesets.h"
#include "sortheap_btree.h"
#include "sortheap_columnstore.h"
#include "sortheap_default_callbacks.h"

static void
compress_toast_write(TapeInsertState *insertstate, SortTuple *tuples,
					 int compress_start, int compress_end,
					 int compress_length)
{
	int curtup;
	HeapTuple tuple;
	SortTuple stup;
	MemoryContext oldcontext;
	MemoryContext compresscontext;
	SortHeapState *shstate = insertstate->parent->parent;
	ColumnChunk **chunks;

	if (compress_start == -1 || compress_end == 0)
		return;

	Assert(compress_start < compress_end);

	/*
	 * If the length of all tuples are smaller than 32KB, it is not worth to
	 * compress or toast
	 */
	if (compress_length < COMPRESS_MIN_GROUPSIZE_THRESHOLD &&
		(compress_end - compress_start) < COMPRESS_MIN_NTUPLES_THRESHOLD)
	{
		for (curtup = compress_start; curtup < compress_end; curtup++)
			WRITETUP(shstate, &tuples[curtup]);
		return;
	}

	compresscontext = AllocSetContextCreate(CurrentMemoryContext,
											"sortheap compress context",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(compresscontext);

	/* accumulate datums by columns */
	chunks = formColumnChunks(insertstate,
							  tuples + compress_start,
							  compress_end - compress_start);

	tuple = formCompressedTuple(insertstate,
								&tuples[compress_start],
								compress_end - compress_start,
								chunks);

	/*
	 * HEAP_HASOID_OLD flag is eliminated, we use it to signal this is a
	 * compressed sortheap tuple
	 */
	tuple->t_data->t_infomask |= HEAP_HASOID_OLD;
	stup = tuples[compress_start];
	stup.tuple = tuple;

	MemoryContextSwitchTo(oldcontext);

	WRITETUP(shstate, &stup);

	MemoryContextDelete(compresscontext);
}

static void
dumptups_compression(SortHeapState *shstate, SortTuple *memtuples,
					 int memtupcount, int64 *availmem, bool lastdump)
{
	int compress_iter;
	int compress_start = -1;
	int32 compress_ntuples = 0;
	int32 compress_length = 0;
	TapeInsertState *tapeinsertstate = TAPEINSERTSTATE(shstate);

	if (!memtuples)
		return;

	for (compress_iter = 0; compress_iter < memtupcount; compress_iter++)
	{
		SortTuple *cur = &memtuples[compress_iter];
		SortTuple *first;

		if (availmem)
			*availmem += GetMemoryChunkSpace(cur->tuple);

		/* update the start offset to compress */
		if (compress_start == -1)
		{
			compress_start = compress_iter;
			compress_ntuples = 0;
			compress_length = 0;
		}

		first = &memtuples[compress_start];

		/*
		 * If the tuple has external storage, flush all pending insert tuples
		 * and insert the tuple in a norma format 0 (bit) | tuple (bytea)
		 */
		if (HeapTupleHasExternal(cur->tuple))
		{
			compress_toast_write(tapeinsertstate, memtuples,
								 compress_start,
								 compress_iter,
								 compress_length);

			WRITETUP(shstate, cur);

			compress_start = -1;
		}
		else if (!COMPARETUP(shstate, first, cur))
		{
			/* Same group */

			/*
			 * If the tuples in the same group exceed the compression
			 * threshold, compress it first
			 */
			if (compress_ntuples == COMPRESS_MAX_NTUPLES_THRESHOLD)
			{
				compress_toast_write(tapeinsertstate, memtuples,
									 compress_start,
									 compress_iter,
									 compress_length);
				compress_start = compress_iter;
				compress_length = 0;
				compress_ntuples = 0;
			}

			compress_ntuples++;
			compress_length += cur->tuple->t_len;
		}
		else
		{
			/* New group, close previous group */
			compress_toast_write(tapeinsertstate, memtuples,
								 compress_start,
								 compress_iter,
								 compress_length);

			compress_start = compress_iter;
			compress_ntuples = 1;
			compress_length = cur->tuple->t_len;
		}
	}

	/* Flush any tuples that are pending compressed */
	compress_toast_write(tapeinsertstate,
						 memtuples,
						 compress_start,
						 memtupcount,
						 compress_length);

	/* Write an end of run marker */
	WRITETUP(shstate, NULL);
}

/*
 * Fetch a tuple from the underlying storage.
 *
 * Returns:
 *
 * slot : stores the virtual tuple.
 * stup : stores the compressed or uncompressed physical heap tuple.
 */
static bool
fetchtup_compression(SortHeapState *shstate, TapeScanDesc tapescandesc,
					 SortTuple *stup, TupleTableSlot *slot, bool ismerge)
{
	int i;
	int attno;
	int chunkno;
	int prev_chunkno = -1;
	int chunksize;
	bool isnull;
	bool continuescan = false;
	Datum d;
	Relation relation = shstate->relation;
	TupleDesc rowdesc = RelationGetDescr(relation);
	ColumnChunk *chunk;
	ColumnChunk *prevchunk = NULL;
	MemoryContext oldcontext;

	if (!tapescandesc)
		return false;

	while (true)
	{
		if (tapescandesc->columnstore_tuple == NULL)
		{
			if (!READTUP(shstate, tapescandesc, stup, ismerge))
				return false;

			/* cache the columnstore tuple */
			tapescandesc->columnstore_tuple = stup->tuple;
		}

		if (!IsColumnStoreTuple(tapescandesc->columnstore_tuple))
		{
			tapescandesc->columnstore_tuple = NULL;

			MemSet(stup->cached, false, MAXKEYCACHE * sizeof(bool));

			if (!ismerge)
			{
				Assert(slot);
				ExecClearTuple(slot);
				ExecStoreHeapTuple(stup->tuple, slot, false);

				if (!sortheap_match_scankey_quals(relation, tapescandesc,
												  stup,
												  slot,
												  &continuescan,
												  true,
												  true))
				{
					CHECK_FOR_INTERRUPTS();

					if (!continuescan)
						return false;
					else
						continue;
				}
			}

			return true;
		}

		/* Start handling column store tuple */

		/*
		 * If a compressed column store tuple is deformed, read the tuples
		 * from the cache
		 */
		if (tapescandesc->cached &&
			tapescandesc->cur_cache < tapescandesc->size_cache)
		{
			Assert(slot);
			Assert(!ismerge);
			ExecClearTuple(slot);

			rowdesc = RelationGetDescr(relation);

			if (tapescandesc->tablescandesc->n_proj_atts < rowdesc->natts)
				MemSet(slot->tts_isnull, true,
					   sizeof(bool) * rowdesc->natts);

			for (i = 0; i < tapescandesc->tablescandesc->n_proj_atts; i++)
			{
				attno = tapescandesc->tablescandesc->proj_atts[i];
				slot->tts_values[attno] =
					tapescandesc->datum_cache[attno][tapescandesc->cur_cache];
				slot->tts_isnull[attno] =
					tapescandesc->isnull_cache[attno][tapescandesc->cur_cache];
			}

			ExecStoreVirtualTuple(slot);

			/*
			 * Tuples in the same compressed tuple has the same sort key
			 * value. So use the cached sort key value may save lots of time
			 * especially when the sort keys are functions
			 */
			memcpy(stup, &tapescandesc->stup_cache, sizeof(SortTuple));

			tapescandesc->cur_cache++;

			/* If the cache is empty, signal to read next tuple */
			if (tapescandesc->cur_cache == tapescandesc->size_cache)
			{
				tapescandesc->columnstore_tuple = NULL;
				tapescandesc->cached = false;
				tapescandesc->cur_cache = 0;
				prevchunk = NULL;
				prev_chunkno = -1;
			}

			/*
			 * Check whether the tuple matches the qual filter, we have
			 * checked the scan keys for the whole compressed tuple, so there
			 * is no need to check scan keys with the individual tuples that
			 * uncompressed.
			 */
			if (!sortheap_match_scankey_quals(relation, tapescandesc,
											  stup,
											  slot,
											  &continuescan,
											  false,
											  true))
			{
				if (!continuescan)
					return false;
				else
				{
					/* Read next cached tuple */
					continue;
				}
			}

			return true;
		}

		/* Get the sort key value from the compressed tuple */
		for (i = 0; i < shstate->nkeys; i++)
		{
			d = heap_getattr(tapescandesc->columnstore_tuple, i + 2,
							 tapescandesc->columnstore_desc,
							 &isnull);

			if (i < MAXKEYCACHE)
			{
				stup->datums[i] = d;
				stup->isnulls[i] = isnull;
				stup->cached[i] = true;
			}
		}

		/*
		 * Merging only cares about the sort key, so we only need to read the
		 * key values stores in the compressed tuple. For other attributes, it
		 * is unnecessary to uncompress, detoast or deform it from array form
		 * to normal form.
		 */
		if (ismerge)
		{
			/* signal to read next tuple */
			tapescandesc->columnstore_tuple = NULL;
			return true;
		}

		/* Mainly to cache the sort key values */
		memcpy(&tapescandesc->stup_cache, stup, sizeof(SortTuple));

		/*
		 * If we have qual filter on the scan key, we also need to read the
		 * key in the compressed tuple, so we can check the qual without
		 * uncompress or detoast.
		 */
		if (tapescandesc->tablescandesc && tapescandesc->tablescandesc->iss)
		{
			/*
			 * Quick check the scankey on the compressed column stored tuple,
			 * we do this before uncompression and de-toast so we can filter
			 * unused tuples in batch.
			 */
			if (!sortheap_match_scankey_quals(relation,
											  tapescandesc,
											  stup,
											  NULL,
											  &continuescan,
											  true,
											  false))
			{
				CHECK_FOR_INTERRUPTS();

				if (!continuescan)
					return false;
				else
				{
					/* signal to read next tuple */
					tapescandesc->columnstore_tuple = NULL;
					continue;
				}
			}
		}

		/*
		 * Start to uncompress the column chunks and cache the values of each
		 * column.
		 */
		Assert(!ismerge);

		MemoryContextReset(tapescandesc->columnchunkcontext);
		oldcontext = MemoryContextSwitchTo(tapescandesc->columnchunkcontext);

		chunksize = ColumnChunkSize(relation);

		d = heap_getattr(tapescandesc->columnstore_tuple, 1,
						 tapescandesc->columnstore_desc,
						 &isnull);
		tapescandesc->size_cache = DatumGetInt32(d);

		for (i = 0; i < tapescandesc->tablescandesc->n_proj_atts; i++)
		{
			int attnoinchunk;
			TupleDesc chunkdesc;

			attno = tapescandesc->tablescandesc->proj_atts[i];
			chunkno = attno / chunksize;
			attnoinchunk = attno % chunksize;
			chunkdesc = tapescandesc->columnchunk_descs[chunkno];

			/* If it is the index key, don't need to deform the chunk */
			if (attno == (shstate->indexinfo->ii_IndexAttrNumbers[0] - 1))
			{
				int j;

				tapescandesc->datum_cache[attno] =
					palloc(sizeof(Datum) * tapescandesc->size_cache);
				tapescandesc->isnull_cache[attno] =
					palloc(sizeof(bool) * tapescandesc->size_cache);

				for (j = 0; j < tapescandesc->size_cache; j++)
				{
					tapescandesc->datum_cache[attno][j] =
						tapescandesc->stup_cache.datums[0];
					tapescandesc->isnull_cache[attno][j] =
						tapescandesc->stup_cache.isnulls[0];
				}

				continue;
			}

			if (chunkno != prev_chunkno)
			{
				/* Get the column chunk */
				d = heap_getattr(tapescandesc->columnstore_tuple,
								 shstate->nkeys + chunkno + 2,
								 tapescandesc->columnstore_desc,
								 &isnull);

				chunk = (ColumnChunk *)DatumGetByteaP(d);

				prevchunk = chunk;
				prev_chunkno = chunkno;
			}
			else
				chunk = prevchunk;

			deformColumnChunk(chunk,
							  chunkdesc,
							  attnoinchunk,
							  tapescandesc->size_cache,
							  &tapescandesc->datum_cache[attno],
							  &tapescandesc->isnull_cache[attno]);
		}

		MemoryContextSwitchTo(oldcontext);

		tapescandesc->cached = true;
		tapescandesc->cur_cache = 0;
	}
}

static void
mergetup_compression(SortHeapState *shstate, SortTuple *stup)
{
	SortTuple *cache;
	MemoryContext oldcontext;
	TapeInsertState *insertstate = TAPEINSERTSTATE(shstate);

	if (stup == NULL)
	{
		/* End of the RUN, flush tuples in the group */
		compress_toast_write(insertstate, insertstate->mergecached,
							 0, insertstate->mergecache_cur,
							 insertstate->mergecache_size);

		MemoryContextReset(insertstate->mergecachecontext);
		insertstate->mergecache_cur = 0;
		insertstate->mergecache_size = 0;

		/* Write a end of run tailer */
		WRITETUP(shstate, NULL);
	}
	else if (insertstate->mergecache_cur == 0 ||
			 !COMPARETUP(shstate, &insertstate->mergecached[insertstate->mergecache_cur - 1], stup))
	{
		/* Same group or first tuple */

		/*
		 * If it's compressed or toast, the tuple cannot be compressed further
		 * with other tuples in the same group, so flush it immediately.
		 */
		if (IsColumnStoreTuple(stup->tuple) ||
			HeapTupleHasExternal(stup->tuple))
			WRITETUP(shstate, stup);
		else
		{
			/*
			 * Cache it so it can be compressed with other tuples in the same
			 * group
			 */
			oldcontext = MemoryContextSwitchTo(insertstate->mergecachecontext);
			cache = &insertstate->mergecached[insertstate->mergecache_cur++];
			cache->tuple = heap_copytuple(stup->tuple);
			memcpy(cache->cached, stup->cached, sizeof(bool) * MAXKEYCACHE);
			memcpy(cache->datums, stup->datums, sizeof(Datum) * MAXKEYCACHE);
			memcpy(cache->isnulls, stup->isnulls, sizeof(bool) * MAXKEYCACHE);

			insertstate->mergecache_size += stup->tuple->t_len;

			MemoryContextSwitchTo(oldcontext);

			if (insertstate->mergecache_cur == COMPRESS_MAX_NTUPLES_THRESHOLD)
			{
				compress_toast_write(insertstate, insertstate->mergecached,
									 0,
									 insertstate->mergecache_cur,
									 insertstate->mergecache_size);
				MemoryContextReset(insertstate->mergecachecontext);
				insertstate->mergecache_cur = 0;
				insertstate->mergecache_size = 0;
			}
		}
	}
	else
	{
		/* New group, write previous group */
		compress_toast_write(insertstate, insertstate->mergecached,
							 0,
							 insertstate->mergecache_cur,
							 insertstate->mergecache_size);
		MemoryContextReset(insertstate->mergecachecontext);
		insertstate->mergecache_cur = 0;
		insertstate->mergecache_size = 0;

		/*
		 * If it's compressed or toast, the tuple cannot be compressed further
		 * with other tuples in the same group, so flush it immediately.
		 */
		if (IsColumnStoreTuple(stup->tuple) ||
			HeapTupleHasExternal(stup->tuple))
			WRITETUP(shstate, stup);
		else
		{
			int i;
			TupleDesc indexdesc = RelationGetDescr(shstate->indexrel);

			oldcontext = MemoryContextSwitchTo(insertstate->mergecachecontext);
			cache = &insertstate->mergecached[0];
			cache->tuple = heap_copytuple(stup->tuple);

			memcpy(cache->cached, stup->cached, sizeof(bool) * MAXKEYCACHE);
			memcpy(cache->isnulls, stup->isnulls, sizeof(bool) * MAXKEYCACHE);

			for (i = 0; i < MAXKEYCACHE; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(indexdesc, i);

				if (!cache->cached[i])
					continue;

				cache->datums[i] =
					datumCopy(stup->datums[i], attr->attbyval, attr->attlen);
			}

			MemoryContextSwitchTo(oldcontext);

			insertstate->mergecache_cur = 1;
			insertstate->mergecache_size += stup->tuple->t_len;
		}
	}
}

const ExternalSortMethods externalsort_compress_methods =
	{
		.comparetup = default_comparetup,
		.fetchtup = fetchtup_compression,
		.readtup = default_readtup,
		.copytup = default_copytup,
		.dumptups = dumptups_compression,
		.mergetup = mergetup_compression,
		.writetup = default_writetup};
