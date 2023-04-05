#include "postgres.h"

#include "access/aocssegfiles.h"
#include "access/table.h"
#include "access/tuptoaster.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_type.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbvars.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "storage/bufmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "funcapi.h"

/**
 * Statistics related parameters.
 */

bool			gp_statistics_pullup_from_child_partition = false;
bool			gp_statistics_use_fkeys = false;


/*
 * gp_acquire_sample_rows - Acquire a sample set of rows from table.
 *
 * This is a SQL callable wrapper around the internal acquire_sample_rows()
 * function in analyze.c. It allows collecting a sample across all segments,
 * from the dispatcher.
 *
 * acquire_sample_rows() actually has three return values: the set of sample
 * rows, and two double values: 'totalrows' and 'totaldeadrows'. It's a bit
 * difficult to return that from a SQL function, so bear with me. This function
 * is a set-returning function, and returns the sample rows, as you might
 * expect. But to return the extra 'totalrows' and 'totaldeadrows' values,
 * it always also returns one extra row, the "summary row". The summary row
 * is all NULLs for the actual table columns, but contains two other columns
 * instead, "totalrows" and "totaldeadrows". Those columns are NULL in all
 * the actual sample rows.
 *
 * To make things even more complicated, each sample row contains one extra
 * column too: oversized_cols_bitmap. It's a bitmap indicating which attributes
 * on the sample row were omitted, because they were "too large". The omitted
 * attributes are returned as NULLs, and the bitmap can be used to distinguish
 * real NULLs from values that were too large to be included in the sample. The
 * bitmap is represented as a text column, with '0' or '1' for every column.
 *
 * So overall, this returns a result set like this:
 *
 * postgres=# select * from pg_catalog.gp_acquire_sample_rows('foo'::regclass, 400, 'f') as (
 *     -- special columns
 *     totalrows pg_catalog.float8,
 *     totaldeadrows pg_catalog.float8,
 *     oversized_cols_bitmap pg_catalog.text,
 *     -- columns matching the table
 *     id int4,
 *     t text
 *  );
 *  totalrows | totaldeadrows | oversized_cols_bitmap | id  |    t    
 * -----------+---------------+-----------------------+-----+---------
 *            |               |                       |   1 | foo
 *            |               |                       |   2 | bar
 *            |               | 01                    |  50 | 
 *            |               |                       | 100 | foo 100
 *          2 |             0 |                       |     | 
 *          1 |             0 |                       |     | 
 *          1 |             0 |                       |     | 
 * (7 rows)
 *
 * The first four rows form the actual sample. One of the columns contained
 * an oversized text datum. The function is marked as EXECUTE ON SEGMENTS in
 * the catalog so you get one summary row *for each segment*.
 */
Datum
gp_acquire_sample_rows(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_acquire_sample_rows() is deprecated in this version.");
}

/*
 * utility stmt version to acquire gp sample rows from segments,
 * the logical keeps the same with gp_acquire_sample_rows() but
 * utility version can extend more arguments without catalog
 * changes.
 */
void
ExecGpAcquireSampleRowsStmt(GpAcquireSampleRowsStmt *stmt,
							DestReceiver *dest)
{
	gp_acquire_sample_rows_context *ctx;
	TupleTableSlot	*slot;
	VacuumParams	params;
	RangeVar	*this_rangevar;
	Relation	onerel;
	TupleDesc	relDesc;
	TupleDesc	outDesc;
	HeapTuple	res;
	int			live_natts;
	int			attno;
	int			outattno;
	Datum		*outvalues; 
	bool		*outnulls; 
	Oid			relOid = stmt->relOid;
	int32		targrows = stmt->targrows;
	List		*va_cols = stmt->va_cols;
	bool		inherited = stmt->inherited;


	if (targrows < 1)
		elog(ERROR, "invalid targrows argument");

	/* Construct the context to keep across calls. */
	ctx = (gp_acquire_sample_rows_context *) palloc0(sizeof(gp_acquire_sample_rows_context));
	ctx->targrows = targrows;
	ctx->inherited = inherited;

	if (!pg_class_ownercheck(relOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relOid));

	onerel = table_open(relOid, AccessShareLock);
	relDesc = RelationGetDescr(onerel);

	MemSet(&params, 0, sizeof(VacuumParams));
	params.options |= VACOPT_ANALYZE;
	params.freeze_min_age = -1;
	params.freeze_table_age = -1;
	params.multixact_freeze_min_age = -1;
	params.multixact_freeze_table_age = -1;
	params.is_wraparound = false;
	params.log_min_duration = -1;
	params.index_cleanup = VACOPT_TERNARY_DEFAULT;
	params.truncate = VACOPT_TERNARY_DEFAULT;

	this_rangevar = makeRangeVar(get_namespace_name(onerel->rd_rel->relnamespace),
								 pstrdup(RelationGetRelationName(onerel)),
								 -1);
	analyze_rel(relOid, this_rangevar, &params, va_cols,
				true, GetAccessStrategy(BAS_VACUUM), ctx);

	/* Count the number of non-dropped cols */
	live_natts = 0;
	for (attno = 1; attno <= relDesc->natts; attno++)
	{
		Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);

		if (relatt->attisdropped)
			continue;
		live_natts++;
	}

	outDesc = CreateTemplateTupleDesc(NUM_SAMPLE_FIXED_COLS + live_natts);

	/* First, some special cols: */

	/* These two are only set in the last, summary row */
	TupleDescInitEntry(outDesc,
					   1,
					   "totalrows",
					   FLOAT8OID,
					   -1,
					   0);
	TupleDescInitEntry(outDesc,
					   2,
					   "totaldeadrows",
					   FLOAT8OID,
					   -1,
					   0);

	/* extra column to indicate oversize cols */
	TupleDescInitEntry(outDesc,
					   3,
					   "oversized_cols_bitmap",
					   TEXTOID,
					   -1,
					   0);

	outattno = NUM_SAMPLE_FIXED_COLS + 1;
	for (attno = 1; attno <= relDesc->natts; attno++)
	{
		Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
		Oid			typid;

		if (relatt->attisdropped)
			continue;

		typid = gp_acquire_sample_rows_col_type(relatt->atttypid);

		TupleDescInitEntry(outDesc,
						   outattno++,
						   NameStr(relatt->attname),
						   typid,
						   relatt->atttypmod,
						   0);
	}

	BlessTupleDesc(outDesc);

	outvalues = (Datum *) palloc(outDesc->natts * sizeof(Datum));
	outnulls = (bool *) palloc(outDesc->natts * sizeof(bool));

	slot = MakeSingleTupleTableSlot(outDesc, &TTSOpsHeapTuple);

	dest->rStartup(dest, (int) CMD_SELECT, outDesc);

	/* First return all the sample rows */
	for (int i = 0; i < ctx->num_sample_rows; i++)
	{
		HeapTuple	relTuple = ctx->sample_rows[i];
		int			attno;
		Bitmapset  *toolarge = NULL;
		Datum	   *relvalues = (Datum *) palloc(relDesc->natts * sizeof(Datum));
		bool	   *relnulls = (bool *) palloc(relDesc->natts * sizeof(bool));

		heap_deform_tuple(relTuple, relDesc, relvalues, relnulls);

		outattno = NUM_SAMPLE_FIXED_COLS + 1;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
			bool		is_toolarge = false;
			Datum		relvalue;
			bool		relnull;

			if (relatt->attisdropped)
				continue;
			relvalue = relvalues[attno - 1];
			relnull = relnulls[attno - 1];

			/* Is this attribute "too large" to return? */
			if (relatt->attlen == -1 && !relnull)
			{
				Size		toasted_size = toast_datum_size(relvalue);

				if (toasted_size > WIDTH_THRESHOLD)
				{
					toolarge = bms_add_member(toolarge, outattno - NUM_SAMPLE_FIXED_COLS);
					is_toolarge = true;
					relvalue = (Datum) 0;
					relnull = true;
				}
			}
			outvalues[outattno - 1] = relvalue;
			outnulls[outattno - 1] = relnull;
			outattno++;
		}

		/*
		 * If any of the attributes were oversized, construct the text datum
		 * to represent the bitmap.
		 */
		if (toolarge)
		{
			char	   *toolarge_str;
			int			i;
			int			live_natts = outDesc->natts - NUM_SAMPLE_FIXED_COLS;

			toolarge_str = palloc((live_natts + 1) * sizeof(char));
			for (i = 0; i < live_natts; i++)
				toolarge_str[i] = bms_is_member(i + 1, toolarge) ? '1' : '0';
			toolarge_str[i] = '\0';

			outvalues[2] = CStringGetTextDatum(toolarge_str);
			outnulls[2] = false;
		}
		else
		{
			outvalues[2] = (Datum) 0;
			outnulls[2] = true;
		}
		outvalues[0] = (Datum) 0;
		outnulls[0] = true;
		outvalues[1] = (Datum) 0;
		outnulls[1] = true;

		res = heap_form_tuple(outDesc, outvalues, outnulls);
		slot = ExecStoreHeapTuple(res, slot, false);
		dest->receiveSlot(slot, dest);
	}

	/* Done returning the sample. Return the summary row, and we're done. */
	outvalues[0] = Float8GetDatum(ctx->totalrows);
	outnulls[0] = false;
	outvalues[1] = Float8GetDatum(ctx->totaldeadrows);
	outnulls[1] = false;

	outvalues[2] = (Datum) 0;
	outnulls[2] = true;
	for (outattno = NUM_SAMPLE_FIXED_COLS + 1; outattno <= outDesc->natts; outattno++)
	{
		outvalues[outattno - 1] = (Datum) 0;
		outnulls[outattno - 1] = true;
	}

	res = heap_form_tuple(outDesc, outvalues, outnulls);
	slot = ExecStoreHeapTuple(res, slot, false);
	dest->receiveSlot(slot, dest);

	/* done */
	dest->rShutdown(dest);
	table_close(onerel, AccessShareLock);

	pfree(ctx);
}

/*
 * Companion to gp_acquire_sample_rows().
 *
 * gp_acquire_sample_rows() returns a different datatype for some
 * columns in the table. This does the mapping. It's in a function, so
 * that it can be used both by gp_acquire_sample_rows() itself, as well
 * as its callers.
 */
Oid
gp_acquire_sample_rows_col_type(Oid typid)
{
	switch (typid)
	{
		case REGPROCOID:
			/*
			 * repproc isn't round-trippable, if there are overloaded
			 * functions. Treat it as plain oid.
			 */
			return OIDOID;

		case PGNODETREEOID:
			/*
			 * Input function of pg_node_tree doesn't allow loading
			 * back values. Treat it as text.
			 */
			return TEXTOID;
	}
	return typid;
}
