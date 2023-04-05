/*-------------------------------------------------------------------------
 *
 * marsam.cc
 *	  MARS storage read/write interface
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/marsam.cc
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"
#include "marsam.h"

#include "FileManager.h"
#include "Footer.h"
#include "ParquetWriter.h"
#include "ParquetReader.h"
#include "ScanKey.h"
#include "tools.h"

#include <algorithm>

#include <sys/stat.h>

using mars::ParquetWriter;
using std::unordered_map;

namespace mars
{

	unordered_map<Oid, ParquetWriter *> writerTab_;

	void CleanInsertEnv()
	{
		writerTab_.clear();
	}

	ParquetWriter *
	GetWriter(::Relation rel, ::Snapshot snapshot = nullptr)
	{
		ParquetWriter *writer;
		Oid relationOid = RelationGetRelid(rel);

		auto iter = writerTab_.find(relationOid);

		if (iter == writerTab_.end())
		{
			if (!snapshot)
			{
				snapshot = ::GetTransactionSnapshot();
			}

			writer = ParquetWriter::Open(rel, snapshot);

			if (writer != nullptr)
			{
				writerTab_.insert({relationOid, writer});
			}
		}
		else
		{
			writer = iter->second;
		}

		if (writer == nullptr)
		{
			elog(ERROR, "Get parquet writer failed, relation: %d", RelationGetRelid(rel));
		}

		return writer;
	}

	void
	DelWriter(Relation rel)
	{
		Oid relationOid = RelationGetRelid(rel);

		writerTab_.erase(relationOid);
	}

} // namespace mars

/* ------------------------------------------------------------------------
 * Table scan callbacks.
 * ------------------------------------------------------------------------
 */
TableScanDesc
mars_scan_begin(Relation rel, Snapshot snapshot,
				int nkeys, struct ScanKeyData *key,
				ParallelTableScanDesc pscan, uint32 flags)
{
	auto scanWrapper = (mars::ScanWrapper *)palloc0(sizeof(mars::ScanWrapper));
	auto scan = &scanWrapper->scan_;
	bool is_analyze = flags & SO_TYPE_ANALYZE;

	scanWrapper->preader_ = new mars::ParquetReader(rel, snapshot, is_analyze);
	scanWrapper->preader_->BeginScan();

	{
		auto natts = RelationGetNumberOfAttributes(rel);
		bool cols[natts];
		memset(cols, true, sizeof(cols));
		auto projs = std::vector<bool>(cols, cols + natts);

		scanWrapper->preader_->SetProjectedColumns(projs);
	}

	scan->rs_rd = rel;
	scan->rs_snapshot = snapshot;
	scan->rs_nkeys = nkeys;
	scan->rs_flags = flags;
	scan->rs_parallel = pscan;

	pgstat_count_heap_scan(rel);

	return (TableScanDesc)scanWrapper;
}

TableScanDesc
mars_scan_begin_extractcolumns(Relation rel,
							   Snapshot snapshot,
							   List *targetlist,
							   List *qual,
							   ParallelTableScanDesc pscan,
							   uint32 flags)
{
	auto scanWrapper = (mars::ScanWrapper *)palloc0(sizeof(mars::ScanWrapper));
	auto scan = &scanWrapper->scan_;
	bool is_analyze = flags & SO_TYPE_ANALYZE;
	auto natts = RelationGetNumberOfAttributes(rel);

	scanWrapper->preader_ = new mars::ParquetReader(rel, snapshot, is_analyze);
	scanWrapper->preader_->BeginScan(qual);

	{
		bool found = false;
		bool cols[natts];
		memset(cols, 0, natts);
		found |= extractcolumns_from_node(rel, (Node *)targetlist, cols, natts);
		found |= extractcolumns_from_node(rel, (Node *)qual, cols, natts);

		if (!found)
		{
			if ((flags & SO_TYPE_ANALYZE) == 0)
			{
				cols[0] = true;
			}
			else
			{
				memset(cols, true, sizeof(cols));
			}
		}

		auto projs = std::vector<bool>(cols, cols + natts);

		scanWrapper->preader_->SetProjectedColumns(projs);
	}

	scan->rs_rd = rel;
	scan->rs_snapshot = snapshot;
	scan->rs_nkeys = 0;
	scan->rs_flags = flags;
	scan->rs_parallel = pscan;

	pgstat_count_heap_scan(rel);

	return (TableScanDesc)scanWrapper;
}

TableScanDesc
mars_scan_begin_extractcolumns_bm(Relation rel, Snapshot snapshot,
								  List *targetList, List *quals,
								  List *bitmapqualorig,
								  uint32 flags)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return (TableScanDesc)NULL;
}

void mars_scan_end(TableScanDesc scan)
{
	auto scanWrapper = (mars::ScanWrapper *)scan;

	delete scanWrapper->preader_;
	scanWrapper->preader_ = NULL;
}

void mars_scan_rescan(TableScanDesc scan, struct ScanKeyData *key,
					  bool set_params, bool allow_strat,
					  bool allow_sync, bool allow_pagemode)
{
	auto scanWrapper = (mars::ScanWrapper *)scan;

	scanWrapper->preader_->Rescan();
}

bool mars_scan_getnextslot(TableScanDesc scan, ScanDirection direction,
						   TupleTableSlot *slot)
{
	auto scanWrapper = (mars::ScanWrapper *)scan;

	::ExecClearTuple(slot);
	if (scanWrapper->preader_->ReadNext(direction, slot))
	{
		::ExecStoreVirtualTuple(slot);
		pgstat_count_heap_getnext(scanWrapper->scan_.rs_rd);

		return true;
	}

	return false;
}

/* ------------------------------------------------------------------------
 * Parallel table scan related functions.
 * ------------------------------------------------------------------------
 */

Size mars_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return 0;
}

Size mars_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return 0;
}

void mars_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks
 * ------------------------------------------------------------------------
 */

struct IndexFetchTableData *
mars_index_fetch_begin(Relation rel)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_index_fetch_reset(struct IndexFetchTableData *data)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_index_fetch_end(struct IndexFetchTableData *data)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

bool mars_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid,
							Snapshot snapshot, TupleTableSlot *slot,
							bool *call_again, bool *all_dead)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples
 * ------------------------------------------------------------------------
 */

bool mars_tuple_fetch_row_version(Relation rel, ItemPointer tid,
								  Snapshot snapshot, TupleTableSlot *slot)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

bool mars_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

void mars_tuple_get_latest_tid(TableScanDesc scan,
							   ItemPointer tid)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

bool mars_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								   Snapshot snapshot)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

TransactionId
mars_compute_xid_horizon_for_tuples(Relation rel,
									ItemPointerData *items,
									int nitems)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return InvalidTransactionId;
}

/* ------------------------------------------------------------------------
 * Manipulations of physical tuples.
 * ------------------------------------------------------------------------
 */
void mars_tuple_insert(Relation rel, TupleTableSlot *slot,
					   CommandId cid, int options,
					   struct BulkInsertStateData *bistate)
{
	::slot_getallattrs(slot);

	ParquetWriter *writer = mars::GetWriter(rel);

	arrow::Status status = writer->BufferedAppend(slot);
	if (!status.ok())
	{
		elog(ERROR, "mars flush error: %s", status.message().data());
	}

	pgstat_count_heap_insert(rel, 1);
}

void mars_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
								   CommandId cid, int options,
								   struct BulkInsertStateData *bistate,
								   uint32 specToken)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									 uint32 specToken, bool succeeded)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_multi_insert(Relation rel, TupleTableSlot **slots,
					   int nslots, CommandId cid, int options,
					   struct BulkInsertStateData *bistate)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

TM_Result
mars_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck, bool wait,
				  TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return TM_Ok;
}

TM_Result
mars_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
				  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
				  bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
				  bool *update_indexes)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);

	return TM_Ok;
}

TM_Result
mars_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
				TupleTableSlot *slot, CommandId cid,
				LockTupleMode mode, LockWaitPolicy wait_policy,
				uint8 flags, TM_FailureData *tmfd)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return TM_Ok;
}

void mars_finish_bulk_insert(Relation rel, int options)
{
	if (mars::writerTab_.empty())
		return;

	ParquetWriter *writer = mars::GetWriter(rel);

	arrow::Status status;
	status = writer->FlushBuffer();

	delete writer;

	mars::DelWriter(rel);

	if (!status.ok())
	{
		elog(ERROR, "mars flush error: %s", status.message().data());
	}
}

/* ------------------------------------------------------------------------
 * DDL related functionality.
 * ------------------------------------------------------------------------
 */
void mars_relation_set_new_filenode(Relation rel, const RelFileNode *newrnode,
									char persistence, TransactionId *freezeXid,
									MultiXactId *minmulti)
{
	::SMgrRelation srel;

	::StdRdOptions *options = (::StdRdOptions *)rel->rd_options;
	if (options)
		/*
		 * Append-optimized tables do not contain transaction information in
		 * tuples.
		 */
		*freezeXid = *minmulti = InvalidTransactionId;

	/*
	 * No special treatment is needed for new AO_ROW/COLUMN relation. Create
	 * the underlying disk file storage for the relation.  No clean up is
	 * needed, RelationCreateStorage() is transactional.
	 *
	 * Segment files will be created when / if needed.
	 */
	// TODO::
	srel = mars::RelationCreateStorage(*newrnode, persistence, SMGR_AO);

#if 0
	//TODO:: re-think it when xlog implementation
	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			       rel->rd_rel->relkind == RELKIND_MATVIEW ||
			       rel->rd_rel->relkind == RELKIND_TOASTVALUE);

		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}
#endif

	// TODO::
	smgrclose(srel);

	mars::footer::CreateFooterCatalog(rel);
}

void mars_relation_nontransactional_truncate(Relation rel)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_relation_copy_for_cluster(Relation NewTable, Relation OldTable,
									Relation OldIndex, bool use_sort,
									TransactionId OldestXmin,
									TransactionId *xid_cutoff,
									MultiXactId *multi_cutoff,
									double *num_tuples,
									double *tups_vacuumed,
									double *tups_recently_dead)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_relation_vacuum(Relation onerel, struct VacuumParams *params,
						  BufferAccessStrategy bstrategy)
{
	elog(DEBUG5, "parallel insert is not on mile stone one feature list."
				 " Mars does not need to support since yet. %s",
		 __func__);
}

bool mars_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								  BufferAccessStrategy bstrategy)
{
	auto scanWrapper = (mars::ScanWrapper *)scan;

	return scanWrapper->preader_->SetScanTargetBlock(blockno);
}

bool mars_scan_analyze_next_tuple(TableScanDesc scan,
								  TransactionId OldestXmin,
								  double *liverows, double *deadrows,
								  TupleTableSlot *slot)
{
	auto scanWrapper = (mars::ScanWrapper *)scan;
	::ExecClearTuple(slot);
	if (scanWrapper->preader_->ReadNext(ForwardScanDirection, slot))
	{

		::ExecStoreVirtualTuple(slot);
		*liverows += 1;
		return true;
	}

	return false;
}

double
mars_index_build_range_scan(Relation table_rel,
							Relation index_rel,
							struct IndexInfo *index_info,
							bool allow_sync,
							bool anyvisible,
							bool progress,
							BlockNumber start_blockno,
							BlockNumber numblocks,
							IndexBuildCallback callback,
							void *callback_state,
							TableScanDesc scan)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return 0;
}

void mars_index_validate_scan(Relation table_rel, Relation index_rel,
							  struct IndexInfo *index_info,
							  Snapshot snapshot,
							  struct ValidateIndexState *state)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

/* ------------------------------------------------------------------------
 * Miscellaneous functions.
 * ------------------------------------------------------------------------
 */
/*
 * This callback is only used by ANALYZE command as a intermediate value to
 * calculate the total number of blocks. However, we do not keep the same size
 * of block as HEAP. Upstream's blocks calculation formula does not suitable for
 * us.
 *
 * Return (Blocks Count) * HEAP_BLOCK_SZ as result to cheat executor
 * sampling algorithm to find correct block number.
 */
uint64
mars_relation_size(Relation rel, ForkNumber forkNumber)
{
	::Oid auxOid;
	::Relation auxRel;
	::Relation rels[2];
	int64 totalsize = 0;

	if (forkNumber != MAIN_FORKNUM)
		return 0;

	/*
	 * ANALYZE can be triggered after an INSERT, make sure the tuples are
	 * flushed before we report the size of the data file.
	 */
	::mars_event_notify(rel, CMD_INSERT, NULL);

	::GetMARSEntryAuxOids(RelationGetRelid(rel), NULL, &auxOid);
	auxRel = ::try_relation_open(auxOid, AccessShareLock, false);

	/* a mars file has only one data table and one aux table */
	rels[0] = rel;
	rels[1] = auxRel;

	for (int i = 0; i < 2; ++i)
	{
		/* below are derived from calculate_relation_size() */
		char *relationpath;
		char pathname[MAXPGPATH];
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		relationpath = ::relpathbackend(rels[i]->rd_node,
										rels[i]->rd_backend,
										MAIN_FORKNUM);

		/* each table contains only one file */
		::snprintf(pathname, MAXPGPATH, "%s", relationpath);

		if (::stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file %s: %m", pathname)));
		}

		totalsize += fst.st_size;
	}

	relation_close(auxRel, AccessShareLock);

	return totalsize;
}

/*
 * Check if the table needs a TOAST table. It does only if any column maximum
 * length exceed TOAST_COLUMN_THRESHOLD.
 */
bool mars_relation_needs_toast_table(Relation rel)
{
	TupleDesc tupdesc = rel->rd_att;
	int i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->atttypid != INT4OID &&
			att->atttypid != INT8OID &&
			att->atttypid != FLOAT4OID &&
			att->atttypid != FLOAT8OID &&
			att->atttypid != TIMESTAMPOID &&
			att->atttypid != TIMESTAMPTZOID &&
			att->atttypid != BOOLOID &&
			att->attbyval)
		{
			ereport(ERROR,
					errcode(ERRCODE_DATATYPE_MISMATCH),
					errmsg("MARS does not support %s type",
						   get_type_name(att->atttypid)));
		}
	}

	/* MARS always does not need toast table */
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related functions.
 * ------------------------------------------------------------------------
 */
void mars_relation_estimate_size(Relation rel, int32 *attr_widths,
								 BlockNumber *pages, double *tuples,
								 double *allvisfrac)
{
	*pages = 1;
	*tuples = 1;
	*allvisfrac = 0;
	std::vector<mars::footer::Footer> footers;

	if (Gp_role == GP_ROLE_DISPATCH)
		return;

	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

/* ------------------------------------------------------------------------
 * Executor related functions.
 * ------------------------------------------------------------------------
 */
bool mars_scan_bitmap_next_block(TableScanDesc scan,
								 struct TBMIterateResult *tbmres)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

bool mars_scan_bitmap_next_tuple(TableScanDesc scan,
								 struct TBMIterateResult *tbmres,
								 TupleTableSlot *slot)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

bool mars_scan_sample_next_block(TableScanDesc scan,
								 struct SampleScanState *scanstate)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

bool mars_scan_sample_next_tuple(TableScanDesc scan,
								 struct SampleScanState *scanstate,
								 TupleTableSlot *slot)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
	return false;
}

void mars_index_fetch_extractcolumns(struct IndexFetchTableData *scan,
									 List *targetlist, List *qual)
{
	elog(ERROR, "Mars does not implement yet. %s", __func__);
}

void mars_event_notify(Relation relation, CmdType operation, void *param)
{

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		if (mars::writerTab_.empty())
			return;

		ParquetWriter *writer = mars::GetWriter(relation);

		arrow::Status status;
		status = writer->FlushBuffer();

		if (!status.ok())
		{
			elog(ERROR, "mars flush error: %s", status.message().data());
		}

		delete writer;

		mars::DelWriter(relation);
	}
	else if (operation == CMD_UTILITY)
	{
		if (::strncmp("DropTable", (char *)param, 8) == 0)
		{
			::RemoveMARSEntry(RelationGetRelid(relation));
		}
	}
}
