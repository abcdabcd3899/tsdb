#include "wrapper.hpp"

#include <parquet/metadata.h>

#include "marsam.h"

#include "marscustomscan.h"

#include "Footer.h"
#include "tools.h"
#include "ParquetWriter.h"
#include "guc.h"
#include "coder/Coder.h"

extern "C" {

PG_MODULE_MAGIC;

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_fini(void);

PG_FUNCTION_INFO_V1(mars_tableam_handler);

PG_FUNCTION_INFO_V1(mars_compress_info);

PG_FUNCTION_INFO_V1(mars_preagg_sum);

PG_FUNCTION_INFO_V1(mars_catalog_manual_init);

} // extern "C" finish

mars::MemoryPool mars::MemoryPool::default_pool;

/* ------------------------------------------------------------------------
 * Slot related callbacks.
 * ------------------------------------------------------------------------
 */
static inline const TupleTableSlotOps *
mars_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static const TableAmRoutine mars_method = {
	.type = T_TableAmRoutine,

	.slot_callbacks = mars_slot_callbacks,

	.scan_begin = mars_scan_begin,
	.scan_begin_extractcolumns = mars_scan_begin_extractcolumns,
	.scan_begin_extractcolumns_bm = mars_scan_begin_extractcolumns_bm,
	.scan_end = mars_scan_end,
	.scan_rescan = mars_scan_rescan,
	.scan_getnextslot = mars_scan_getnextslot,

	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = mars_index_fetch_begin,
	.index_fetch_reset = mars_index_fetch_reset,
	.index_fetch_end = mars_index_fetch_end,
	.index_fetch_tuple = mars_index_fetch_tuple,

	.tuple_fetch_row_version = mars_tuple_fetch_row_version,
	.tuple_tid_valid = mars_tuple_tid_valid,
	.tuple_get_latest_tid = mars_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = mars_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = mars_compute_xid_horizon_for_tuples,

	.tuple_insert = mars_tuple_insert,
	.tuple_insert_speculative = mars_tuple_insert_speculative,
	.tuple_complete_speculative = mars_tuple_complete_speculative,
	.multi_insert = mars_multi_insert,
	.tuple_delete = mars_tuple_delete,
	.tuple_update = mars_tuple_update,
	.tuple_lock = mars_tuple_lock,
	.finish_bulk_insert = mars_finish_bulk_insert,

	.relation_set_new_filenode = mars_relation_set_new_filenode,
	.relation_nontransactional_truncate = mars_relation_nontransactional_truncate,
	.relation_copy_data = mars_relation_copy_data,
	.relation_copy_for_cluster = mars_relation_copy_for_cluster,
	.relation_vacuum = mars_relation_vacuum ,
	.scan_analyze_next_block = mars_scan_analyze_next_block,
	.scan_analyze_next_tuple = mars_scan_analyze_next_tuple,
	.index_build_range_scan = mars_index_build_range_scan,
	.index_validate_scan = mars_index_validate_scan,

	.relation_size = mars_relation_size,
	.relation_needs_toast_table = mars_relation_needs_toast_table,
	.relation_estimate_size = mars_relation_estimate_size,
	.scan_bitmap_next_block = mars_scan_bitmap_next_block ,
	.scan_bitmap_next_tuple = mars_scan_bitmap_next_tuple,
	.scan_sample_next_block = mars_scan_sample_next_block,
	.scan_sample_next_tuple = mars_scan_sample_next_tuple ,

	.index_fetch_extractcolumns = mars_index_fetch_extractcolumns,

	.event_notify= mars_event_notify,
};

static void
ms_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{

		case XACT_EVENT_PRE_COMMIT:
			break;

		case XACT_EVENT_ABORT:
			mars::CleanInsertEnv();
			break;

		default:
			/* do nothing */
			;
	}
}

Datum
mars_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&mars_method);
}

void _PG_init(void)
{
	RegisterXactCallback(ms_xact_callback, NULL);

	mars_guc_init();

	mars_register_customsortscan_methods();
	mars_register_customaggscan_methods();

	_mars_planner_init();
}

void _PG_fini(void)
{
	mars_unregister_customsortscan_methods();
	mars_unregister_customaggscan_methods();

	mars_guc_fini();
}

Datum
mars_compress_info(PG_FUNCTION_ARGS)
{
	// tuple<batchNo, rowGroupId, CompSize, UncompSize>
	using Meta = std::tuple<int64_t, int32_t , int64_t, int64_t>;
	using Data = std::pair<int, std::vector<Meta>* >;

	MemoryContext   mctx;
	FuncCallContext *fctx;
	Oid     relOid = PG_GETARG_OID(0);
	int     segNo  = PG_GETARG_INT32(1);
	int64   batch  = PG_GETARG_INT64(2);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupleDesc;
		fctx = SRF_FIRSTCALL_INIT();

		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		std::shared_ptr<parquet::FileMetaData> metadata;
		Relation rel = table_open(relOid, AccessShareLock);

		std::vector<Meta>* pResVec = new std::vector<Meta> {};
		Data* pData = new Data {};

		auto snapshot = ::GetTransactionSnapshot();
		mars::footer::Footer::footers_type footers;
		mars::footer::FetchFooters(fctx->multi_call_memory_ctx, footers,
								   rel, snapshot);

		// fetch compression info by each footer
		for (auto &footer: footers) {
			if (footer.impl_->batch_ != batch) {
				continue;
			}

			uint32 size;
			auto content = footer.GetFooterContent(&size);
			metadata = parquet::FileMetaData::Make(content, &size);

			for (int i = 0; i < metadata->num_row_groups(); i++) {
				auto rg = metadata->RowGroup(i);
				int64_t compress_size = 0;
				int64_t uncompress_size = 0;
				for (int j = 0;j < rg->num_columns(); j++) {
					compress_size += rg->ColumnChunk(j)->total_compressed_size();
					uncompress_size += rg->ColumnChunk(j)->total_uncompressed_size();
				}

				// do not use implicit initializer, .push_back({1,2,3}), which
				// is not supported by gcc 4.8 .  We have to use that version
				// on centos 7 because that is the gcc version used by the
				// official parquet rpm, if we compile mars.so with a different
				// gcc version, unexpected crash would happen.
				pResVec->push_back(std::make_tuple(footer.impl_->batch_,
												   i,
												   compress_size,
												   uncompress_size));
			}
		}

		table_close(rel, AccessShareLock);

		tupleDesc = CreateTemplateTupleDesc(5);

		TupleDescInitEntry(tupleDesc, (AttrNumber) 1, "SegNo",
		                   INT4OID, -1, 0);

		TupleDescInitEntry(tupleDesc, (AttrNumber) 2, "Batch",
		                   INT8OID, -1, 0);

		TupleDescInitEntry(tupleDesc, (AttrNumber) 3, "RowGroupId",
		                   INT4OID, -1, 0);

		TupleDescInitEntry(tupleDesc, (AttrNumber) 4, "Compression Size",
		                   INT8OID, -1, 0);

		TupleDescInitEntry(tupleDesc, (AttrNumber) 5, "UNcompression Size",
		                   INT8OID, -1, 0);

		fctx->tuple_desc = ::BlessTupleDesc(tupleDesc);

		pData->first = 0;

		pData->second= pResVec;

		fctx->user_fctx = pData;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();

	if (segNo != -1 && segNo != GpIdentity.segindex)
	{
		SRF_RETURN_DONE(fctx);
	}

	Data *pData = static_cast<Data *>(fctx->user_fctx);
	std::vector<Meta>* pMetaVec = pData->second;
	::Datum values[5];
	bool isnulls[5];
	::HeapTuple tuple;

	if (pData->first < (int) pMetaVec->size()) {
		Datum           result;

		values[0] = Int32GetDatum(GpIdentity.segindex);
		values[1] = Int64GetDatum(std::get<0>(pMetaVec->at(pData->first)));
		values[2] = Int32GetDatum(std::get<1>(pMetaVec->at(pData->first)));
		values[3] = Int64GetDatum(std::get<2>(pMetaVec->at(pData->first)));
		values[4] = Int64GetDatum(std::get<3>(pMetaVec->at(pData->first)));

		memset(isnulls, false, sizeof(isnulls));

		tuple = heap_form_tuple(fctx->tuple_desc, values, isnulls);
		result = HeapTupleGetDatum(tuple);
		pData->first ++;

		SRF_RETURN_NEXT(fctx, result);
	}

	delete pMetaVec;
	delete pData;

	SRF_RETURN_DONE(fctx);
}

Datum
mars_preagg_sum(PG_FUNCTION_ARGS)
{
	return mars::tools::mars_preagg_sum(fcinfo);
}

Datum
mars_catalog_manual_init(PG_FUNCTION_ARGS)
{
	bool load = true;

	mars_monotonic_function_catalog_init(&load);

	return BoolGetDatum(true);
}
