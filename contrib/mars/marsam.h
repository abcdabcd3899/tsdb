/*-------------------------------------------------------------------------
 *
 * marsam.h
 *	  MARS storage read/write interface
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/marsam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MARSAM_H
#define MATRIXDB_MARSAM_H

extern "C"
{

#include "wrapper.hpp"

    /* ------------------------------------------------------------------------
     * Txn callbacks.
     * ------------------------------------------------------------------------
     */
    void mars_storage_txn_cb(bool isCommit);

    /* ------------------------------------------------------------------------
     * Table scan callbacks.
     * ------------------------------------------------------------------------
     */
    TableScanDesc mars_scan_begin(Relation rel,
                                  Snapshot snapshot,
                                  int nkeys, struct ScanKeyData *key,
                                  ParallelTableScanDesc pscan,
                                  uint32 flags);

    TableScanDesc mars_scan_begin_extractcolumns(Relation rel,
                                                 Snapshot snapshot,
                                                 List *targetlist,
                                                 List *qual,
                                                 ParallelTableScanDesc pscan,
                                                 uint32 flags);

    TableScanDesc mars_scan_begin_extractcolumns_bm(Relation rel, Snapshot snapshot,
                                                    List *targetList, List *quals,
                                                    List *bitmapqualorig,
                                                    uint32 flags);

    void mars_scan_end(TableScanDesc scan);

    void mars_scan_rescan(TableScanDesc scan, struct ScanKeyData *key,
                          bool set_params, bool allow_strat,
                          bool allow_sync, bool allow_pagemode);

    bool mars_scan_getnextslot(TableScanDesc scan, ScanDirection direction,
                               TupleTableSlot *slot);

    /* ------------------------------------------------------------------------
     * Parallel table scan related functions.
     * ------------------------------------------------------------------------
     */

    Size mars_parallelscan_estimate(Relation rel);

    Size mars_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);

    void mars_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);

    /* ------------------------------------------------------------------------
     * Index Scan Callbacks
     * ------------------------------------------------------------------------
     */

    struct IndexFetchTableData *mars_index_fetch_begin(Relation rel);

    void mars_index_fetch_reset(struct IndexFetchTableData *data);

    void mars_index_fetch_end(struct IndexFetchTableData *data);

    bool mars_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid,
                                Snapshot snapshot, TupleTableSlot *slot,
                                bool *call_again, bool *all_dead);

    /* ------------------------------------------------------------------------
     * Callbacks for non-modifying operations on individual tuples
     * ------------------------------------------------------------------------
     */

    bool mars_tuple_fetch_row_version(Relation rel, ItemPointer tid,
                                      Snapshot snapshot, TupleTableSlot *slot);

    bool mars_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);

    void mars_tuple_get_latest_tid(TableScanDesc scan,
                                   ItemPointer tid);

    bool mars_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                       Snapshot snapshot);

    TransactionId mars_compute_xid_horizon_for_tuples(Relation rel,
                                                      ItemPointerData *items,
                                                      int nitems);

    /* ------------------------------------------------------------------------
     * Manipulations of physical tuples.
     * ------------------------------------------------------------------------
     */

    void mars_tuple_insert(Relation rel, TupleTableSlot *slot,
                           CommandId cid, int options,
                           struct BulkInsertStateData *bistate);

    void mars_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
                                       CommandId cid, int options,
                                       struct BulkInsertStateData *bistate,
                                       uint32 specToken);

    void mars_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
                                         uint32 specToken, bool succeeded);

    void mars_multi_insert(Relation rel, TupleTableSlot **slots,
                           int nslots, CommandId cid, int options,
                           struct BulkInsertStateData *bistate);

    TM_Result mars_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
                                Snapshot snapshot, Snapshot crosscheck, bool wait,
                                TM_FailureData *tmfd, bool changingPart);

    TM_Result mars_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
                                CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                                bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
                                bool *update_indexes);

    TM_Result mars_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
                              TupleTableSlot *slot, CommandId cid,
                              LockTupleMode mode, LockWaitPolicy wait_policy,
                              uint8 flags, TM_FailureData *tmfd);

    void mars_finish_bulk_insert(Relation rel, int options);

    /* ------------------------------------------------------------------------
     * DDL related functionality.
     * ------------------------------------------------------------------------
     */
    void mars_relation_set_new_filenode(Relation rel, const RelFileNode *newrnode,
                                        char persistence, TransactionId *freezeXid,
                                        MultiXactId *minmulti);

    void mars_relation_nontransactional_truncate(Relation rel);

    void mars_relation_copy_data(Relation rel, const RelFileNode *newrnode);

    void mars_relation_copy_for_cluster(Relation NewTable, Relation OldTable,
                                        Relation OldIndex, bool use_sort,
                                        TransactionId OldestXmin,
                                        TransactionId *xid_cutoff,
                                        MultiXactId *multi_cutoff,
                                        double *num_tuples,
                                        double *tups_vacuumed,
                                        double *tups_recently_dead);

    void mars_relation_vacuum(Relation onerel, struct VacuumParams *params,
                              BufferAccessStrategy bstrategy);

    bool mars_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                      BufferAccessStrategy bstrategy);

    bool mars_scan_analyze_next_tuple(TableScanDesc scan,
                                      TransactionId OldestXmin,
                                      double *liverows, double *deadrows,
                                      TupleTableSlot *slot);

    double mars_index_build_range_scan(Relation table_rel,
                                       Relation index_rel,
                                       struct IndexInfo *index_info,
                                       bool allow_sync,
                                       bool anyvisible,
                                       bool progress,
                                       BlockNumber start_blockno,
                                       BlockNumber numblocks,
                                       IndexBuildCallback callback,
                                       void *callback_state,
                                       TableScanDesc scan);

    void mars_index_validate_scan(Relation table_rel, Relation index_rel,
                                  struct IndexInfo *index_info,
                                  Snapshot snapshot,
                                  struct ValidateIndexState *state);

    /* ------------------------------------------------------------------------
     * Miscellaneous functions.
     * ------------------------------------------------------------------------
     */
    uint64 mars_relation_size(Relation rel, ForkNumber forkNumber);

    bool mars_relation_needs_toast_table(Relation rel);

    /* ------------------------------------------------------------------------
     * Planner related functions.
     * ------------------------------------------------------------------------
     */
    void mars_relation_estimate_size(Relation rel, int32 *attr_widths,
                                     BlockNumber *pages, double *tuples,
                                     double *allvisfrac);

    /* ------------------------------------------------------------------------
     * Executor related functions.
     * ------------------------------------------------------------------------
     */
    bool mars_scan_bitmap_next_block(TableScanDesc scan,
                                     struct TBMIterateResult *tbmres);

    bool mars_scan_bitmap_next_tuple(TableScanDesc scan,
                                     struct TBMIterateResult *tbmres,
                                     TupleTableSlot *slot);

    bool mars_scan_sample_next_block(TableScanDesc scan,
                                     struct SampleScanState *scanstate);

    bool mars_scan_sample_next_tuple(TableScanDesc scan,
                                     struct SampleScanState *scanstate,
                                     TupleTableSlot *slot);

    void mars_index_fetch_extractcolumns(struct IndexFetchTableData *scan,
                                         List *targetlist, List *qual);

    void
    mars_event_notify(Relation relation, CmdType operation, void *param);

} /* extern "C" finish */

namespace mars
{
    void CleanInsertEnv();
}

#endif // MATRIXDB_MARSAM_H
