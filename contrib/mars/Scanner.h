/*-------------------------------------------------------------------------
 *
 * Scanner.h
 *	  The MARS Scanner
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_SCANNER_H
#define MARS_SCANNER_H

#include "type_traits.hpp"

#include "BlockSlice.h"
#include "ColumnStore.h"
#include "FileManager.h"
#include "Footer.h"
#include "RowStore.h"
#include "ScanKey.h"

#include <memory>
#include <vector>

namespace mars
{

	/*!
	 * The MARS Scanner.
	 *
	 * The MARS Scanner performs optimized scanning with the scankeys and filters.
	 * However it does not involve the actual data loading logic, the caller is
	 * responsible to load the data into the scanner's column stores.
	 *
	 * @see ParquetReader::ReadNext(), BlockReader::ReadNext()
	 */
	class Scanner
	{
	public:
		/*!
		 * The scanning stage.
		 */
		enum Stage
		{
			//! filtering tuples before or during the reordering.
			PREFILTER,
			//! filtering tuples after the reordering.
			POSTFILTER,
			//! filtered all the tuples, the only thing is to report them.
			FILTERED,
		};

		/*!
		 * Ending positions of each kind of scankeys.
		 *
		 * @note This could be retired soon, it is not very useful.
		 */
		struct EndOfKey
		{
			EndOfKey()
				: primary(0), secondary(0)
			{
			}

			int primary;   /*!< the end of the primary keys, 0 means no */
			int secondary; /*!< the end of the secondary keys, 0 means no */
		};

		/*!
		 * The ctor of the scanner.
		 *
		 * @param rel The relation, it must be valid and open during the scanning.
		 *            Only in unittests we allow it to be InvalidOid.
		 */
		Scanner(::Relation rel, ::Snapshot snapshot, bool is_analyze = false);

		/*!
		 * Set the scan direction.
		 *
		 * @param scandir The scan direction.
		 */
		void SetScanDir(::ScanDirection scandir)
		{
			scandir_ = scandir;
		}

		/*!
		 * Get the scan direction.
		 *
		 * @return The scan direction.
		 */
		::ScanDirection GetScanDir() const
		{
			return scandir_;
		}

		bool GetIsAnalyze() const
		{
			return is_analyze_;
		}

		const std::vector<std::shared_ptr<mars::ScanKey>> &
		GetTableKeys()
		{
			return table_keys_;
		}

		/*!
		 * Set the projected columns.
		 *
		 * By providing this information, we could only check and return the
		 * involved columns.
		 *
		 * @param projs The projected column information, when \p projs[0] is true,
		 *              the column 0, attr 1, is projected and needed.
		 */
		void SetProjectedColumns(const std::vector<bool> &projs);

		/*!
		 * Mark all the columns as projected.
		 */
		void SetProjectedColumns();

		/*!
		 * Set the store orders.
		 *
		 * This is automatically called by the ctor, so do not call it explicitly
		 * unless in unittests.
		 *
		 * @param block_order The block-level store order.
		 * @param table_order The table-level store order.
		 */
		void SetStoreOrder(const std::vector<::AttrNumber> &block_order,
						   const std::vector<::AttrNumber> &table_order);

		void SetTableStoreOrder(const std::vector<::AttrNumber> &table_order);

		void SetAuxIndexOid(::Oid oid)
		{
			auxindexoid_ = oid;
		}

		::Oid GetIndexOid() const
		{
			return auxindexoid_;
		}

		const std::vector<::AttrNumber> &GetTableStoreOrder(void) const
		{
			return table_store_order_;
		}

		std::vector<::AttrNumber> &GetTableStoreOrder(void)
		{
			return table_store_order_;
		}

		std::vector<::AttrNumber> &GetBlockStoreOrder(void)
		{
			return block_store_order_;
		}

		const std::vector<::AttrNumber> &GetScanOrder() const
		{
			return scan_order_;
		}

		const std::vector<::MarsDimensionAttr> &GetGroupKeys() const
		{
			return groupkeys_;
		}

		const DatumCompare &GetColumnComparator(int column) const
		{
			return comps_.GetComparator(column);
		}

		const RelationCompare &GetRelationComparator() const
		{
			return comps_;
		}

		/*!
		 * Set the orderkeys, the output order of the result.
		 *
		 * @param orderkeys The orderkeys.
		 *
		 * @note To set both orderkeys and scankeys, the orderkeys must be set
		 *       prior to the scankeys.
		 *
		 * @see SetScanKeys()
		 * @todo Merge SetOrderKeys() and SetScanKeys() into one.
		 */
		void SetOrderKeys(const std::vector<::AttrNumber> &orderkeys);

		/*!
		 * Set the time bucket information.
		 *
		 * With the time bucket information the data will be sorted by
		 * `time_bucket(interval, time)` instead of `time`.  The time column does
		 * not need to be the only scan order key.
		 *
		 * @param attr The attribute number of the timestamp column.
		 * @param interval The time bucket interval.
		 */
		void SetTimeBucket(::AttrNumber attr, const ::Interval &interval)
		{
			time_bucket_attr_ = attr;
			time_bucket_interval_ = interval;
		}

		/*!
		 * Set the scankeys.
		 *
		 * Scankeys describe `WHERE` conditions like `c1 = 1`, `c2 > 2`, etc., they
		 * are further divided into scankeys and filters.  A scankey is a condition
		 * on an orderkey, otherwise it is only a filter.  Scankeys can be used to
		 * seek the begin and end of the matching range, filters can only be used
		 * to test whether a block or row matches or not, so scankeys are more
		 * efficient than filters.
		 *
		 * Arraykeys, which describe the `IN` expressions, are also supported,
		 * the caller must first bind the arraykeys to the \p scankeys with the
		 * ScanKey::BindArrayKey(), then initialize them by calling the
		 * ::ExecIndexEvalArrayKeys() before setting the scankeys.
		 *
		 * MARS supports one primary array key at block level, and/or one at table
		 * level, the primary array keys are stored in #block_arraykey_ and
		 * #table_arraykey_.  Non-primary array keys, aka array filters, are stored
		 * in #filters_.
		 *
		 * @param scankeys The scankeys, must be valid during the scanning, it is
		 *                 recommended to transfer the ownership to scanner.
		 * @param arraykeys The arraykeys, which describe the `IN` expressions,
		 *                  must be valid during the scanning, it is recommended to
		 *                  transfer the ownership to scanner.  The caller is
		 *                  responsible to call ::ExecIndexEvalArrayKeys() to
		 *                  initialize the arraykeys.
		 *
		 * @note To set both orderkeys and scankeys, the orderkeys must be set
		 *       prior to the scankeys.
		 *
		 * @see SetOrderKeys()
		 * @see mars::ScanKey::ArrayKeyType for the definitions of array keys and
		 *      array filters.
		 * @todo Merge SetOrderKeys() and SetScanKeys() into one.
		 */
		void SetScanKeys(std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
						 std::shared_ptr<std::vector<::IndexArrayKeyInfo>> &&arraykeys,
						 std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> &&runtimekeys);

		/*!
		 * Set only the scankeys, no arraykeys or runtimekeys.
		 *
		 * This is only called by SeqScan, which does not pass a valid ScanState to
		 * mars and thus we have no chance to extract the arraykeys or runtimekeys.
		 */
		void SetScanKeys(std::vector<std::shared_ptr<mars::ScanKey>> &scankeys)
		{
			std::shared_ptr<std::vector<::IndexArrayKeyInfo>> arraykeys;
			std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> runtimekeys;

			SetScanKeys(scankeys, std::move(arraykeys), std::move(runtimekeys));
		}

		std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> GetRuntimeKeys() const
		{
			return runtimekeys_;
		}

		/*!
		 * Optimize the scankeys and filters.
		 *
		 * It removes useless keys, for example, `c1 > 1 and c1 > 2` is reduced to
		 * `c1 > 2`.
		 *
		 * It also detects paradox keys, for example, `c1 > 1 and c1 < 1`, in such
		 * a case we can stop the scan immediately because we believe that no match
		 * is possible.
		 *
		 * This function should be called after SetOrderKeys() and SetScanKeys(),
		 * and should be re-called in every arraykey round.
		 *
		 * @param table_level Optimizing at table-level if it is true, or at
		 *                    block-level if it is false.
		 *
		 * @todo This function is only called by the scanner itself, make it
		 *       private.
		 */
		void OptimizeScanKeys(bool table_level);

		/*!
		 * Getter of the projected columns.
		 *
		 * @return A readonly ref to the projected column information.
		 */
		const std::vector<int> &GetProjectedColumns() const
		{
			return projcols_;
		}

		/*!
		 * Getter of the column stores.
		 *
		 * @return A writable ref to the column stores.
		 *
		 * @todo We should redesign the column store handling, do not let the
		 *       caller modifies the column stores directly.
		 */
		std::vector<std::shared_ptr<ColumnStore>> &GetColumnStores()
		{
			return column_stores_;
		}

		std::vector<int> &GetReorderMap()
		{
			return remap_;
		}

		/*!
		 * Get the relnode oid of the relation bound to the scanner.
		 *
		 * @return The relnode.
		 */
		::Oid GetDataRelNode() const
		{
			return rel_->rd_node.relNode;
		}

		/*!
		 * Get the data filepath of the relation bound to the scanner.
		 *
		 * @return The filepath.
		 */
		std::string GetDataFilepath(const mars::footer::Footer &footer) const
		{
			auto filepath = mars::GetRelationPath(rel_->rd_node, InvalidBackendId,
												  footer.impl_->segno_);
			std::string result(filepath);
			pfree(filepath);
			return result;
		}

		/*!
		 * Get the effecitive row count.
		 *
		 * When there is a primary order key, it is the non-null row count of that
		 * column, otherwise it is the full row count.
		 *
		 * @param footer The footer.
		 *
		 * @return The effective row count.
		 */
		int GetEffectiveRowCount(const mars::footer::Footer &footer) const;

		/*!
		 * Sort the arraykeys according to the scandir.
		 */
		void SortArrayKeys();

		/*!
		 * Optimize scankeys, at the block-level.
		 */
		bool OptimizeBlockScanKeys();

		/*!
		 * Begin arraykey scan, at the table-level.
		 *
		 * @return false if there are an arraykey, but it contains only null
		 *         values, so no match is possible, the caller should stop asap.
		 * @return true otherwise, there are no arraykey, or a non-null value, the
		 *         scan should continue.
		 *
		 * @see BlockArrayScanBegin()
		 */
		bool TableArrayScanBegin();

		/*!
		 * Move to the next round of the arraykey scan, at the table-level.
		 *
		 * @see BlockArrayScanNext()
		 */
		bool TableArrayScanNext();

		/*!
		 * Seek the first matching block in the range.
		 *
		 * On return, the \p blocks is seeked to the first / last matching block,
		 * depends on the scan direction.  If no match is found, \p blocks becomes
		 * empty, the caller should end the scan.
		 *
		 * The seeking is a bsearch, based on the scankeys.  It is usually a good
		 * practice to call NextMatchBlock() after this one, which checks the
		 * filters.
		 *
		 * @param[in,out] blocks The block slice to scan.
		 */
		void FirstMatchBlock(BlockSlice &blocks) const;

		/*!
		 * Move to the first matching block in the range [begin, end).
		 *
		 * On return, the \p begin or \p end points to the first matching block,
		 * depends on the scan direction.  If no match is found, \p begin and \p
		 * end will be equal, the caller should end the scan.
		 *
		 * This performs a block-by-block checking, with both scankeys and filters.
		 *
		 * @param[in,out] blocks The block slice to scan.
		 *
		 * @todo A better name would be SkipUnmatchBlocks().
		 */
		void NextMatchBlock(BlockSlice &blocks) const;

		/*!
		 * Test whether a block matches the scankeys and filters.
		 *
		 * @param footer The block to test.
		 * @param logical Test as a logical block or not.
		 *
		 * @return ScanKey::BLOCK_FULL_MATCH if the block can be fully matched, all
		 *         the tuples of the block should pass all the scankeys and
		 *         filters, in theory we do not need to filter the tuples one by
		 *         one.
		 * @return ScanKey::BLOCK_MAYBE_MATCH if the block is possibly to have some
		 *         matches.
		 * @return ScanKey::MISMATCH_FIRST_ORDERKEY if the block mismatches on a
		 *         primary key, so no more match is possible in the block, the
		 *         caller should end the scan.
		 * @return ScanKey::MISMATCH_FILTER if the block mismatches on a filter,
		 *         the caller should skip to the next block.
		 */
		mars::ScanKey::MatchType MatchBlock(const mars::footer::Footer &footer,
											bool logical = false) const;

		::OffsetNumber SearchTuple(const ::TupleTableSlot *slot, const ::AttrNumber merge_attr);

		void ReorderBlock(const footer::Footer &footer);

		bool GetRowsFilter(const footer::Footer &footer,
						   std::vector<std::shared_ptr<mars::ScanKey>> &filters,
						   bool overlapped) const;
		bool GetRowsRange(const footer::Footer &footer, int &first, int &last,
						  std::vector<std::shared_ptr<mars::ScanKey>> &filters) const;
		bool FilterRows(const footer::Footer &footer, int &nrows, bool overlapped);
		bool CollectRowIds(int &nrows);

		/*!
		 * Sort the filters for better performance.
		 *
		 * Sort in below order:
		 * - IS NULL / IS NOT NULL
		 * - keys on byval types
		 * - keys on non-byval types
		 *
		 * The purpose is to reduce the actual calls to pg opers, especially the
		 * calls on slow types.
		 */
		void SortBlockFilters(std::vector<std::shared_ptr<mars::ScanKey>> &filters) const;

		void BeginRow(int nrows);

		bool NextRow(::TupleTableSlot *slot, int &offset)
		{
			return lazy_rows_.NextRow(slot, offset);
		}

		/*!
		 * The relation bound to the scanner, allow readonly access publicly.
		 *
		 * @todo There isn't any reason to bind a relation to the scanner, move it
		 *       to some other classes.
		 */
		const ::Relation rel_;

		/*!
		 * The snapshot for the scanning.
		 */
		const ::Snapshot snapshot_;

		/*!
		 * The block-level never-match flag.
		 *
		 * When it is true, no match is possible in the block, the caller should
		 * move to next block, or next array key, immediately.
		 *
		 * Public access is allowed because once the called takes the action to end
		 * the current scan loop, it needs to clear this flag.
		 */
		bool block_never_match_;

		/*!
		 * The table-level never-match flag.
		 *
		 * When it is true, no match is possible in the table, the caller should
		 * end the scan, or move to next array key, immediately.
		 *
		 * Public access is allowed because once the called takes the action to end
		 * the current scan loop, it needs to clear this flag.
		 */
		bool table_never_match_;

	protected:
		/*!
		 * The scan direction.
		 */
		::ScanDirection scandir_;

		/*!
		 * True if doing analyze scan.
		 */
		bool is_analyze_;

		/*!
		 * The projected columns.
		 *
		 * @see SetProjectedColumns() and GetProjectedColumns()
		 */
		std::vector<int> projcols_;

		/*!
		 * The original scan order set by SetOrderKeys().
		 *
		 * The table-level and block-level scan orders are determined based this
		 * information, as well as the store orders.
		 */
		std::vector<::AttrNumber> scan_order_;

		/*!
		 * The block-level store order.
		 *
		 * It is specified during the creation of the MARS table, like below:
		 *
		 *     CREATE TABLE t1 USING mars
		 *       WITH (block_order_attr1=1, block_order_attr2=2);
		 */
		std::vector<::AttrNumber> block_store_order_;

		/*!
		 * The table-level store order.
		 *
		 * It is specified during the creation of the MARS table, like below:
		 *
		 *     CREATE TABLE t1 USING mars
		 *       WITH (table_order_attr1=1, table_order_attr2=2);
		 */
		std::vector<::AttrNumber> table_store_order_;

		/*!
		 * The block-level scan order.
		 *
		 * It is determined by the #scan_order_ and #block_store_order_.
		 */
		std::vector<::AttrNumber> block_scan_order_;

		/*!
		 * The table-level scan order.
		 *
		 * It is determined by the #scan_order_ and #table_store_order_.
		 */
		std::vector<::AttrNumber> table_scan_order_;

		/*!
		 * Whether to reorder at the block-level.
		 *
		 * When this flag is true, the rows should be reordered at block-level
		 * according to the #block_scan_order_.
		 */
		bool block_reorder_;

		/*!
		 * Whether to reorder at the table-level.
		 *
		 * When this flag is true, a merge-sort of all the blocks should be
		 * performed at the table-level, the resulting order should be compatitble
		 * with #block_scan_order_.
		 *
		 * @todo This is currently unsupported.
		 */
		bool table_reorder_;

		/*!
		 * The original scankeys set by the SetScanKeys().
		 *
		 * The table-level and block-level scankeys and filters are determined in
		 * OptimizeScanKeys().
		 */
		std::vector<std::shared_ptr<mars::ScanKey>> scankeys_;

		/*!
		 * The filters.
		 *
		 * They are never considered as scankeys, at present only array filters are
		 * put here.
		 */
		std::vector<std::shared_ptr<mars::ScanKey>> filters_;

		/*!
		 * The arraykeys set by the SetScanKeys().
		 *
		 * MARS supports one arraykey at block level, as well as table table, the
		 * effective arraykeys are stored in #block_arraykey_ and #table_arraykey_.
		 * The other arraykeys are not used by MARS, but they are checked by the
		 * SeqScan node, anyway, so the result is still correct.
		 */
		std::shared_ptr<std::vector<::IndexArrayKeyInfo>> arraykeys_;

		std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> runtimekeys_;

		//! The block-level scankeys.
		std::vector<std::shared_ptr<mars::ScanKey>> block_keys_;
		//! The block-level filters.
		std::vector<std::shared_ptr<mars::ScanKey>> block_filters_;
		//! The table-level scankeys.
		std::vector<std::shared_ptr<mars::ScanKey>> table_keys_;
		//! The table-level filters.
		std::vector<std::shared_ptr<mars::ScanKey>> table_filters_;

		//! The block-level EoK.
		EndOfKey block_eok_;
		//! The table-level EoK.
		EndOfKey table_eok_;

		//! The table-level arraykeys.
		std::shared_ptr<mars::ScanKey> table_arraykey_;

		std::vector<::MarsDimensionAttr> groupkeys_;

		const RelationCompare comps_;

		/*!
		 * The column stores.
		 *
		 * The caller is responsible to fill the column stores.
		 */
		std::vector<std::shared_ptr<ColumnStore>> column_stores_;

		/*!
		 * The column store reorder map.
		 *
		 * It is only used when scanning a block which requires block-level
		 * reordering, only indices of matching rows are put into the remap, so no
		 * more filtering is needed when reading the reordered rows.
		 */
		std::vector<int> remap_;

		std::vector<bool> matches_;
		mars::ScanKey::MatchType match_type_;

		RowStore lazy_rows_;

		::Oid auxindexoid_;

		::AttrNumber time_bucket_attr_;
		::Interval time_bucket_interval_;
	};

} // namespace mars

#endif /* MARS_SCANNER_H */
