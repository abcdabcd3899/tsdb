/*-------------------------------------------------------------------------
 *
 * Scanner.h
 *	  The MARS scanner
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"

#include "Scanner.h"
#include "BlockReader.h"
#include "Footer.h"
#include "ScanKey.h"
#include "utils.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <set>
#include <vector>

namespace mars
{

	static bool
	_find_unique(const std::vector<::AttrNumber> &v, const ::AttrNumber &x)
	{
		return std::find(v.begin(), v.end(), x) != v.end();
	}

	static void
	_insert_unique(std::vector<::AttrNumber> &v, const ::AttrNumber &x)
	{
		if (!_find_unique(v, x))
		{
			v.push_back(x);
		}
	}

	/// Adjust the scankey orders.
	static void
	_reorder_scankeys(const std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
					  const std::vector<::AttrNumber> &order,
					  std::vector<std::shared_ptr<mars::ScanKey>> &keys,
					  std::vector<std::shared_ptr<mars::ScanKey>> &filters,
					  Scanner::EndOfKey &eok)
	{
		std::set<int> unused;
		for (unsigned long i = 0; i < scankeys.size(); ++i)
		{
			unused.insert(i);
		}

		eok.primary = 0;
		eok.secondary = 0;

		keys.clear();
		filters.clear();

		::AttrNumber first_order = order.size() > 0 ? order[0] : InvalidAttrNumber;
		::AttrNumber second_order = order.size() > 1 ? order[1] : InvalidAttrNumber;

		// first put the ordered scankeys
		for (auto &attr : order)
		{
			bool found = false;
			for (unsigned long i = 0; i < scankeys.size(); ++i)
			{
				auto &scankey = scankeys[i];

				if (scankey->GetArrayKeyType() ==
					mars::ScanKey::ArrayKeyType::ARRAY_FILTER)
				{
					// an array filter is never a scankey
					filters.push_back(scankey);
					continue;
				}

				if (scankey->scankey_.sk_attno == attr)
				{
					keys.push_back(scankey);
					unused.erase(i);
					found = true;

					if (first_order == attr)
					{
						// this is a primary scankey
						eok.primary = keys.size();
					}
					else if (second_order == attr)
					{
						// this is a primary scankey
						eok.secondary = keys.size();
					}
				}
			}

			if (!found)
			{
				// found a hole, keys after a hole is considered as unordered
				break;
			}
		}

		// then put all the other scankeys
		for (auto i : unused)
		{
			auto &scankey = scankeys[i];

			filters.push_back(scankey);
		}
	}

	/// Find out the arraykey.
	static std::shared_ptr<mars::ScanKey>
	_find_arraykey(const std::vector<::AttrNumber> &order, bool reorder,
				   std::vector<std::shared_ptr<mars::ScanKey>> &scankeys)
	{
		std::shared_ptr<mars::ScanKey> result;

		// when reordering is needed, no primary arraykey is allowed.
		if (order.empty() || reorder)
		{
			return result;
		}

		auto first_attr = order.front();
		for (auto &iter : scankeys)
		{
			if (iter->scankey_.sk_attno != first_attr)
			{
				continue;
			}
			if (iter->GetArrayKeyType() == mars::ScanKey::ArrayKeyType::NO_ARRAY)
			{
				continue;
			}

			result = iter;
			break;
		}

		return result;
	}

	Scanner::Scanner(::Relation rel, ::Snapshot snapshot, bool is_analyze)
		: rel_(rel), snapshot_(snapshot), block_never_match_(false), table_never_match_(false), scandir_(), is_analyze_(is_analyze), projcols_(), scan_order_(), block_store_order_(), table_store_order_(), block_scan_order_(), table_scan_order_(), block_reorder_(false), table_reorder_(false), scankeys_(), filters_(), arraykeys_(nullptr), block_keys_(), block_filters_(), table_keys_(), table_filters_(), block_eok_(), table_eok_(), table_arraykey_(nullptr), groupkeys_(), comps_(RelationGetDescr(rel)), column_stores_(), remap_(), auxindexoid_(InvalidOid), time_bucket_attr_(0), time_bucket_interval_()
	{
		if (!rel_)
		{
			// this happens only in utittests, in real world a scanner must be
			// bound to a valid data relation.
			return;
		}

		{
			auto relid = RelationGetRelid(rel_);

			std::vector<::MarsDimensionAttr> table_order_dims;
			std::vector<::MarsDimensionAttr> block_order_dims;

			utils::GetRelOptions(relid, nullptr /* groupkeys */,
								 &table_order_dims, &block_order_dims);

			std::vector<::AttrNumber> table_order_keys;
			std::vector<::AttrNumber> block_order_keys;

			table_order_keys.resize(table_order_dims.size());
			std::transform(table_order_dims.begin(), table_order_dims.end(),
						   table_order_keys.begin(),
						   [](const ::MarsDimensionAttr &dimattr) -> ::AttrNumber
						   {
							   return dimattr.attnum;
						   });

			block_order_keys.resize(block_order_dims.size());
			std::transform(block_order_dims.begin(), block_order_dims.end(),
						   block_order_keys.begin(),
						   [](const ::MarsDimensionAttr &dimattr) -> ::AttrNumber
						   {
							   return dimattr.attnum;
						   });

			SetStoreOrder(block_order_keys, table_order_keys);
		}

		{
			const auto tupdesc = RelationGetDescr(rel_);

			column_stores_.resize(tupdesc->natts);
			for (size_t i = 0; i < column_stores_.size(); ++i)
			{
				column_stores_[i] = ColumnStore::Make(comps_, i);
			}

			utils::GetRelOptions(RelationGetRelid(rel), &groupkeys_,
								 nullptr /* global_orderkeys */,
								 nullptr /* local_orderkeys */);
		}
	}

	void
	Scanner::SetProjectedColumns(const std::vector<bool> &projs)
	{
		std::set<int> cols;

		for (unsigned int i = 0; i < projs.size(); ++i)
		{
			if (projs.at(i))
			{
				cols.insert(i);
			}
		}

		// groupkeys and global orderkeys must always be projected
		for (const auto &groupkey : groupkeys_)
		{
			auto column = AttrNumberGetAttrOffset(groupkey.attnum);
			cols.insert(column);
		}
		for (const auto &orderkey : table_store_order_)
		{
			auto column = AttrNumberGetAttrOffset(orderkey);
			cols.insert(column);
		}

		projcols_.clear();
		projcols_.reserve(cols.size());
		for (auto column : cols)
		{
			projcols_.push_back(column);
		}
	}

	void
	Scanner::SetProjectedColumns()
	{
		auto tupdesc = RelationGetDescr(rel_);
		auto natts = tupdesc->natts;

		projcols_.resize(natts);
		std::iota(projcols_.begin(), projcols_.end(), 0);
	}

	void
	Scanner::SetStoreOrder(const std::vector<::AttrNumber> &block_order,
						   const std::vector<::AttrNumber> &table_order)
	{
		block_store_order_.clear();
		for (auto attr : block_order)
		{
			if (attr > 0)
			{
				_insert_unique(block_store_order_, attr);
			}
		}

		SetTableStoreOrder(table_order);
	}

	void
	Scanner::SetTableStoreOrder(const std::vector<::AttrNumber> &table_order)
	{
		table_store_order_.clear();
		for (auto attr : table_order)
		{
			if (attr > 0)
			{
				_insert_unique(table_store_order_, attr);
			}
		}
	}

	void
	Scanner::SetOrderKeys(const std::vector<::AttrNumber> &orderkeys)
	{
		scan_order_ = orderkeys;

		// assume that we could keep our native orders
		block_scan_order_ = block_store_order_;
		table_scan_order_ = table_store_order_;
		block_reorder_ = false;
		table_reorder_ = false;

		// the store order must be set before this

		if (scan_order_.empty())
		{
			// fastpath: the scan is unordered, just keep our native orders
			return;
		}

		// decide table-level scan order
		if (table_store_order_.empty())
		{
			// fastpath: table-level reorder is needed.
			//
			// this should not happen, the orderkeys are decided in
			// build_relation_pathkeys(), so the order must matches our policies.
			table_reorder_ = true;
			table_scan_order_ = scan_order_;

			elog(WARNING, "mars: table-level reordering is not implemented");
		}
		else
		{
			auto pair = std::mismatch(scan_order_.begin(), scan_order_.end(),
									  table_store_order_.begin());
			if (pair.first == scan_order_.end())
			{
				// the scan order is the prefix of the table-level store order, no
				// table-level reorder is needed.
			}
			else if (pair.second == table_store_order_.end())
			{
				// the table-level store order is the prefix of the scan order, no
				// table-level reorder is needed.
				//
				// FIXME: this is only true when a) there are no intervals
				// involved, or b) the table-level interval is times of the scan
				// interval.
			}
			else
			{
				// table-level reorder is needed
				table_reorder_ = true;
				table_scan_order_ = scan_order_;

				elog(WARNING, "mars: table-level reordering is not implemented");
			}
		}

		// decide block-level scan order.
		//
		// groupkeys are not considered here, so the block-level reorder might be
		// enabled unnecessarily, but don't worry, in ReorderBlock() there is the
		// second round of decision based on the single-value columns.
		//
		// TODO: the decisions made here are for SetScanKeys() only, so maybe we
		// want to split SetScanKeys() to SetTableScanKeys() and SetBlockScanKeys(),
		// the latter should be called after the opening of each block.
		if (block_store_order_.empty())
		{
			// fastpath: block-level reorder is needed
			block_reorder_ = true;
			block_scan_order_ = scan_order_;
		}
		else
		{
			auto pair = std::mismatch(scan_order_.begin(), scan_order_.end(),
									  block_store_order_.begin());
			if (pair.first == scan_order_.end())
			{
				// the scan order is the prefix of the block-level store order, no
				// block-level reorder is needed.
			}
			else if (pair.second == block_store_order_.end())
			{
				// the block-level store order is the prefix of the scan order,
				// block-level reorder is needed.
				block_reorder_ = true;
				block_scan_order_ = scan_order_;
			}
			else
			{
				// block-level reorder is needed
				block_reorder_ = true;
				block_scan_order_ = scan_order_;
			}
		}
	}

	void
	Scanner::SetScanKeys(std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
						 std::shared_ptr<std::vector<::IndexArrayKeyInfo>> &&arraykeys,
						 std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> &&runtimekeys)
	{
		scankeys_.clear();
		scankeys_.reserve(scankeys.size());
		filters_.clear();
		filters_.reserve(scankeys.size());
		arraykeys_ = std::move(arraykeys);
		runtimekeys_ = std::move(runtimekeys);

		// sort original scankeys by attrnum, which is required by below function
		// FIXME: we should sort with the order keys.
		using ScanKeyType = std::shared_ptr<mars::ScanKey>;
		auto comp = [](const ScanKeyType &a, const ScanKeyType &b) -> bool
		{
			return a->scankey_.sk_attno < b->scankey_.sk_attno;
		};
		std::sort(scankeys.begin(), scankeys.end(), comp);

		// process the arraykeys.
		table_arraykey_ = _find_arraykey(table_store_order_, table_reorder_, scankeys);

		// process the arraykeys to identify array keys and filters, also convert
		// the array values to a set for fast matching.
		for (auto &iter : scankeys)
		{
			iter->PreprocessArrayKeys();
		}

		for (auto &iter : scankeys)
		{
			if (iter->GetArrayKeyType() == mars::ScanKey::ArrayKeyType::NO_ARRAY)
			{
				scankeys_.push_back(iter);
			}
			else
			{
				// force all the arraykeys to be filters, the primary ones will be
				// upgraded later.
				if (iter != table_arraykey_)
				{
					// block and table arraykeys are handled separately
					filters_.push_back(iter);
				}
			}
		}
	}

	void
	Scanner::OptimizeScanKeys(bool table_level)
	{
		std::vector<std::shared_ptr<mars::ScanKey>> keys;
		// the original size of scankeys
		auto nscankeys = scankeys_.size();

		// put the primary arraykey into the scankeys according to the level
		if (table_level)
		{
			if (table_arraykey_)
			{
				scankeys_.push_back(table_arraykey_);
			}
		}
		else
		{
			// the table-level arraykey is already filled with a specific value, it
			// can be used as a normal scankey at the block-level.
			if (table_arraykey_)
			{
				Assert(table_arraykey_->GetArrayKeyType() ==
					   mars::ScanKey::ArrayKeyType::ARRAY_KEY);

				scankeys_.push_back(table_arraykey_);
			}
		}

		// optimize the scankeys
		auto never_match = !mars::ScanKey::Reduce(scankeys_, keys);
		if (table_level)
		{
			table_never_match_ = never_match;
		}
		else
		{
			block_never_match_ = never_match;
		}

		// restore the original size.
		scankeys_.resize(nscankeys);

		if (never_match)
		{
			return;
		}

		// process the scankeys.
		if (!table_level)
		{
			// always use the store order as the result order at block level:
			//
			// - when no reorder is needed, the scankeys are used as postkeys;
			// - when reorder is needed, the scankeys are used as prekeys;
			//
			// in either case, scan with the store order can better leverage the
			// order based optimizations.
			_reorder_scankeys(keys, block_store_order_,
							  block_keys_, block_filters_, block_eok_);

			// use the array filters as the filters.
			for (auto &iter : filters_)
			{
				Assert(iter->GetArrayKeyType() ==
					   mars::ScanKey::ArrayKeyType::ARRAY_FILTER);

				block_filters_.push_back(iter);
			}
		}
		else
		{
			// we do not support merge-sort of blocks yet.
			Assert(!table_reorder_);
			// no reorder is needed, use the scankeys as the postkeys, use the
			// store order as the result order.
			_reorder_scankeys(keys, table_store_order_,
							  table_keys_, table_filters_, table_eok_);

			// use the array filters as the filters.
			for (auto &iter : filters_)
			{
				Assert(iter->GetArrayKeyType() ==
					   mars::ScanKey::ArrayKeyType::ARRAY_FILTER);

				table_filters_.push_back(iter);
			}
		}
	}

	int
	Scanner::GetEffectiveRowCount(const mars::footer::Footer &footer) const
	{
		const auto &keys = block_keys_;

		if (keys.empty())
		{
			// no primary key in the block-level, return full row count.
			return footer.GetRowCount();
		}
		else
		{
			// there is a primary order, return the non-null row count of that
			// column.
			auto column = keys[0]->scankey_.sk_attno - 1;
			return footer.GetRowCount(column);
		}
	}

	void
	Scanner::SortArrayKeys()
	{
		if (arraykeys_ == nullptr)
		{
			// no arraykey at all.
			return;
		}

		for (auto &arraykey : *(arraykeys_.get()))
		{
			auto nelems = arraykey.num_elems;

			if (nelems == 0)
			{
				continue;
			}

			// now we are in a per tuple memory context, so do not re-allocate
			// the original values and nulls buffers.
			std::vector<::Datum> values(nelems);
			std::vector<bool> nulls(nelems);

			for (auto i = 0; i < nelems; ++i)
			{
				values[i] = arraykey.elem_values[i];
				nulls[i] = arraykey.elem_nulls[i];
			}

			/*
			 * "x in (null)" is always null even when x is null, so there is no
			 * match, so "x in (1, 2, null, null)" can be deduced by removing
			 * all the null values, which becomes "x in (1, 2)".
			 *
			 * however "x in (null, null)" is deduced to "x in (null)", so the
			 * scanner can distinguish it from the no-arraykey case.
			 */
			arraykey.num_elems = 0;
			for (auto i = 0; i < nelems; ++i)
			{
				/* only keep the non-null values. */
				if (nulls[i])
					continue;

				arraykey.elem_values[arraykey.num_elems] = values[i];
				arraykey.elem_nulls[arraykey.num_elems] = nulls[i];
				++(arraykey.num_elems);
			}

			if (arraykey.num_elems == 0)
			{
				/* but leave one null when there are only nulls. */
				arraykey.elem_values[0] = 0;
				arraykey.elem_nulls[0] = true;
				arraykey.num_elems = 1;
			}
			else
			{
				/* the non-null values should be sorted. */
				std::sort(&arraykey.elem_values[0],
						  &arraykey.elem_values[arraykey.num_elems]);
			}
		}
	}

	bool
	Scanner::OptimizeBlockScanKeys()
	{
		OptimizeScanKeys(false /* table_level */);

		return true;
	}

	bool
	Scanner::TableArrayScanBegin()
	{
		if (table_arraykey_.get())
		{
			if (!table_arraykey_->ArrayKeyScanBegin(scandir_))
			{
				return false;
			}

			OptimizeScanKeys(true /* table_level */);

			if (table_never_match_)
			{
				return TableArrayScanNext();
			}
		}
		else
		{
			// this is the best place to optimize the scankeys
			OptimizeScanKeys(true /* table_level */);
		}

		return true;
	}

	bool
	Scanner::TableArrayScanNext()
	{
		if (table_arraykey_.get())
		{
			do
			{
				if (!table_arraykey_->ArrayKeyScanNext(scandir_))
				{
					return false;
				}
				OptimizeScanKeys(true /* table_level */);
			} while (table_never_match_);
			return true;
		}
		else
		{
			return false;
		}
	}

	void
	Scanner::FirstMatchBlock(BlockSlice &blocks) const
	{
		if (blocks.IsEmpty())
		{
			// nothing to do if the footers table is empty
			return;
		}

		if (table_never_match_)
		{
			blocks.Clear();
			return;
		}

		if (table_reorder_)
		{
			// TODO: merge sort all the blocks
		}

		// search with only the primary scankeys
		const auto &keys = table_keys_;
		int nkeys = table_eok_.primary;

		if (keys.empty())
		{
			// 1. there are no ordered scankeys at all:
			// no optimization is possible, simply scan from the beginning.
		}
		else
		{
			// 2. there are ordered keys:

			// 2.1. we can do a bisect on the first key
			// XXX: in fact we could do a bisect on all the ordered keys, but it
			// might not be efficient enough for a column-store.
			for (int i = 0; i < nkeys; ++i)
			{
				auto &key = keys[i];

				key->FirstMatchBlock(blocks);
				if (blocks.IsEmpty())
				{
					// no match in the range
					return;
				}
			}
		}

		// 2.2. anyway, we can do a block by block filtering as long as there are
		// some scankeys.
		NextMatchBlock(blocks);
	}

	void
	Scanner::NextMatchBlock(BlockSlice &blocks) const
	{
		if (table_keys_.empty() && table_filters_.empty())
		{
			// always consider as match if no scankey at all
			return;
		}

		for (; !blocks.IsEmpty(); blocks.Next())
		{
			const auto &footer = blocks.GetBlock();
			auto match = MatchBlock(footer);

			if (match == mars::ScanKey::MatchType::MATCH ||
				match == mars::ScanKey::MatchType::MATCH_FULL)
			{
				// this block matches all the keys
				// TODO: can we have some special handlings on BLOCK_FULL_MATCH?
				return;
			}
			else if (match == mars::ScanKey::MatchType::MISMATCH_FIRST_ORDERKEY)
			{
				// no more match is possible
				blocks.Clear();
				return;
			}
		}
	}

	mars::ScanKey::MatchType
	Scanner::MatchBlock(const mars::footer::Footer &footer, bool logical) const
	{
		// search with all the scankeys
		const auto &keys = table_keys_;
		const auto &filters = table_filters_;

		// assume we have a full match
		auto final_match = mars::ScanKey::MatchType::BLOCK_FULL_MATCH;

		// FIXME: currently we are assuming that all the scankeys are ANDed
		// FIXME: currently we are using scankeys as filters
		for (int i = 0; i < (int)keys.size(); ++i)
		{
			auto &key = keys[i];
			auto &scankey = key->scankey_;
			int column = scankey.sk_attno - 1;

			auto match = key->MatchBlock(footer, column, logical);
			if (match == mars::ScanKey::MatchType::BLOCK_MISMATCH)
			{
				if (i < table_eok_.primary)
				{
					return mars::ScanKey::MatchType::MISMATCH_FIRST_ORDERKEY;
				}
				else if (i < table_eok_.secondary)
				{
					return mars::ScanKey::MatchType::MISMATCH_SECOND_ORDERKEY;
				}
				else
				{
					return mars::ScanKey::MatchType::MISMATCH_FILTER;
				}
			}
			else if (match == mars::ScanKey::MatchType::BLOCK_MAYBE_MATCH)
			{
				// downgrade the match level accordingly
				final_match = match;
			}
		}

		for (int i = 0; i < (int)filters.size(); ++i)
		{
			auto &key = filters[i];
			auto &scankey = key->scankey_;
			int column = scankey.sk_attno - 1;

			auto match = key->MatchBlock(footer, column, logical);
			if (match == mars::ScanKey::MatchType::BLOCK_MISMATCH)
			{
				return mars::ScanKey::MatchType::MISMATCH_FILTER;
			}
			else if (match == mars::ScanKey::MatchType::BLOCK_MAYBE_MATCH)
			{
				// downgrade the match level accordingly
				final_match = match;
			}
		}

		return final_match;
	}

	::OffsetNumber
	Scanner::SearchTuple(const ::TupleTableSlot *slot, const ::AttrNumber merge_attr)
	{
		::OffsetNumber ip_posid = FirstOffsetNumber;
		int id = AttrNumberGetAttrOffset(merge_attr);
		auto &comparator = comps_.GetComparator(id);
		for (auto &v : column_stores_[id]->Values())
		{
			if (comparator.equal(v, slot->tts_values[id]))
				break;

			ip_posid++;
		}

		ip_posid = ip_posid > column_stores_[id]->Values().size() ? InvalidOffsetNumber : ip_posid;

		return ip_posid;
	}

	void
	Scanner::ReorderBlock(const footer::Footer &footer)
	{
		if (remap_.empty())
		{
			// fastpath: no row matches the block filters
			return;
		}

		auto reorder = block_reorder_;
		std::vector<::AttrNumber> store_order;
		std::vector<::AttrNumber> scan_order;

		if (reorder)
		{
			// reorder was considered as needed for the block, however when there
			// are proper single-value columns there are still chances to skip the
			// reordering.
			store_order.reserve(block_store_order_.size());
			scan_order.reserve(block_scan_order_.size());

			for (auto attr : block_store_order_)
			{
				int column = AttrNumberGetAttrOffset(attr);
				if (!footer.SingleValue(column, GetColumnComparator(column)))
				{
					store_order.push_back(attr);
				}
			}

			for (auto attr : block_scan_order_)
			{
				int column = AttrNumberGetAttrOffset(attr);
				if (!footer.SingleValue(column, GetColumnComparator(column)))
				{
					scan_order.push_back(attr);
				}
			}

			// no reorder is needed if scan_order is a prefix of store_order
			if (scan_order.size() <= store_order.size())
			{
				auto test = std::mismatch(scan_order.begin(), scan_order.end(),
										  store_order.begin());
				reorder = test.first != scan_order.end();
			}
		}

		if (!reorder)
		{
			// no need to reorder
			return;
		}

		ColumnStoreSorter sorter(scan_order);
		sorter.BindData(&column_stores_, &remap_);

		// TODO: the interval should be considered by IsSorted(), it is possible
		// that the data is already ordered by time_bucket(ts), but not by ts,
		// there is no need to Sort() in such a case.
		//
		// on the other hand, we always reorder in the ASC direction, because we
		// are adjusting the storing order.  The scan order, or call it the
		// reporting order, is controled by the caller.
		if (!sorter.IsSorted())
		{
			// do not need to consider time bucket when sorting, when the data is
			// ordered by ts, it is ordered by time_bucket(ts), too, of course.
			sorter.Sort();
		}
	}

	bool
	Scanner::GetRowsFilter(const footer::Footer &footer,
						   std::vector<std::shared_ptr<mars::ScanKey>> &filters,
						   bool overlapped) const
	{
		std::vector<std::shared_ptr<mars::ScanKey>> allfilters;

		// use all the arraykeys as filters
		allfilters.resize(block_keys_.size() + block_filters_.size());
		std::copy(block_keys_.begin(), block_keys_.end(),
				  allfilters.begin());

		// however block filters can only be used in non-overlapping mode
		if (overlapped)
			allfilters.resize(block_keys_.size());
		else
			std::copy(block_filters_.begin(), block_filters_.end(),
					  allfilters.begin() + block_keys_.size());

		filters.clear();
		filters.reserve(allfilters.size());

		for (const auto &_filter : allfilters)
		{
			auto &filter = *_filter.get();
			auto sk_isnull = filter.scankey_.sk_flags & SK_ISNULL;
			auto column = AttrNumberGetAttrOffset(filter.scankey_.sk_attno);

			if (footer.SingleValue(column, GetColumnComparator(column)))
			{
				// this a single value column, for example, the group key.
				// - if the value matches, all the rows match, so no need to touch
				//   the bitmap;
				// - otherwise no match is possible;
				auto &cstore = column_stores_[column];
				auto &isnull = cstore->Nulls();
				auto &values = cstore->Values();

				if (sk_isnull)
				{
					if (!isnull.front())
					{
						// no match is possible
						return false;
					}
				}
				else
				{
					Assert(!values.empty());
					if (!filter.MatchPoint(values.front()))
					{
						// no match is possible
						return false;
					}
				}

				// all the rows match, useless
				continue;
			}

			// optimize with block min & max
			auto match = filter.MatchBlock(footer, column);
			if (match == mars::ScanKey::MatchType::BLOCK_FULL_MATCH)
			{
				// all the rows match, useless
				continue;
			}
			else if (match == mars::ScanKey::MatchType::BLOCK_MISMATCH)
			{
				// no match is possible, this should not happen otherwise we won't
				// reach here, the block won't be scanned at all.
				return false;
			}

			// ok, this is a useful filter
			filters.push_back(_filter);
		}

		return true;
	}

	bool
	Scanner::GetRowsRange(const footer::Footer &footer, int &first, int &last,
						  std::vector<std::shared_ptr<mars::ScanKey>> &filters) const
	{
		// the primary order attrnum
		::AttrNumber primary = InvalidAttrNumber;

		// find the first non-single-value order key
		if (!block_store_order_.empty())
		{
			for (auto attrnum : block_store_order_)
			{
				int column = AttrNumberGetAttrOffset(attrnum);
				if (!footer.SingleValue(column, GetColumnComparator(column)))
				{
					primary = attrnum;
					break;
				}
			}
		}

		// shrink the matching range by bsearch on the primary order key
		if (!AttrNumberIsForUserDefinedAttr(primary))
		{
			// fastpath: no primary orderkey, keep the range untouched
			return true;
		}

		// split filters into primary keys and other filters
		std::vector<std::shared_ptr<mars::ScanKey>> primarykeys;
		std::vector<std::shared_ptr<mars::ScanKey>> otherfilters;
		primarykeys.reserve(filters.size());
		otherfilters.reserve(filters.size());
		for (auto key : filters)
		{
			if (key->scankey_.sk_attno == primary)
			{
				primarykeys.push_back(std::move(key));
			}
			else
			{
				otherfilters.push_back(std::move(key));
			}
		}

		// as we will run binary search on the primary key, there is no need to
		// filter them row by row again, so delete them from the original filters.
		filters = std::move(otherfilters);

		// the primary order column index
		auto column = AttrNumberGetAttrOffset(primary);
		const auto &cstore = *column_stores_[column];

		// there are no null values in the primary order keys, so we do the
		// bsearch on the values only.
		auto begin0 = std::next(cstore.Values().cbegin(), first);
		auto end0 = std::next(cstore.Values().cbegin(), last);
		auto begin = begin0;
		auto end = end0;

		for (const auto &_key : primarykeys)
		{
			auto &key = *_key.get();

			if (key.GetArrayKeyType() != mars::ScanKey::ArrayKeyType::NO_ARRAY)
			{
				// it might not be worthy to do binary search on arraykeys
				// TODO: but maybe we could do it with the min & max values.
				filters.push_back(std::move(_key));
				continue;
			}

			const auto &comp = GetColumnComparator(column);
			auto arg = key.scankey_.sk_argument;
			switch (key.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// move end to last pos where end[-1] < arg, it equals:
				// move end to first pos where end[-1] >= arg
				end = std::lower_bound(begin, end, arg, comp);
				break;
			case BTLessEqualStrategyNumber:
				// move end to last pos where end[-1] <= arg, it equals:
				// move end to first pos where end[-1] > arg
				end = std::upper_bound(begin, end, arg, comp);
				break;
			case BTEqualStrategyNumber:
			{
				auto pair = std::equal_range(begin, end, arg, comp);
				begin = pair.first;
				end = pair.second;
			}
			break;
			case BTGreaterEqualStrategyNumber:
				// move begin to the first pos where *begin >= arg
				begin = std::lower_bound(begin, end, arg, comp);
				break;
			case BTGreaterStrategyNumber:
				// move begin to the first pos where *begin > arg
				begin = std::upper_bound(begin, end, arg, comp);
				break;
			}

			if (begin == end)
			{
				// fastpath: no match is possible
				return false;
			}
		}

		Assert(begin != end);

		first = std::distance(begin0, begin);
		last = std::distance(begin0, end);
		return true;
	}

	bool
	Scanner::FilterRows(const footer::Footer &footer, int &nrows, bool overlapped)
	{
		if (nrows <= 0)
		{
			// no match is possible
			match_type_ = mars::ScanKey::MatchType::BLOCK_MISMATCH;
			matches_.clear();
			return false;
		}

		std::vector<std::shared_ptr<mars::ScanKey>> filters;
		if (!GetRowsFilter(footer, filters, overlapped))
		{
			// no match is possible
			match_type_ = mars::ScanKey::MatchType::BLOCK_MISMATCH;
			matches_.clear();
			return false;
		}

		if (filters.empty())
		{
			// fastpath: all rows match
			match_type_ = mars::ScanKey::MatchType::BLOCK_FULL_MATCH;
			matches_.clear();
			return true;
		}

		ColumnStore::FilterContext ctx;
		ctx.footer = &footer;
		ctx.matches = &matches_;
		ctx.first = 0;
		ctx.last = nrows;

		if (!GetRowsRange(footer, ctx.first, ctx.last, filters))
		{
			// no match is possible
			match_type_ = mars::ScanKey::MatchType::BLOCK_MISMATCH;
			matches_.clear();
			return false;
		}

		Assert(ctx.first != ctx.last);
		nrows = ctx.last;

		// assume that the range [first, last) match
		matches_.clear();
		matches_.resize(nrows, true);
		if (ctx.first > 0)
		{
			// mark [0, first) as not matching
			std::fill(matches_.begin(), std::next(matches_.begin(), ctx.first),
					  false);
		}

		ctx.has_mismatch = ctx.first != 0 || ctx.last != nrows;

		SortBlockFilters(filters);

		for (const auto &_filter : filters)
		{
			auto &filter = *_filter.get();
			auto column = AttrNumberGetAttrOffset(filter.scankey_.sk_attno);
			auto &cstore = *column_stores_[column];

			ctx.filter = &filter;

			if (!cstore.FilterRows(ctx))
			{
				// fastpath: no match in current column, so no match at all
				match_type_ = mars::ScanKey::MatchType::BLOCK_MISMATCH;
				matches_.clear();
				return false;
			}
		}

		if (!ctx.has_mismatch)
		{
			match_type_ = mars::ScanKey::MatchType::BLOCK_FULL_MATCH;
			matches_.clear();
		}
		else
		{
			match_type_ = mars::ScanKey::MatchType::BLOCK_MAYBE_MATCH;
		}

		return true;
	}

	bool
	Scanner::CollectRowIds(int &nrows)
	{
		if (match_type_ == mars::ScanKey::MatchType::BLOCK_FULL_MATCH)
		{
			remap_.resize(nrows);
			std::iota(remap_.begin(), remap_.end(), 0);
		}
		else if (match_type_ == mars::ScanKey::MatchType::BLOCK_MISMATCH)
		{
			remap_.clear();
			nrows = 0;
		}
		else
		{
			Assert(nrows <= (int)matches_.size());
			remap_.resize(nrows);

			auto iter = remap_.begin();
			for (int row = 0; row < nrows; ++row)
			{
				if (matches_[row])
				{
					*iter = row;
					++iter;
				}
			}
			Assert(iter <= remap_.end());

			// decide the actual # of rows
			nrows = std::distance(remap_.begin(), iter);
			remap_.resize(nrows);
		}

		return !remap_.empty();
	}

	void
	Scanner::SortBlockFilters(std::vector<std::shared_ptr<mars::ScanKey>> &filters) const
	{
		constexpr int rc_null = 100;  // IS NULL / IS NOT NULL
		constexpr int rc_byval = 200; // filter on byval type
		constexpr int rc_byptr = 300; // filter on non-byval type

		auto classify = [](const std::shared_ptr<mars::ScanKey> &filter) -> int
		{
			const auto &scankey = filter->scankey_;
			if (scankey.sk_flags & SK_ISNULL)
				return rc_null;
			else if (get_typbyval(scankey.sk_subtype))
				return rc_byval;
			else
				return rc_byptr;
		};

		auto comp = [classify](const std::shared_ptr<mars::ScanKey> &a,
							   const std::shared_ptr<mars::ScanKey> &b) -> bool
		{
			return classify(a) < classify(b);
		};

		std::sort(filters.begin(), filters.end(), comp);
	}

	void
	Scanner::BeginRow(int nrows)
	{
		if (match_type_ == mars::ScanKey::MatchType::BLOCK_MISMATCH)
		{
			lazy_rows_.BindEmpty();
		}
		else
		{
			lazy_rows_.Bind(&column_stores_, &projcols_, scandir_, nrows, &remap_);
		}
	}

} // namespace mars
