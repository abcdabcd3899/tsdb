/*-------------------------------------------------------------------------
 *
 * ParquetReader.cc
 *	  The MARS Parquet Reader
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "Footer.h"
#include "wrapper.hpp"

#include "ParquetReader.h"
#include "FileManager.h"
#include "tools.h"
#include "utils.h"

#include <algorithm>

extern "C"
{

	/*
	 * Derived from ExecIndexBuildScanKeys():
	 *
	 * ExecIndexBuildScanKeys
	 *		Build the index scan keys from the index qualification expressions
	 *
	 * The index quals are passed to the index AM in the form of a ScanKey array.
	 * This routine sets up the ScanKeys, fills in all constant fields of the
	 * ScanKeys, and prepares information about the keys that have non-constant
	 * comparison values.  We divide index qual expressions into five types:
	 *
	 * 1. Simple operator with constant comparison value ("indexkey op constant").
	 * For these, we just fill in a ScanKey containing the constant value.
	 *
	 * 2. Simple operator with non-constant value ("indexkey op expression").
	 * For these, we create a ScanKey with everything filled in except the
	 * expression value, and set up an IndexRuntimeKeyInfo struct to drive
	 * evaluation of the expression at the right times.
	 *
	 * 3. RowCompareExpr ("(indexkey, indexkey, ...) op (expr, expr, ...)").
	 * For these, we create a header ScanKey plus a subsidiary ScanKey array,
	 * as specified in access/skey.h.  The elements of the row comparison
	 * can have either constant or non-constant comparison values.
	 *
	 * 4. ScalarArrayOpExpr ("indexkey op ANY (array-expression)").  If the index
	 * supports amsearcharray, we handle these the same as simple operators,
	 * setting the SK_SEARCHARRAY flag to tell the AM to handle them.  Otherwise,
	 * we create a ScanKey with everything filled in except the comparison value,
	 * and set up an IndexArrayKeyInfo struct to drive processing of the qual.
	 * (Note that if we use an IndexArrayKeyInfo struct, the array expression is
	 * always treated as requiring runtime evaluation, even if it's a constant.)
	 *
	 * 5. NullTest ("indexkey IS NULL/IS NOT NULL").  We just fill in the
	 * ScanKey properly.
	 *
	 * This code is also used to prepare ORDER BY expressions for amcanorderbyop
	 * indexes.  The behavior is exactly the same, except that we have to look up
	 * the operator differently.  Note that only cases 1 and 2 are currently
	 * possible for ORDER BY.
	 *
	 * Input params are:
	 *
	 * planstate: executor state node we are working for
	 * index: the index we are building scan keys for
	 * quals: indexquals (or indexorderbys) expressions
	 * isorderby: true if processing ORDER BY exprs, false if processing quals
	 * *runtimeKeys: ptr to pre-existing IndexRuntimeKeyInfos, or NULL if none
	 * *numRuntimeKeys: number of pre-existing runtime keys
	 *
	 * Output params are:
	 *
	 * *scanKeys: receives ptr to array of ScanKeys
	 * *numScanKeys: receives number of scankeys
	 * *runtimeKeys: receives ptr to array of IndexRuntimeKeyInfos, or NULL if none
	 * *numRuntimeKeys: receives number of runtime keys
	 * *arrayKeys: receives ptr to array of IndexArrayKeyInfos, or NULL if none
	 * *numArrayKeys: receives number of array keys
	 *
	 * Caller may pass NULL for arrayKeys and numArrayKeys to indicate that
	 * IndexArrayKeyInfos are not supported.
	 */
	void
	BuildScanKeys(ScanState *ss, List *quals,
				  std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
				  std::shared_ptr<std::vector<::IndexArrayKeyInfo>> &arraykeys,
				  std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> &runtimekeys,
				  int &ndiscarded_keys)
	{
		bool isorderby = false;
		ListCell *qual_cell;
		int j;

		/* Allocate array for ScanKey structs: one per qual */
		scankeys.clear();
		scankeys.reserve(list_length(quals));

		/* Allocate array_keys as large as it could possibly need to be */
		Assert(arraykeys.get());
		arraykeys->clear();
		arraykeys->reserve(list_length(quals));

		runtimekeys->clear();
		runtimekeys->reserve(list_length(quals));

		ndiscarded_keys = 0;

		/*
		 * for each opclause in the given qual, convert the opclause into a single
		 * scan key
		 */
		foreach_with_count(qual_cell, quals, j)
		{
			Expr *clause = (Expr *)lfirst(qual_cell);
			::ScanKey this_scan_key;
			Oid opno;			   /* operator's OID */
			RegProcedure opfuncid; /* operator proc id used in scan */
			Oid opclass;		   /* mars: opclass of the column left type */
			Oid opfamily;		   /* opfamily of index column */
			int op_strategy;	   /* operator's strategy number */
			Oid op_lefttype;	   /* operator's declared input types */
			Oid op_righttype;
			Expr *leftop;		 /* expr on lhs of operator */
			Expr *rightop;		 /* expr on rhs ... */
			AttrNumber varattno; /* att number used in scan */

			if (IsA(clause, OpExpr))
			{
				/* indexkey op const or indexkey op expression */
				int flags = 0;
				Datum scanvalue;
				char *opname;

				opno = ((OpExpr *)clause)->opno;
				opfuncid = ((OpExpr *)clause)->opfuncid;

				opname = get_opname(opno);
				if (strcmp(opname, "<") &&
					strcmp(opname, "<=") &&
					strcmp(opname, "=") &&
					strcmp(opname, ">=") &&
					strcmp(opname, ">"))
					/* "<>", etc., are not supported yet */
					goto unsupported;

				/*
				 * leftop should be the index key Var, possibly relabeled
				 */
				leftop = (Expr *)get_leftop(clause);

				/*
				 * rightop is the constant or variable comparison value
				 */
				rightop = (Expr *)get_rightop(clause);

				if (leftop && IsA(leftop, RelabelType))
					leftop = ((RelabelType *)leftop)->arg;

				Assert(leftop != NULL);

				if (rightop && IsA(rightop, RelabelType))
					rightop = ((RelabelType *)rightop)->arg;

				Assert(rightop != NULL);

				/*
				 * If the clause is grab from JoinRel and MARS is innertable,
				 * we need to swap the OpExpr args.
				 */
				if (IsA(leftop, Param))
				{
					if (strcmp(opname, "=") != 0)
						/* mars only supports equal join */
						goto unsupported;

					std::swap(leftop, rightop);
				}

				if (!IsA(leftop, Var))
				{
					if (IsA(rightop, Var))
					{
						/*
						 * the expr is like "1 > c1", swap left & right, as well
						 * as the operator, so it becomes "c1 < 1".
						 */
						std::swap(leftop, rightop);

						/* also need to reflect the operator, unless it is "=" */
						if (strcmp(opname, "=") != 0)
						{
							opno = get_commutator(opno);
							opfuncid = get_opcode(opno);
						}
					}
					else
						/* the expr is like "1 = 1", mars cannot optimize with it */
						goto unsupported;
				}

				if (IsA(rightop, Var))
					/* the expr is like "c1 = c2", mars cannot optimize with it */
					goto unsupported;

				if (!IsA(rightop, Const) && !ss)
					/* mars: cannot build a runtimekey without a valid ss */
					goto unsupported;

				varattno = ((Var *)leftop)->varattno;
				if (!AttrNumberIsForUserDefinedAttr(varattno))
					/* the expr is on a sysattr, mars cannot optimize with it */
					goto unsupported;

				/* mars: use the btree opclass for the comparison */
				opclass = ::GetDefaultOpClass(((Var *)leftop)->vartype,
											  BTREE_AM_OID);
				opfamily = ::get_opclass_family(opclass);

				::get_op_opfamily_properties(opno, opfamily, isorderby,
											 &op_strategy,
											 &op_lefttype,
											 &op_righttype);

				if (isorderby)
					flags |= SK_ORDER_BY;

				{
					auto collation = ((Var *)leftop)->varcollid;
					auto key = mars::ScanKey::Make(op_lefttype, op_righttype,
												   collation);

					if (key == nullptr)
						/* mars: the type is unsupported yet */
						goto unsupported;

					scankeys.push_back(std::move(key));
					this_scan_key = &scankeys.back()->scankey_;
				}

				/*
				 * We believe the op is supported by mars, so a placeholder scankey
				 * has been pushed to the vector, which will be filled in the
				 * following lines.  Note that no fallback ("goto unsupported")
				 * should happen since this line, otherwise an uninitialized
				 * scankey is left in the vector.
				 */

				if (IsA(rightop, Const))
				{
					/* OK, simple constant comparison value */
					scanvalue = ((Const *)rightop)->constvalue;
					if (((Const *)rightop)->constisnull)
						flags |= SK_ISNULL;
				}
				else
				{
					runtimekeys->emplace_back();
					auto runtimekey = &runtimekeys->back();

					runtimekey->scan_key = this_scan_key;
					runtimekey->key_expr = ::ExecInitExpr(rightop, (PlanState *)ss);
					runtimekey->key_toastable = TypeIsToastable(op_righttype);
					scankeys.back()->BindRuntimeKey(runtimekey);

					scanvalue = (Datum)0;
				}

				/*
				 * initialize the scan key's fields appropriately
				 */
				::ScanKeyEntryInitialize(this_scan_key,
										 flags,
										 varattno,						  /* attribute number to scan */
										 op_strategy,					  /* op's strategy */
										 op_righttype,					  /* strategy subtype */
										 ((OpExpr *)clause)->inputcollid, /* collation */
										 opfuncid,						  /* reg proc to use */
										 scanvalue);					  /* constant */
			}
			else if (IsA(clause, RowCompareExpr))
			{
				// RowCompareExpr is not supported, fallback to seqscan filters.
				// TODO: we should support this
				goto unsupported;
			}
			else if (IsA(clause, ScalarArrayOpExpr))
			{
				/* indexkey op ANY (array-expression) */
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *)clause;
				int flags = 0;
				Datum scanvalue;

				if (!ss)
					/* mars: cannot build an arraykey without a valid ss */
					continue;

				Assert(!isorderby);

				Assert(saop->useOr);
				opno = saop->opno;
				opfuncid = saop->opfuncid;

				/*
				 * leftop should be the index key Var, possibly relabeled
				 */
				leftop = (Expr *)linitial(saop->args);

				if (leftop && IsA(leftop, RelabelType))
					leftop = ((RelabelType *)leftop)->arg;

				Assert(leftop != NULL);

				if (!IsA(leftop, Var))
					goto unsupported;

				varattno = ((Var *)leftop)->varattno;
				if (!AttrNumberIsForUserDefinedAttr(varattno))
					/* the expr is on a sysattr, mars cannot optimize with it */
					goto unsupported;

				auto dup = [&varattno](const ::IndexArrayKeyInfo &arraykey) -> bool
				{
					return arraykey.scan_key->sk_attno == varattno;
				};
				if (std::find_if(arraykeys->begin(),
								 arraykeys->end(), dup) != arraykeys->end())
				{
					// mars: multiple IN exprs on the same attr is not supported.
					goto unsupported;
				}

				/* mars: use the btree opclass for the comparison */
				opclass = ::GetDefaultOpClass(((Var *)leftop)->vartype,
											  BTREE_AM_OID);
				opfamily = ::get_opclass_family(opclass);
				::get_op_opfamily_properties(opno, opfamily, isorderby,
											 &op_strategy,
											 &op_lefttype,
											 &op_righttype);

				/*
				 * rightop is the constant or variable array value
				 */
				rightop = (Expr *)lsecond(saop->args);

				if (rightop && IsA(rightop, RelabelType))
					rightop = ((RelabelType *)rightop)->arg;

				Assert(rightop != NULL);

				{
					auto collation = ((Var *)leftop)->varcollid;
					auto key = mars::ScanKey::Make(op_lefttype, op_righttype,
												   collation);
					if (key == nullptr)
						/* mars: the type is unsupported yet */
						goto unsupported;
					scankeys.push_back(std::move(key));
				}
				this_scan_key = &scankeys.back()->scankey_;

				/* Executor has to expand the array value */
				arraykeys->emplace_back();
				auto &arraykey = arraykeys->back();
				arraykey.scan_key = this_scan_key;
				scankeys.back()->BindArrayKey(&arraykey);

				/* mars: FIXME: how to get the planstate? do we really need it? */
				arraykey.array_expr = ::ExecInitExpr(rightop, (PlanState *)ss);
				/* the remaining fields were zeroed by palloc0 */
				scanvalue = (Datum)0;

				/*
				 * initialize the scan key's fields appropriately
				 */
				::ScanKeyEntryInitialize(this_scan_key,
										 flags,
										 varattno,			/* attribute number to scan */
										 op_strategy,		/* op's strategy */
										 op_righttype,		/* strategy subtype */
										 saop->inputcollid, /* collation */
										 opfuncid,			/* reg proc to use */
										 scanvalue);		/* constant */
			}
			else if (IsA(clause, NullTest))
			{
				// NullTest is not supported, fallback to seqscan filters.
				// TODO: we should support this
				goto unsupported;
			}
			else
			{
				// unsupported indexqual type, such as OR, fallback to seqscan
				// filters.
				goto unsupported;
			}

			if (false)
			// only goto statement can reach here!
			unsupported:
			{
				ndiscarded_keys++;
			}
		}
	}

} // extern "C"

namespace mars
{

	ParquetReader::ParquetReader(::Relation rel, ::Snapshot snapshot,
								 bool is_analyze)
		: scanner_(rel, snapshot, is_analyze), mreader_(scanner_), merger_(scanner_), rel_(rel), seeked_(false), footerscan_ntuples_(0.0), contentscan_ntuples_(0.0)
	{
		per_table_mcxt_ = AllocSetContextCreate(TopTransactionContext,
												"mars table context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
		per_block_mcxt_ = AllocSetContextCreate(per_table_mcxt_,
												"mars block context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	}

	ParquetReader::~ParquetReader()
	{
		merger_.End();
		mreader_.EndScan();

		footerscan_tlist_.clear();
		footerscan_group_tlist_.clear();

		if (per_table_mcxt_)
		{
			MemoryContextDelete(per_table_mcxt_);
		}
	}

	void
	ParquetReader::Rewind(::ScanDirection direction)
	{
		mreader_.Rewind(direction);

		merger_.Rescan();

		// we have seeked the first block, at least we tried.
		seeked_ = true;
	}

	void
	ParquetReader::Rescan()
	{
		merger_.Rescan();
		seeked_ = false;
	}

	bool
	ParquetReader::ReadNext(::ScanDirection direction, ::TupleTableSlot *slot)
	{
		bool found = true;
		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		while (scanner_.table_never_match_ ||
			   !merger_.PopMergedRow(slot))
		{
			if (scanner_.GetIsAnalyze())
			{
				// in ANALYZE mode we only scan one block each time, which is
				// picked by the ANALYZE command itself.
				found = false;
				break;
			}

			// move to the next block
			if (!NextBlock(direction))
			{
				found = false;
				break;
			}

			// this block is likely to have a match
			Assert(!mreader_.IsEnd());
			LoadBlock();
		}

		::MemoryContextSwitchTo(oldcxt);
		return found;
	}

	bool
	ParquetReader::NextBlock(::ScanDirection direction)
	{
		if (!seeked_)
		{
			// this is the first call, try to seek the first block.
			scanner_.SetScanDir(direction);
			scanner_.SortArrayKeys();

			if (!scanner_.TableArrayScanBegin())
			{
				return false;
			}

			Rewind(direction);
		}
		else
		{
			mreader_.NextBlock(direction);
		}

		while (scanner_.table_never_match_ || mreader_.IsEnd())
		{
			if (scanner_.TableArrayScanNext())
			{
				Rewind(direction);
				// rescan with the new array key
				continue;
			}
			else
			{
				return false;
			}
		}

		return true;
	}

	void
	ParquetReader::LoadBlock()
	{
		::MemoryContextReset(per_block_mcxt_);
		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		if (!mreader_.IsEnd())
		{
			const auto &footers = mreader_.GetFooters();
			bool overlapped = footers.size() > 1;

			for (const auto &footer : footers)
			{
				merger_.PushBlock(*footer, overlapped);
			}
		}

		::MemoryContextSwitchTo(oldcxt);
	}

	bool
	ParquetReader::SetScanTargetBlock(::BlockNumber blockno)
	{
		auto scandir = ForwardScanDirection;

		if (mreader_.SeekBlock(scandir, blockno))
		{
			LoadBlock();
			return true;
		}
		else
		{
			return false;
		}
	}

	bool
	ParquetReader::FooterRead(::ScanDirection direction,
							  ::TupleTableSlot *slot, bool plain_agg)
	{
		// Cannot leverage pre-agg info in footer
		// because not all scan keys are supported
		if (can_aggscan_)
		{
			return false;
		}

		if (mreader_.IsEnd())
		{
			return false;
		}

		const auto &footers = mreader_.GetFooters();
		Assert(!footers.empty());

		// the most up-to-date preagg information is always in the latest footer,
		// and the merge reader ensures that the returned footers are sorted from
		// old to new, so only need to use the last for footer scan.
		const auto &footer = *footers.back();

		// The aggregate is valid when:
		// 1) All tuples are matching the scankeys.
		// 2) Current footer contains only one group (need to know aggregate)
		if (scanner_.MatchBlock(footer) !=
			mars::ScanKey::MatchType::BLOCK_FULL_MATCH)
		{
			return false;
		}

		if (!plain_agg)
		{
			for (const auto &tle : footerscan_group_tlist_)
			{
				if (!tle->UniqueWithinFooter(footer))
				{
					return false;
				}
			}
		}

		for (const auto &tle : footerscan_tlist_)
		{
			if (!tle->Compatible(footer))
			{
				// the tle requires to fallback to data scan
				return false;
			}
		}

		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		for (unsigned int i = 0; i < footerscan_tlist_.size(); ++i)
		{
			auto tle = footerscan_tlist_[i];

			slot->tts_isnull[i] = !tle->HasResult(footer);
			if (slot->tts_isnull[i])
			{
				continue;
			}
			slot->tts_values[i] = tle->GetDatumValue(footer);
		}

		footerscan_ntuples_ += footer.GetRowCount();

		::ExecClearTuple(slot);
		::ExecStoreVirtualTuple(slot);

		::MemoryContextSwitchTo(oldcxt);

		return true;
	}

	bool
	ParquetReader::ReadNextInRowGroup(::ScanDirection direction,
									  ::TupleTableSlot *slot)
	{
		bool found = false;
		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		if (merger_.PopMergedRow(slot))
		{
			::ExecClearTuple(slot);
			::ExecStoreVirtualTuple(slot);

			contentscan_ntuples_++;
			found = true;
		}

		::MemoryContextSwitchTo(oldcxt);
		return found;
	}

	void
	ParquetReader::ExtractColumns(::ScanState *ss)
	{
		::Relation rel = ss->ss_currentRelation;
		auto natts = RelationGetNumberOfAttributes(rel);
		::List *targetlist = ss->ps.plan->targetlist;
		::List *qual = ss->ps.plan->qual;

		Assert(natts > 0);
		bool cols[natts];
		auto found = false;

		memset(cols, 0, sizeof(cols));

		found |= extractcolumns_from_node(rel, (Node *)targetlist, cols, natts);
		found |= extractcolumns_from_node(rel, (Node *)qual, cols, natts);

		/*
		 * GPDB_12_MERGE_FIXME: is this still true? varattno == 0 is checked inside
		 * extractcolumns_from_node.
		 *
		 * In some cases (for example, count(*)), no columns are specified.
		 * We always scan the first column.
		 *
		 * Analyze may also specify no columns which means analyze all
		 * columns.
		 */
		if (!found)
		{
			cols[0] = true;
		}

		auto projs = std::vector<bool>(cols, cols + natts);

		SetProjectedColumns(projs);
	}

	std::vector<::AttrNumber>
	ParquetReader::ExtractOrderKeys(::ScanState *ss)
	{
		std::vector<::AttrNumber> orderkeys;
		TargetEntry *tle;
		SeqScan *scan;
		FuncExpr *funcExpr;

		if (!IsA(&ss->ps, CustomScanState))
			return {};

		auto cs = reinterpret_cast<CustomScan *>(ss->ps.plan);
		auto sort = (Sort *)linitial(cs->custom_plans);

		if (!IsA(sort, Sort))
		{
			// This is possible when it is a unordered scan.
			return {};
		}

		orderkeys.reserve(sort->numCols);
		for (int i = 0; i < sort->numCols; i++)
		{
			tle = get_tle_by_resno(sort->plan.targetlist, sort->sortColIdx[i]);
			Assert(IsA(tle->expr, Var));
			Var *var = (Var *)tle->expr;

			scan = (SeqScan *)sort->plan.lefttree;
			tle = get_tle_by_resno(scan->plan.targetlist, var->varattno);

			if (IsA(tle->expr, Var))
			{

				Assert(IsA(tle->expr, Var));
				orderkeys.push_back(((Var *)tle->expr)->varattno);
			}
			else if (IsA(tle->expr, FuncExpr))
			{
				funcExpr = (FuncExpr *)tle->expr;

				if (strcmp(get_func_name(funcExpr->funcid), "time_bucket") == 0)
				{
					::AttrNumber attr = lsecond_node(Var, funcExpr->args)->varattno;
					orderkeys.push_back(attr);

					::Interval interval = {.time = 0, .day = 0, .month = 0};
					{
						// interval
						auto arg = (Const *)linitial(funcExpr->args);
						if (!IsA(arg, Const))
							continue;
						switch (arg->consttype)
						{
						case INTERVALOID:
							interval = *DatumGetIntervalP(arg->constvalue);
							break;
						case INT2OID:
							interval.time = (DatumGetInt16(arg->constvalue) *
											 USECS_PER_SEC);
							break;
						case INT4OID:
							interval.time = (DatumGetInt32(arg->constvalue) *
											 USECS_PER_SEC);
							break;
						case INT8OID:
							interval.time = (DatumGetInt64(arg->constvalue) *
											 USECS_PER_SEC);
							break;
						default:
							continue;
						}
					}
					SetTimeBucket(attr, interval);
				}
				else
				{
					elog(ERROR, "MARS ExtrctOrderKeys unrecognized function : %s",
						 get_func_name(funcExpr->funcid));
				}
			}
			else
			{
				elog(ERROR, "MARS ExtrctOrderKeys unrecognized node type: %d",
					 (int)nodeTag(tle->expr));
			}
		}

		return orderkeys;
	}

	void
	ParquetReader::SetOrderKeys(::ScanState *ss,
								const std::vector<::AttrNumber> &orderkeys,
								const std::unordered_set<::AttrNumber> &equalkeys)
	{
		/*
		 * Planner rid of ORDER BY column if the column appear in WHERE equality clause.
		 * For the table order column, we need to recognize this optimazation to avoid
		 * data reorder.
		 * refer to `pathkey_is_redundant`.
		 */
		auto &table_order = scanner_.GetTableStoreOrder();
		std::vector<::AttrNumber> full_table_order;
		full_table_order.swap(table_order);

		for (auto &iter : full_table_order)
		{
			bool is_equalkey = equalkeys.find(iter) != equalkeys.end();
			if (!is_equalkey)
			{
				table_order.push_back(iter);
			}
		}

		scanner_.SetOrderKeys(orderkeys);

		/*
		 * To restore original table order here.
		 *
		 * The above code deletes some redundant columns from table_store_order
		 * then uses it as candidate table_scan_order.
		 * It's OK but we should not update table_scan_order itself
		 * because this will cause some sorting metainfo to be lost.
		 *
		 * e.g. `create table :tname (c1 int, c2 int) using mars
		 *  with (group_col_ = "{c1}", global_order_col_= "{c1, c2}")
		 * when we select something and add where condition `c1=1 and c2<2 order by c1, c2`
		 * If we remove c1 from table_store_order and then we will deal the data as if they are sorted by c2.
		 * Obviously, this is not right.
		 *
		 * TODO: this is a temporary fix due to the tight schedule.
		 * We should re-examine this problem and give a better fix.
		 */
		scanner_.GetTableStoreOrder().swap(full_table_order);
	}

	void
	ParquetReader::ExtractScanKeys(
		::ScanState *ss, std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
		std::shared_ptr<std::vector<::IndexArrayKeyInfo>> &arraykeys,
		std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> &runtimekeys)
	{
		::List *qual = ss->ps.plan->qual;

		int ndiscarded_keys;
		BuildScanKeys(ss, qual, scankeys, arraykeys, runtimekeys, ndiscarded_keys);

		can_aggscan_ = ndiscarded_keys > 0;

		if (!arraykeys->empty())
		{
			ExecIndexEvalArrayKeys(ss->ps.ps_ExprContext,
								   arraykeys->data(),
								   arraykeys->size());

			for (auto &arraykey : *(arraykeys.get()))
			{
				/*
				 * the values and nulls are allocated in the per tuple memory
				 * context, we need to duplicate it manually.
				 */
				auto nelems = arraykey.num_elems;
				auto values = arraykey.elem_values;
				auto nulls = arraykey.elem_nulls;
				auto values_size = sizeof(values[0]) * nelems;
				auto nulls_size = sizeof(nulls[0]) * nelems;

				arraykey.elem_values = (::Datum *)palloc(values_size);
				arraykey.elem_nulls = (bool *)palloc(nulls_size);

				memcpy(arraykey.elem_values, values, values_size);
				memcpy(arraykey.elem_nulls, nulls, nulls_size);

				// the arraykey values should be sorted according to the scandir,
				// but we do not know the scandir yet, so we have to do it later.

				pfree(values);
				pfree(nulls);
			}
		}

		// make sure that runtimekeys are evaluated
		if (!runtimekeys->empty())
			::ExecIndexEvalRuntimeKeys(ss->ps.ps_ExprContext,
									   runtimekeys->data(),
									   runtimekeys->size());
	}

	void
	ParquetReader::ExtractScanKeys(::List *qual,
								   std::vector<std::shared_ptr<mars::ScanKey>> &scankeys)
	{
		auto arraykeys = std::make_shared<std::vector<::IndexArrayKeyInfo>>();
		auto runtimekeys = std::make_shared<std::vector<::IndexRuntimeKeyInfo>>();
		int nbadkeys;

		BuildScanKeys(nullptr /* ss */, qual, scankeys, arraykeys, runtimekeys,
					  nbadkeys);
	}

	void
	ParquetReader::BeginScan(::ScanState *ss)
	{
		ExtractColumns(ss);
		auto orderkeys = ExtractOrderKeys(ss);

		std::vector<std::shared_ptr<mars::ScanKey>> scankeys;
		auto arraykeys = std::make_shared<std::vector<::IndexArrayKeyInfo>>();
		auto runtimekeys = std::make_shared<std::vector<::IndexRuntimeKeyInfo>>();
		ExtractScanKeys(ss, scankeys, arraykeys, runtimekeys);

		// collect the "=" keys, they should be ignored when matching the indexes
		std::unordered_set<::AttrNumber> equalkeys;
		equalkeys.reserve(scankeys.size());
		for (const auto &scankey : scankeys)
		{
			if (scankey->IsEqualKey())
				equalkeys.insert(scankey->scankey_.sk_attno);
		}

		auto oid2orders = footer::DecideAuxIndex(rel_, orderkeys,
												 scankeys, equalkeys);
		if (OidIsValid(oid2orders.first))
		{
			scanner_.SetAuxIndexOid(oid2orders.first);
			SetTableStoreOrder(oid2orders.second);
		}

		SetOrderKeys(ss, orderkeys, equalkeys);
		scanner_.SetScanKeys(scankeys,
							 std::move(arraykeys),
							 std::move(runtimekeys));

		mreader_.BeginScan(ParquetReaderImpl::Make(per_table_mcxt_,
												   per_block_mcxt_, scanner_));
	}

	void
	ParquetReader::BeginScan(::List *qual)
	{
		// prefer aux index scan whenever possible, even for seqscan, this is
		// important to trigger the merge scan.
		{
			std::vector<AttrNumber> orderkeys;
			std::vector<std::shared_ptr<mars::ScanKey>> scankeys;
			std::unordered_set<::AttrNumber> equalkeys;
			auto oid2orders = footer::DecideAuxIndex(rel_, orderkeys,
													 scankeys, equalkeys);
			if (OidIsValid(oid2orders.first))
			{
				scanner_.SetAuxIndexOid(oid2orders.first);
				SetTableStoreOrder(oid2orders.second);
			}
		}

		if (qual)
		{
			std::vector<std::shared_ptr<mars::ScanKey>> scankeys;

			ExtractScanKeys(qual, scankeys);
			scanner_.SetScanKeys(scankeys);
		}

		mreader_.BeginScan(ParquetReaderImpl::Make(per_table_mcxt_,
												   per_block_mcxt_, scanner_));
	}

	std::shared_ptr<ParquetReaderImpl>
	ParquetReaderImpl::Make(::MemoryContext per_table_mcxt,
							::MemoryContext per_block_mcxt,
							Scanner &scanner)
	{
		auto auxindexoid = scanner.GetIndexOid();

		if (!OidIsValid(auxindexoid) || scanner.GetIsAnalyze())
		{
			return std::make_shared<ParquetReaderImplLegacy>(per_table_mcxt,
															 per_block_mcxt,
															 scanner);
		}
		else
		{
			return std::make_shared<ParquetReaderImplLazy>(per_table_mcxt,
														   per_block_mcxt,
														   scanner);
		}
	}

	ParquetReaderImplLegacy::ParquetReaderImplLegacy(::MemoryContext per_table_mcxt,
													 ::MemoryContext per_block_mcxt,
													 Scanner &scanner)
		: ParquetReaderImpl(per_table_mcxt, per_block_mcxt, scanner), footers_(), loaded_footers_(false)
	{
	}

	ParquetReaderImplLegacy::~ParquetReaderImplLegacy()
	{
		// must clear the footers before deleting the memory contexts, and note
		// that calling footers_.clear() won't be enough because it does not
		// guarantee the deallocation, so we force that by swapping with a temp
		// empty one.
		footer::Footer::footers_type tmp(std::move(footers_));
		Assert(footers_.empty());
		Assert(footers_.capacity() == 0);
	}

	bool
	ParquetReaderImplLegacy::IsEnd() const
	{
		return blocks_.IsEmpty();
	}

	void
	ParquetReaderImplLegacy::Rewind(::ScanDirection scandir)
	{
		// have to do a clean refetching, because the table scankeys might be
		// different this round.
		footer::FetchFooters(per_table_mcxt_, footers_,
							 scanner_.rel_, scanner_.snapshot_);
		loaded_footers_ = true;

		blocks_.SetRange(scandir, footers_.begin(), footers_.end());
		scanner_.FirstMatchBlock(blocks_);
	}

	bool
	ParquetReaderImplLegacy::SeekBlock(::ScanDirection scandir,
									   ::BlockNumber blockno)
	{
		if (!loaded_footers_)
		{
			Rewind(scandir);
		}

		if (blockno < (::BlockNumber)footers_.size())
		{
			blocks_.SetPosition(footers_.begin() + blockno);
			return true;
		}
		else
		{
			return false;
		}
	}

	bool
	ParquetReaderImplLegacy::NextBlock(::ScanDirection scandir)
	{
		if (blocks_.IsEmpty())
		{
			return false;
		}

		// seek the next block.
		blocks_.Next();
		scanner_.NextMatchBlock(blocks_);

		return !blocks_.IsEmpty();
	}

	const footer::Footer &
	ParquetReaderImplLegacy::GetFooter()
	{
		return blocks_.GetBlock();
	}

	footer::Footer
	ParquetReaderImplLegacy::TransferFooter()
	{
		return blocks_.TransferBlock();
	}

	ParquetReaderImplLazy::ParquetReaderImplLazy(::MemoryContext per_table_mcxt,
												 ::MemoryContext per_block_mcxt,
												 Scanner &scanner)
		: ParquetReaderImpl(per_table_mcxt, per_block_mcxt, scanner), auxscan_(per_table_mcxt, scanner.rel_, scanner.snapshot_)
	{
	}

	bool
	ParquetReaderImplLazy::IsEnd() const
	{
		return auxscan_.IsEnd();
	}

	void
	ParquetReaderImplLazy::Rewind(::ScanDirection scandir)
	{
		// auxscan does not provide a rescan method, even if it does we should do a
		// clean end-and-begin, because the table scankeys might be different this
		// round.
		auxscan_.EndScan();
		auxscan_.BeginScan(scanner_.GetIndexOid(), scanner_.GetTableKeys());

		NextBlock(scandir);
	}

	bool
	ParquetReaderImplLazy::SeekBlock(::ScanDirection scandir,
									 ::BlockNumber blockno)
	{
		if (!auxscan_.Scanning())
			Rewind(scandir);

		if (auxscan_.IsEnd())
			return false;

		::BlockNumber current = auxscan_.GetBatchNumber();
		while (current < blockno &&
			   auxscan_.GetNext(scandir, false /* load */))
		{
			current = auxscan_.GetBatchNumber();
		}

		if (current == blockno)
		{
			auxscan_.LoadFooter();
			return true;
		}
		else
		{
			// no such block, or end of scan
			return false;
		}
	}

	bool
	ParquetReaderImplLazy::NextBlock(::ScanDirection scandir)
	{
		// XXX: we used to filter the blocks immediately with scanner MatchBlock(),
		// however to support the merge scan now we need to postpone this.  The
		// scankeys on groupkeys are handled by the aux index scan already, so
		// what's skipped are only the filters.
		return auxscan_.GetNext(scandir);
	}

	const footer::Footer &
	ParquetReaderImplLazy::GetFooter()
	{
		return auxscan_.GetFooter();
	}

	footer::Footer
	ParquetReaderImplLazy::TransferFooter()
	{
		return auxscan_.TransferFooter();
	}

	ParquetMergeReader::ParquetMergeReader(const Scanner &scanner)
		: context_(scanner), impl_(nullptr), footers_(), group_()
	{
	}

	void
	ParquetMergeReader::BeginScan(std::shared_ptr<ParquetReaderImpl> &&impl)
	{
		impl_ = std::move(impl);
	}

	void
	ParquetMergeReader::EndScan()
	{
		Clear();

		impl_.reset();
	}

	bool
	ParquetMergeReader::IsEnd() const
	{
		return footers_.empty() && impl_->IsEnd();
	}

	void
	ParquetMergeReader::Rewind(::ScanDirection scandir)
	{
		Clear();

		impl_->Rewind(scandir);
		NextBlock(scandir);
	}

	bool
	ParquetMergeReader::SeekBlock(::ScanDirection scandir, ::BlockNumber blockno)
	{
		Clear();

		if (impl_->SeekBlock(scandir, blockno))
		{
			footers_.push_back(impl_->TransferFooter());
			impl_->NextBlock(scandir);

			const auto &footer = footers_.front();
			group_.push_back(&footer);
		}

		return !footers_.empty();
	}

	bool
	ParquetMergeReader::NextBlock(::ScanDirection scandir)
	{
	retry:

		// pop the previous overlapping group
		if (!group_.empty())
		{
			// std::list::size() is slow until c++11, but we are using c++14
			auto n = group_.size();
			group_.clear();
			for (std::size_t i = 0; i < n; ++i)
				footers_.pop_front();
		}

		// get at least 1 footer to detect the overlapping
		if (footers_.empty() && !impl_->IsEnd())
		{
			footers_.push_back(impl_->TransferFooter());
			impl_->NextBlock(scandir);
		}

		if (footers_.empty())
			return false; // end of the scan

		// initialize the overlapping group
		for (const auto &footer : footers_)
		{
			if (group_.empty())
			{
				// it is the first one in the group
			}
			else if (Overlap(scandir, &footer))
			{
				// it overlaps with the group
			}
			else
			{
				goto out; // stop on the first non-overlapping one
			}

			group_.push_back(&footer);
		}

		// the overlapping group is not finished yet, load more
		while (!impl_->IsEnd())
		{
			footers_.push_back(impl_->TransferFooter());
			impl_->NextBlock(scandir);

			const auto &footer = footers_.back();
			if (Overlap(scandir, &footer))
			{
				// extend the overlapping group
				group_.push_back(&footer);
			}
			else
			{
				goto out;
			}
		}

		// end of scan, but the group is non-empty, so it is not the real end yet

	out:
		// reaching here means an entire logical block is got, sort the physical
		// blocks from old to new, this is useful for aggscan to get the up-to-date
		// preagg information.
		//
		// FIXME: should we accumulate the result with a vector so it can be more
		// efficiently sorted?  but then we need to move the footers from the
		// vector to the list, and why we want group_ to be a list is to avoid
		// vector resizing, so using a vector might not always be a win.
		if (group_.size() > 1)
			group_.sort(footer::Footer::CompareBatch);

		// a logical block is found, however note that the NextBlock() is expected
		// to seek the next matching block, not only the groupkeys, but also the
		// filters.  it is not a problem if we don't do the matching, the caller
		// will also run the filters, however it's more efficient to run the
		// filters here.
		{
			const auto footer = group_.back();
			auto logical = footer->HasLogicalInfo();
			auto match = context_.scanner_.MatchBlock(*footer, logical);
			if (match == ScanKey::MatchType::BLOCK_MISMATCH)
				goto retry;
		}

		return true;
	}

	const std::list<const footer::Footer *> &
	ParquetMergeReader::GetFooters() const
	{
		return group_;
	}

	void
	ParquetMergeReader::Clear()
	{
		footers_.clear();
		group_.clear();
	}

	bool
	ParquetMergeReader::Overlap(::ScanDirection scandir,
								const footer::Footer *follow) const
	{
		Assert(!footers_.empty());

		if (context_.mergekeys_.empty())
			return false;

		return context_.Overlap(group_.front(), follow);
	}

	ParquetMergeReader::Context::Context(const Scanner &scanner)
		: scanner_(scanner), mergekeys_()
	{
		// we use the global orderkeys or the groupkeys as the overlapkeys, the
		// scanner should ensure that all the keys are projected, this is important
		// to produce the correct merged output.
		const auto &projcols = scanner_.GetProjectedColumns();
		std::unordered_set<int> cols(projcols.begin(), projcols.end());

		// use the groupkeys as the mergekeys
		const auto &groupkeys = scanner_.GetGroupKeys();
		mergekeys_ = groupkeys;
	}

	bool
	ParquetMergeReader::Context::Overlap(const footer::Footer *a,
										 const footer::Footer *b) const
	{
		Assert(!mergekeys_.empty());

		for (auto mergekey : mergekeys_)
		{
			auto column = AttrNumberGetAttrOffset(mergekey.attnum);

			// the groupkeys must be NOT NULL, and empty blocks are never stored.
			Assert(a->GetRowCount() > 0);
			Assert(b->GetRowCount() > 0);

			auto a_datum = a->GetMinDatum(column);
			auto b_datum = b->GetMinDatum(column);

			if (mergekey.dimensiontype == MARS_DIM_ATTR_TYPE_WITH_PARAM)
			{
				// param group keys must be byval
				auto param = DatumGetInt64(mergekey.param);
				auto a_value = DatumGetInt64(a_datum);
				auto b_value = DatumGetInt64(b_datum);
				a_value = a_value - a_value % param;
				b_value = b_value - b_value % param;
				if (a_value != b_value)
					return false;
			}
			else
			{
				const auto &comp = scanner_.GetColumnComparator(column);
				if (!comp.equal(a_datum, b_datum))
					return false;
			}
		}

		// all equal
		return true;
	}

} // namespace mars
