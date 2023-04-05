/*-------------------------------------------------------------------------
 *
 * tools.cc
 *	  The MARS debug tools.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/tools.cc
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"
#include "Footer.h"
#include "tools.h"

extern "C"
{

	/*!
	 * Extract columns from targetlists or quals.
	 * Copied from aocsam_handler.c.
	 */
	struct ExtractcolumnContext
	{
		Relation rel;
		bool *cols;
		AttrNumber natts;
		bool found;
	};

	/*!
	 * Copied from aocsam_handler.c.
	 */
	static bool
	extractcolumns_walker(Node *node, struct ExtractcolumnContext *ecCtx)
	{
		if (node == NULL)
			return false;

		if (IsA(node, Var))
		{
			Var *var = (Var *)node;

			if (IS_SPECIAL_VARNO(var->varno))
				return false;

			if (var->varattno > 0 && var->varattno <= ecCtx->natts)
			{
				ecCtx->cols[var->varattno - 1] = true;
				ecCtx->found = true;
			}
			/*
			 * If all attributes are included,
			 * set all entries in mask to true.
			 */
			else if (var->varattno == 0)
			{
				for (AttrNumber attno = 0; attno < ecCtx->natts; attno++)
					ecCtx->cols[attno] = true;
				ecCtx->found = true;

				return true;
			}

			return false;
		}
		else if (IsA(node, String))
		{
			/* Case for analyze t1 (c1, c2) */
			AttrNumber attno;
			char *col;

			col = strVal(node);
			attno = attnameAttNum(ecCtx->rel, col, false);
			Assert(attno > InvalidAttrNumber);

			ecCtx->cols[attno - 1] = true;
			ecCtx->found = true;

			return false;
		}

		return expression_tree_walker(node,
									  (bool (*)())extractcolumns_walker,
									  (void *)ecCtx);
	}

	/*!
	 * Copied from aocsam_handler.c.
	 */
	bool
	extractcolumns_from_node(Relation rel, Node *expr, bool *cols, AttrNumber natts)
	{
		struct ExtractcolumnContext ecCtx;

		ecCtx.rel = rel;
		ecCtx.cols = cols;
		ecCtx.natts = natts;
		ecCtx.found = false;

		extractcolumns_walker(expr, &ecCtx);

		return ecCtx.found;
	}

	/*
	 * Traverse an expression tree if it contains a single
	 * value filter.
	 */
	bool
	column_walker(Node *node, ColumnWalkerContext *cxt)
	{
		if (node == NULL)
			return false;

		if (IsA(node, OpExpr))
		{
			Var *v = NULL;
			OpExpr *opExpr = (OpExpr *)node;
			Node *arg1 = (Node *)linitial(opExpr->args);
			Node *arg2 = (Node *)llast(opExpr->args);
			AttrNumber attr = cxt->attrNumber;

			if (IsA(arg1, Var) && IsA(arg2, Const))
			{
				v = (Var *)arg1;
			}
			else if (IsA(arg2, Var) && IsA(arg1, Const))
			{
				v = (Var *)arg2;
			}

			if (v)
			{
				if (v->varattno == attr)
				{
					char *oprname = get_opname(opExpr->opno);

					if (pg_strcasecmp(oprname, "=") == 0)
						return true;
				}
			}
		}
		else if (IsA(node, BoolExpr))
		{
			BoolExpr *boolExpr = (BoolExpr *)node;

			if (boolExpr->boolop != AND_EXPR)
				/* OR_EXPR, NOT_EXPR are never considered as single-value */
				return false;
		}

		return expression_tree_walker(node,
									  (bool (*)())column_walker,
									  (void *)cxt);
	}

} // extern "C"

namespace mars
{

	namespace tools
	{

		Datum
		mars_preagg_sum(PG_FUNCTION_ARGS)
		{
			struct ctx
			{
				ctx(int iter, int colnum,
					const std::shared_ptr<footer::Footer::footers_type> &footers)
					: iter_(iter), colnum_(colnum), footers_(std::move(footers)), typeOidArray(new ::Oid[colnum_]), totalRow_(colnum_ * footers_->size())
				{
				}

				~ctx() = default;

				int iter_;
				int colnum_;
				const std::shared_ptr<footer::Footer::footers_type> footers_;
				std::unique_ptr<Oid[]> typeOidArray;
				int totalRow_;
			};

			FuncCallContext *funcctx;
			MemoryContext oldcontext;
			if (SRF_IS_FIRSTCALL())
			{
				AttrNumber attrnum = 4;
				TupleDesc desc;
				int attno;
				int colnum;
				Oid relOid = PG_GETARG_OID(0);
				Relation rel;
				funcctx = SRF_FIRSTCALL_INIT();

				rel = heap_open(relOid, AccessShareLock);
				colnum = RelationGetNumberOfAttributes(rel);
				auto footers = std::make_shared<footer::Footer::footers_type>();
				auto snapshot = ::GetTransactionSnapshot();
				footer::FetchFooters(funcctx->multi_call_memory_ctx, *footers,
									 rel, snapshot);

				oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
				desc = CreateTemplateTupleDesc(attrnum);
				attno = 1;
				TupleDescInitEntry(desc, attno++, "segid", INT4OID, -1, 0);
				TupleDescInitEntry(desc, attno++, "batch", INT4OID, -1, 0);
				TupleDescInitEntry(desc, attno++, "colid", INT4OID, -1, 0);
				TupleDescInitEntry(desc, attno++, "sum", TEXTOID, -1, 0);
				funcctx->tuple_desc = BlessTupleDesc(desc);
				auto pctx = new ctx{0, colnum, std::move(footers)};
				funcctx->user_fctx = pctx;
				for (int i = 0; i < colnum; i++)
				{
					pctx->typeOidArray[i] = rel->rd_att->attrs[i].atttypid;
				}
				heap_close(rel, AccessShareLock);
				MemoryContextSwitchTo(oldcontext);
			}

			funcctx = SRF_PERCALL_SETUP();
			struct ctx *pctx = static_cast<struct ctx *>(funcctx->user_fctx);

			Datum result;
			Datum values[4];
			bool nulls[4];
			if (pctx->iter_ < pctx->totalRow_)
			{

				int footer_num = pctx->iter_ / pctx->colnum_;
				int col_num = pctx->iter_ % pctx->colnum_;

				values[0] = Int32GetDatum(pctx->footers_->at(footer_num).impl_->segno_);
				nulls[0] = false;
				values[1] = Int32GetDatum(pctx->footers_->at(footer_num).impl_->batch_);
				nulls[1] = false;
				values[2] = Int32GetDatum(col_num);
				nulls[2] = false;
				auto &footer = pctx->footers_->at(footer_num);

				std::string sum_str;
				switch (pctx->typeOidArray[col_num])
				{
				case INT4OID:
				case INT8OID:
				{
					__int128 int_sum = footer.FooterGetSum<parquet::Type::INT32>(col_num);
					sum_str = to_string(int_sum);
					nulls[3] = false;
					break;
				}
				case FLOAT4OID:
				case FLOAT8OID:
				{
					long double float_sum = footer.FooterGetSum<parquet::Type::FLOAT>(col_num);
					sum_str = to_string(float_sum);
					nulls[3] = false;
					break;
				}
				default:
					nulls[3] = true;
					break;
				}

				if (!nulls[3])
				{
					::text *txt = (::text *)::palloc(VARHDRSZ + sum_str.size());
					SET_VARSIZE(txt, VARHDRSZ + sum_str.size());
					::memcpy(VARDATA(txt), sum_str.data(), sum_str.size());
					values[3] = PointerGetDatum(txt);
				}

				auto datum = ::heap_form_tuple(funcctx->tuple_desc, values, nulls);
				result = HeapTupleGetDatum(datum);
				++pctx->iter_;

				SRF_RETURN_NEXT(funcctx, result);
			}

			delete pctx;

			SRF_RETURN_DONE(funcctx);
		}

	} // namespace tools

} // namespace mars
