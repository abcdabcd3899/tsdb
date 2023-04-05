/*-------------------------------------------------------------------------
 *
 * ParquetWriter.cc
 *	  MARS data writer.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/ParquetWriter.cc
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"

#include "type_traits.hpp"

#include "BlockReader.h"
#include "FileManager.h"
#include "Footer.h"
#include "ParquetWriter.h"
#include "InsertOp.h"
#include "MarsFileSerializer.h"
#include "ts_func.h"
#include "coder/Coder.h"
#include "guc.h"
#include "utils.h"

#include <iostream>
#include <fstream>
#include <list>

#include <arrow/status.h>
#include <arrow/util/bitmap_builders.h>
#include <arrow/util/io_util.h>
#include <parquet/api/writer.h>
#include <parquet/types.h>

namespace mars
{

	parquet::schema::NodePtr
	ParquetWriter::MakeParquetNode(coder::Version version,
								   const ::Form_pg_attribute attr)
	{
		auto name = NameStr(attr->attname);
		auto type_oid = attr->atttypid;
		auto byval = attr->attbyval;

		if (version != coder::Version::V0_1)
		{
			/*
			 * in block format v1.0+ the values are always encoded, by a real
			 * encoder or a no-op one, depends on the column type.
			 *
			 * the encoded data are always stored as parquet bytes, the statistics
			 * are collected by ourself, so turn off the parquet statistics by
			 * passing byval=false.
			 */
			type_oid = BYTEAOID;
			byval = false;
		}

		parquet::Type::type parquet_type;
		parquet::ConvertedType::type converted_type;

		switch (type_oid)
		{
		case INT4OID:
			parquet_type = pg_type_traits<INT4OID>::parquet_type;
			converted_type = pg_type_traits<INT4OID>::converted_type;
			break;
		case INT8OID:
			parquet_type = pg_type_traits<INT8OID>::parquet_type;
			converted_type = pg_type_traits<INT8OID>::converted_type;
			break;
		case FLOAT4OID:
			parquet_type = pg_type_traits<FLOAT4OID>::parquet_type;
			converted_type = pg_type_traits<FLOAT4OID>::converted_type;
			break;
		case FLOAT8OID:
			parquet_type = pg_type_traits<FLOAT8OID>::parquet_type;
			converted_type = pg_type_traits<FLOAT8OID>::converted_type;
			break;
		case TIMESTAMPTZOID:
			parquet_type = pg_type_traits<TIMESTAMPTZOID>::parquet_type;
			converted_type = pg_type_traits<TIMESTAMPTZOID>::converted_type;
			break;
		case TIMESTAMPOID:
			parquet_type = pg_type_traits<TIMESTAMPOID>::parquet_type;
			converted_type = pg_type_traits<TIMESTAMPOID>::converted_type;
			break;
		default:
			if (!byval)
			{
				// all variable-length types are treated as text
				parquet_type = pg_type_traits<TEXTOID>::parquet_type;
				converted_type = pg_type_traits<TEXTOID>::converted_type;
				break;
			}

			return nullptr;
		}

		return parquet::schema::PrimitiveNode::Make(name,
													parquet::Repetition::OPTIONAL,
													parquet_type,
													converted_type);
	}

	ParquetWriter::ParquetWriter(::Relation rel, ::Snapshot snapshot,
								 ::Oid footerOid, std::vector<::AttrNumber> &mergeby)
		: footerOid_(footerOid), fileWriter_(nullptr), version_(coder::default_version), groupkey_store_order_(utils::DecideGroupKeyStoreOrder(RelationGetRelid(rel))), block_store_order_(utils::DecideLocalStoreOrder(RelationGetRelid(rel))), block_merge_attr_(), scanner_(rel, snapshot), merger_(scanner_), current_batch_(InvalidBlockNumber), auxfetch_(ExclusiveLock), sorter_(block_store_order_), pending_(), pending_remap_(), pending_sorter_(groupkey_store_order_)
	{
		per_block_mcxt_ = AllocSetContextCreate(CurrentMemoryContext,
												"mars writer block context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

		auto relid = RelationGetRelid(scanner_.rel_);

		mars::utils::GetRelOptions(relid, &groupkeys_, &global_orderkeys_, nullptr);

		// detect the aux version
		auto auxrel = ::relation_open(footerOid, RowExclusiveLock);
		auto auxtupdesc = RelationGetDescr(auxrel);
		auxversion_ = footer::footer_version_from_tupdesc(auxtupdesc);
		::relation_close(auxrel, RowExclusiveLock);

		// the scanner of a writer is used to load conflict blocks, all the columns
		// must be projected.
		scanner_.SetProjectedColumns();

		can_merge_ = auxfetch_.OpenIndexForFullGroupKeys(relid, snapshot);
		block_merge_attr_ = auxfetch_.ReduceMergeKey(mergeby);

		// init the pending cache
		{
			const auto tupdesc = RelationGetDescr(scanner_.rel_);
			auto &cstores = pending_;
			auto &comps = scanner_.GetRelationComparator();

			cstores.resize(tupdesc->natts);
			for (size_t i = 0; i < cstores.size(); ++i)
				cstores[i] = ColumnStore::Make(comps, i);
		}
	};

	ParquetWriter::~ParquetWriter()
	{
		auxfetch_.CloseIndex();

		opVec_.clear();
		groupkeys_.clear();
		global_orderkeys_.clear();

		out_file_.reset();
		schema_.reset();
		properties_.reset();
		fileWriter_.release();

		MemoryContextDelete(per_block_mcxt_);
	}

	arrow::Status
	ParquetWriter::Flush()
	{
		// only switch to the per-block memory context inside Flush(), most of the
		// palloc() happen here, such as the aux relation insertion, and the column
		// encoding.
		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		// TODO: the reordered data are accessed multiple times, in such a case
		// maybe clustering them can be more efficient because it's more cpu cache
		// friendly.
		Reorder();
		// the data should not be pushed to the ops before this is done
		MergeOverlapped();

		auto &cstores = scanner_.GetColumnStores();
		auto &remap = scanner_.GetReorderMap();
		auto ncols = cstores.size();
		auto nrows = cstores.front()->Count();

		if (nrows > 0)
		{
			// flush the data to the ops
			for (std::size_t i = 0; i < ncols; ++i)
			{
				auto &op = opVec_[i];
				const auto &cstore = *cstores[i];
				op->Flush(cstore.Values(), cstore.Nulls(), remap);
				op->Reset();
			}

			fileWriter_->Close();

			footer::RowGroupPreAgg block_preagg(ncols, nrows);

			// collect preagg for the current physical block
			{
				for (std::size_t column = 0; column < ncols; ++column)
				{
					auto &preagg = preaggs_[column];
					preagg.Reset();

					const auto &cstore = *cstores[column];
					const auto values = cstore.Values().data();
					const auto &isnull = cstore.Nulls();

					for (int row = 0; row < nrows; ++row)
						preagg.Append(values[row], isnull[row]);

					block_preagg.Push(preagg);
				}
			}

			// need a fake slot to fetch the logical block, and retrive the rows
			// from the merger.
			std::vector<::Datum> tts_values(ncols);
			std::vector<uint8_t> tts_isnull(ncols);

			// g++-4.8 requires the const member, tts_ops, to be initialized
			::TupleTableSlot slot{
				T_Invalid /* type */,
				0 /* tts_flags */,
				0 /* tts_nvalid */,
				nullptr /* tts_ops */,
				RelationGetDescr(scanner_.rel_) /* tts_tupleDescriptor */,
			};
			slot.tts_values = tts_values.data();
			slot.tts_isnull = (bool *)tts_isnull.data();

			// collect preagg for the entire logical block
			{
				// build a row to represent the current block
				for (const auto &groupkey : groupkeys_)
				{
					auto column = AttrNumberGetAttrOffset(groupkey.attnum);
					const auto &cstore = *cstores[column];
					slot.tts_values[column] = cstore.Values()[remap.front()];
					slot.tts_isnull[column] = cstore.Nulls()[remap.front()];
				}

				// the preagg information must be collected on the full logical
				// block, including the currently block, and we pass MaxBlockNumber
				// as the block number to force the current block be considered as
				// the latest.
				merger_.Begin();
				merger_.PushBlock(cstores, remap, MaxBlockNumber);

				// this must be done after the current block, the remap and cstores
				// are modified during LoadExistingBlock(), so the they must be
				// pushed first.
				LoadExistingBlock(&slot);

				if (!footers_.empty())
				{
					// found overlapping blocks, collect the logical preagg

					for (auto &preagg : preaggs_)
						preagg.Reset();

					while (merger_.PopMergedRow(&slot))
					{
						for (unsigned int i = 0; i < ncols; i++)
						{
							auto &preagg = preaggs_[i];
							preagg.Append(slot.tts_values[i], slot.tts_isnull[i]);
						}
					}

					// combine the column preaggs to block preagg
					for (const auto &preagg : preaggs_)
						block_preagg.Push(preagg);

					// the logical row count is separately specified
					block_preagg.SetLogicalRowCount(preaggs_[0].total_count_);

					// this must be done before resetting the per-block memory
					// context.
					footers_.clear();
				}

				// anyway, the merger must be clearer before the per-block memory
				// context gets reset.
				merger_.End();
			}

			// must create the footer inside a "{}" block, so it is destructed
			// before resetting the per-block memory context.
			{
				auto relid = RelationGetRelid(scanner_.rel_);

				// must create the footer in the actual aux version
				mars::footer::Footer footer{auxversion_,
											CurrentMemoryContext,
											RelationGetDescr(scanner_.rel_),
											relid, footerOid_};

				auto footer_content = fileWriter_->metadata()->SerializeToString();

				footer.StoreFooterToAux(footer_content, block_preagg,
										groupkeys_, global_orderkeys_);
			}

			footer::SeeLocalChanges();

			fileWriter_ = nullptr;
		}

		current_batch_ = InvalidBlockNumber;

		// it is possible that the oldcxt is also the per-block one, the caller
		// should ensure that objects allocated before the Flush() are never
		// used after that.
		::MemoryContextSwitchTo(oldcxt);
		::MemoryContextReset(per_block_mcxt_);

		return arrow::Status::OK();
	}

	ParquetWriter *
	ParquetWriter::Open(::Relation rel, ::Snapshot snapshot)
	{
		// fetch relation info
		auto tupdesc = RelationGetDescr(rel);
		::Oid footerOid = ::mars::footer::RelationGetFooterRelid(rel);

		auto mergeby = ::mars::utils::ProcessInferenceElems(mars_infer_elems);
		ParquetWriter *pWriter = new ParquetWriter(rel, snapshot, footerOid, mergeby);

		// column meta info
		std::vector<CreationProp> types(RelationGetNumberOfAttributes(rel));

		StdRdOptions **info = ::RelationGetAttributeOptions(rel);
		for (int i = 0; i < RelationGetNumberOfAttributes(rel); ++i)
		{
			auto compression = coder::Encoding::NONE;

			if (::strcmp(info[i]->compresstype, "lz4") == 0)
			{
				compression = coder::Encoding::LZ4;
			}
			else if (::strcmp(info[i]->compresstype, "zstd") == 0)
			{
				compression = coder::Encoding::ZSTD;
			}

			types[i] = std::make_tuple(&tupdesc->attrs[i], compression);

			::pfree(info[i]);
		}
		::pfree(info);

		Status s = pWriter->Init(rel->rd_node, types, RelationNeedsWAL(rel));

		if (!s.ok())
		{
			delete pWriter;
			pWriter = nullptr;
			elog(ERROR, "mars open file error: %s", s.message().data());
		}

		return pWriter;
	}

	arrow::Status
	ParquetWriter::Init(const ::RelFileNode &rd_node, std::vector<CreationProp> &typeVec, bool needWAL)
	{
		auto ncols = typeVec.size();
		fields_.reserve(ncols);
		opVec_.reserve(ncols);
		preaggs_.reserve(ncols);

		for (std::size_t i = 0; i < ncols; ++i)
		{
			auto attr = std::get<colAttr>(typeVec[i]);
			auto compression = std::get<colCompression>(typeVec[i]);
			auto field = MakeParquetNode(version_, attr);
			auto op = InsertOp::Make(version_, attr, mars_enable_column_encode, compression);

			if (op == nullptr || field == nullptr)
			{
				return arrow::Status(arrow::StatusCode::TypeError,
									 "Type " + std::to_string(attr->atttypid) + " unsupport.");
			}

			fields_.push_back(field);
			opVec_.push_back(std::move(op));
			preaggs_.emplace_back(attr);
		}

		ARROW_ASSIGN_OR_RAISE(out_file_, MarsFile::Open(rd_node, true, needWAL));

		schema_ = std::static_pointer_cast<GroupNode>(
			GroupNode::Make("schema", parquet::Repetition::OPTIONAL, fields_));

		// prepare contents
		auto builder = parquet::WriterProperties::Builder();
		builder.version(parquet::ParquetVersion::PARQUET_2_0);
		// global disable dictionary and enable statistics information for all columns
		builder.disable_dictionary();

		// By our PreAgg, instead of parquet_statistics.
		builder.disable_statistics();

		for (const auto &iter : typeVec)
		{
			auto attr = std::get<colAttr>(iter);
			auto name = NameStr(attr->attname);

			// turn off statistics for varlen types, e.g. text, json...
			if (version_ != coder::Version::V0_1 || !attr->attbyval)
			{
				builder.disable_statistics(name);
			}
		}

		// record the file format
		builder.created_by(coder::version_to_string(version_));

		properties_ = builder.build();

		return arrow::Status::OK();
	}

	arrow::Status
	ParquetWriter::AppendTuple(const ::TupleTableSlot *slot)
	{
		if (!BelongCurrentBlock(slot))
		{
			ARROW_RETURN_NOT_OK(Flush());
		}

		if (fileWriter_ == nullptr)
		{
			RowGroupCreate();
		}

		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		auto &cstores = scanner_.GetColumnStores();

		if (OverlapLastRow(slot))
		{
			// merge with the last row
			for (int i = 0; i < slot->tts_nvalid; ++i)
				cstores[i]->UpsertLastRow(slot->tts_values[i], slot->tts_isnull[i]);
		}
		else
		{
			// in tuple merge mode rows are pushed to the columns stores
			for (int i = 0; i < slot->tts_nvalid; ++i)
				cstores[i]->Append(slot->tts_values[i], slot->tts_isnull[i]);
		}

		::MemoryContextSwitchTo(oldcxt);

		return arrow::Status::OK();
	}

	arrow::Status
	ParquetWriter::UpdateTuple(const ::ItemPointer otid,
							   const ::TupleTableSlot *slot, ::CommandId cid,
							   ::Snapshot snapshot, ::Snapshot crosscheck)
	{
		// we cannot assume that the new row can still stay in the current block,
		// so we have to do a delete+insert instead of a in-place update, the new
		// row can still be put in the block if it should.
		//
		// however it's more complicated when updating multile rows, suppose we
		// are replacing 2 old rows in the current block and the 2 new rows are
		// all out of the current block, then we should not perform them in below
		// order:
		//
		// - delete old1 from block A
		// - insert new1 to   block B
		// - delete old2 from block A
		// - insert new2 to   block B
		//
		// this is inefficient.
		//
		// so to make things easier, we restrict that updating an groupkey is not
		// allowed, to be more accurate, we allow an update as long as the new row
		// stays in the same block with the old row.
		if (!BelongCurrentBlock(otid))
		{
			ARROW_RETURN_NOT_OK(Flush());
		}

		if (fileWriter_ == nullptr)
		{
			RowGroupCreate();

			if (!LoadBlock(otid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("mars: cannot find tuple (%u,%hu) in relation \"%s\"",
								ItemPointerGetBlockNumber(otid),
								ItemPointerGetOffsetNumber(otid),
								RelationGetRelationName(scanner_.rel_))));
			}
		}

		if (!groupkeys_.empty() && !BelongCurrentBlock(slot))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mars: cannot update on the groupkeys")));
		}

		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);

		auto &cstores = scanner_.GetColumnStores();
		for (int i = 0; i < slot->tts_nvalid; ++i)
		{
			cstores[i]->Update(otid, slot);
		}

		::MemoryContextSwitchTo(oldcxt);

		return arrow::Status::OK();
	}

	void
	ParquetWriter::RowGroupCreate()
	{
		auto contents = MarsFileSerializer::Open(out_file_, schema_, properties_, nullptr);

		// create writer
		fileWriter_.reset(new parquet::ParquetFileWriter());

		// open content by writer
		fileWriter_->Open(std::move(contents));

		auto row_group = fileWriter_->AppendBufferedRowGroup();
		auto &cstores = scanner_.GetColumnStores();

		for (size_t i = 0; i < opVec_.size(); i++)
		{
			auto c_writer = row_group->column(i);
			opVec_[i]->RegisterWriter(c_writer);
			cstores[i]->Clear();
		}
	}

	bool
	ParquetWriter::OverlapLastRow(const ::TupleTableSlot *slot)
	{
		const auto &cstores = scanner_.GetColumnStores();
		Assert(!cstores.empty());

		std::size_t nrows = cstores[0]->Count();
		if (nrows == 0)
			return false;

		return OverlapRow(slot, nrows - 1);
	}

	bool
	ParquetWriter::OverlapRow(const ::TupleTableSlot *slot, int row)
	{
		if (groupkeys_.empty())
			return false;

		const auto &cstores = scanner_.GetColumnStores();
		Assert(!cstores.empty());
		Assert(row < cstores[0]->Count());

		for (const auto &groupkey : groupkeys_)
		{
			int column = AttrNumberGetAttrOffset(groupkey.attnum);
			const auto &cstore = cstores[column];
			const auto &comp = scanner_.GetColumnComparator(column);

			// all groupkeys should be NOT NULL
			Assert(!cstore->Nulls().at(row));
			Assert(!slot->tts_isnull[column]);

			auto current_value = cstore->Values().at(row);
			auto new_value = slot->tts_values[column];
			if (!comp.equal(current_value, new_value))
			{
				return false;
			}
		}

		return true;
	}

	bool
	ParquetWriter::BelongCurrentBlock(const ::TupleTableSlot *slot)
	{
		const auto &cstores = scanner_.GetColumnStores();
		Assert(!cstores.empty());
		std::size_t nrows = cstores[0]->Count();

		if (groupkeys_.empty())
		{
			return nrows < mars::MAXIMUM_ROW_COUNT;
		}
		else if (nrows > 0)
		{
			for (const auto &groupkey : groupkeys_)
			{
				int column = AttrNumberGetAttrOffset(groupkey.attnum);
				const auto &cstore = cstores[column];
				const auto &comp = scanner_.GetColumnComparator(column);

				// all groupkeys should be NOT NULL
				Assert(!cstore->Nulls().at(0));
				Assert(!slot->tts_isnull[column]);

				auto current_value = cstore->Values().at(0);
				auto new_value = slot->tts_values[column];
				switch (groupkey.dimensiontype)
				{
				case MARS_DIM_ATTR_TYPE_WITH_PARAM:
					// param groupkeys must be dividable by the interval, so we
					// could align them to the time bucket simply by clearing
					// the modulos.
					current_value -= current_value % groupkey.param;
					new_value -= new_value % groupkey.param;
					// fall-through

				case MARS_DIM_ATTR_TYPE_DEFAULT:
				default:
					if (!comp.equal(current_value, new_value))
					{
						return false;
					}
					break;
				}
			}
		}

		return true;
	}

	bool
	ParquetWriter::BelongCurrentBlock(const ::ItemPointer tid)
	{
		auto batch = ItemPointerGetBlockNumber(tid);
		Assert(BlockNumberIsValid(batch));

		return (!BlockNumberIsValid(current_batch_) ||
				current_batch_ == batch);
	}

	void
	ParquetWriter::LoadExistingBlock(const ::TupleTableSlot *slot)
	{
		footers_.clear();

		if (!can_merge_)
		{
			return;
		}

		auto oldcxt = ::MemoryContextSwitchTo(per_block_mcxt_);
		auxfetch_.BeginFetch(slot);

		current_batch_ = InvalidBlockNumber;

		while (auxfetch_.FetchNext())
		{
			// found a match, load the footer
			footers_.emplace_back(auxversion_,
								  per_block_mcxt_,
								  RelationGetDescr(scanner_.rel_),
								  nullptr,
								  nullptr);

			auto &footer = footers_.back();
			auxfetch_.GetFooter(footer);

			if (!BlockNumberIsValid(current_batch_))
				current_batch_ = footer.impl_->batch_;

			merger_.PushBlock(footer, true /* overlapped */);
		}

		auxfetch_.EndFetch();
		::MemoryContextSwitchTo(oldcxt);
	}

	bool
	ParquetWriter::LoadBlock(const ::ItemPointer tid)
	{
		footer::Footer footer(auxversion_, per_block_mcxt_,
							  RelationGetDescr(scanner_.rel_), nullptr, nullptr);
		if (!footer::FetchFooter(per_block_mcxt_, footer,
								 scanner_.rel_, scanner_.snapshot_, tid))
			// no matching block
			return false;

		auto block_reader = BlockReader::Get(scanner_, footer);
		if (!block_reader->ReadBatch(false /* overlapped */))
			// empty block
			return false;

		current_batch_ = footer.impl_->batch_;
		return true;
	}

	void
	ParquetWriter::Reorder()
	{
		// the old and new data are all in the column stores
		const auto &cstores = scanner_.GetColumnStores();
#ifdef USE_ASSERT_CHECKING
		auto ncols = cstores.size();
		Assert(ncols > 0);
#endif // USE_ASSERT_CHECKING

		// build a sorter
		sorter_.BindData(&cstores, &scanner_.GetReorderMap());
		sorter_.ResetReorderMap();

		if (!sorter_.IsSorted())
		{
			sorter_.Sort();
		}
	}

	void
	ParquetWriter::MergeOverlapped()
	{
		if (groupkeys_.empty())
			return;

		const auto &cstores = scanner_.GetColumnStores();
		auto remap = scanner_.GetReorderMap().data();
		auto nrows = scanner_.GetReorderMap().size();
		auto ncols = cstores.size();
		Assert(ncols > 0);

		if (nrows <= 1)
			return;

		// pass 1: scan for any mergeable rows, this happens only when the input
		// data is not fully sorted, this is not the recommended way to load into
		// mars, so it's likely that there is no such row.
		std::list<int> deleted;
		for (std::size_t nth = nrows - 1; nth > 0; --nth)
		{
			auto row1 = remap[nth];
			auto row0 = remap[nth - 1];
			bool overlap = true;

			for (const auto &groupkey : groupkeys_)
			{
				int column = AttrNumberGetAttrOffset(groupkey.attnum);
				const auto &cstore = *cstores[column];
				const auto &comp = scanner_.GetColumnComparator(column);

				Assert(!cstore.Nulls().at(row0));
				Assert(!cstore.Nulls().at(row1));

				auto value0 = cstore.Values().at(row0);
				auto value1 = cstore.Values().at(row1);
				if (!comp.equal(value0, value1))
				{
					overlap = false;
					break;
				}
			}

			if (overlap)
			{
				// merge [row] into [row-1].
				for (std::size_t col = 0; col < ncols; ++col)
				{
					const auto &cstore = cstores[col];
					const auto &values = cstore->Values();
					const auto &isnull = cstore->Nulls();
					cstore->Upsert(row0, values[row1], isnull[row1]);
				}

				// mark [row] as deleted.
				deleted.push_front(row1);
			}
		}

		if (deleted.empty())
			// lucky, no mergeable row, so no row to delete.
			return;

		// pass 2: actually delete the rows.
		for (std::size_t col = 0; col < ncols; ++col)
		{
			const auto &cstore = cstores[col];
			cstore->DeleteRows(deleted);
		}
	}

} // namespace mars
