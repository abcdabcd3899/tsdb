/*-------------------------------------------------------------------------
 *
 * ParquetWriter.h
 *	  MARS data writer.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/ParquetWriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_PARQUETWRITER_H
#define MATRIXDB_PARQUETWRITER_H

#include "wrapper.hpp"

#include "BlockMerger.h"
#include "FileManager.h"
#include "Footer.h"
#include "InsertOp.h"
#include "MarsFileSerializer.h"
#include "MarsFile.h"
#include "Scanner.h"

#include <unordered_map>
#include <set>
#include <memory>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/api/writer.h>
#include <parquet/properties.h>

namespace mars
{

	using arrow::ArrayBuilder;
	using arrow::DataType;
	using arrow::Field;
	using arrow::Status;
	using parquet::ParquetFileWriter;
	using parquet::WriterProperties;
	using parquet::schema::GroupNode;
	using std::shared_ptr;
	using std::size_t;
	using std::string;
	using std::unique_ptr;
	using std::vector;

	class ParquetWriter
	{
	public:
		virtual ~ParquetWriter();

		ParquetWriter(const ParquetWriter &rhs) = delete;
		ParquetWriter(ParquetWriter &&rhs) = delete;
		ParquetWriter &operator=(ParquetWriter &&rhs) = delete;
		ParquetWriter &operator=(const ParquetWriter &rhs) = delete;

	protected:
		arrow::Status Flush();

		arrow::Status AppendTuple(const ::TupleTableSlot *slot);
		arrow::Status UpdateTuple(const ::ItemPointer otid,
								  const ::TupleTableSlot *slot, ::CommandId cid,
								  ::Snapshot snapshot, ::Snapshot crosscheck);

	public:
		static ParquetWriter *Open(::Relation rel, ::Snapshot snapshot);

		arrow::Status BufferedAppend(const ::TupleTableSlot *slot)
		{
			for (int i = 0; i < slot->tts_nvalid; ++i)
				pending_[i]->Append(slot->tts_values[i], slot->tts_isnull[i]);

			// force flush the cache if there are enough amount of data
			if (pending_.front()->Count() >= (int)mars::MAXIMUM_WRITE_BUFFER_ROW_COUNT)
				return FlushBuffer();

			return arrow::Status::OK();
		}

		arrow::Status FlushBuffer()
		{
			const auto &cstores = pending_;
			auto &remap = pending_remap_;
			auto ncols = cstores.size();

			if (pending_.front()->Count() == 0)
				return arrow::Status::OK(); // no pending data

			// first sort the pending data
			{
				// build a sorter
				pending_sorter_.BindData(&cstores, &remap);
				pending_sorter_.ResetReorderMap();

				if (!pending_sorter_.IsSorted())
					pending_sorter_.Sort();
			}

			// then actually append them to the writer
			{
				// need a fake slot to append.
				//
				// TODO: refactor the api to take column stores directly
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
				slot.tts_nvalid = ncols;

				for (auto row : remap)
				{
					for (int i = 0; i < slot.tts_nvalid; ++i)
					{
						auto &cstore = *pending_[i];
						slot.tts_values[i] = cstore.Values()[row];
						slot.tts_isnull[i] = cstore.Nulls()[row];
					}

					ARROW_RETURN_NOT_OK(AppendTuple(&slot));
				}
			}

			// flush all
			ARROW_RETURN_NOT_OK(Flush());

			// all pending data has been actually flushed, clear the buffer
			for (auto &cstore : pending_)
				cstore->Clear();

			return arrow::Status::OK();
		}

	private:
		static constexpr int colAttr = 0;
		static constexpr int colCompression = 1;
		using CreationProp = std::tuple<::Form_pg_attribute, coder::Encoding>;

	private:
		/*!
		 * Make a parquet node for the type oid.
		 *
		 * @param version The format version.
		 * @param attr The column attribute.
		 *
		 * @return the parquet node.
		 * @return nullptr if the type is unsupported.
		 */
		static parquet::schema::NodePtr
		MakeParquetNode(coder::Version version, const ::Form_pg_attribute attr);

		explicit ParquetWriter(::Relation rel, ::Snapshot snapshot,
							   ::Oid footerOid, std::vector<::AttrNumber> &mergeby);

		/*!
		 * Init column writer.
		 */
		arrow::Status Init(const ::RelFileNode &rd_node, std::vector<CreationProp> &typeVec, bool needWAL);

		bool OverlapLastRow(const ::TupleTableSlot *slot);
		bool OverlapRow(const ::TupleTableSlot *slot, int row);

		/*!
		 * @return true if the slot belongs to the current block.
		 */
		bool BelongCurrentBlock(const ::TupleTableSlot *slot);
		bool BelongCurrentBlock(const ::ItemPointer tid);

		/*!
		 * Load the existing logical block of the given slot.
		 */
		void LoadExistingBlock(const ::TupleTableSlot *slot);

		bool LoadBlock(const ::ItemPointer tid);
		void Reorder();
		void MergeOverlapped();

		::MemoryContext per_block_mcxt_;

		::Oid footerOid_;
		std::shared_ptr<MarsFile> out_file_;

		std::vector<PreAgg> preaggs_; //<! per-column preagg
		vector<std::shared_ptr<InsertOp>> opVec_;
		parquet::schema::NodeVector fields_;

		std::shared_ptr<GroupNode> schema_;
		std::shared_ptr<WriterProperties> properties_;

		void RowGroupCreate();

		std::unique_ptr<parquet::ParquetFileWriter> fileWriter_;

		const coder::Version version_;
		footer::Version auxversion_;

		vector<::MarsDimensionAttr> groupkeys_;
		vector<::MarsDimensionAttr> global_orderkeys_;

		std::vector<::AttrNumber> groupkey_store_order_;
		std::vector<::AttrNumber> block_store_order_;

		std::vector<::AttrNumber> block_merge_attr_;

		Scanner scanner_;
		BlockMerger merger_;
		std::list<footer::Footer> footers_; //<! existing logical footers
		::BlockNumber current_batch_;

		footer::AuxFetch auxfetch_;
		ColumnStoreSorter sorter_;
		bool can_merge_;

		std::vector<std::shared_ptr<ColumnStore>> pending_;
		std::vector<int> pending_remap_;
		ColumnStoreSorter pending_sorter_;
	};

} // namespace mars

#endif // MATRIXDB_PARQUETWRITER_H
