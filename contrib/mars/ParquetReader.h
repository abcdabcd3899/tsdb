/*-------------------------------------------------------------------------
 *
 * ParquetReader.h
 *	  The MARS Parquet Reader
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_PARQUETREADER_H
#define MARS_PARQUETREADER_H

#include "type_traits.hpp"

#include "BlockSlice.h"
#include "BlockMerger.h"
#include "Footer.h"
#include "ScanKey.h"
#include "Scanner.h"
#include "Tle.h"

#include <list>
#include <memory>
#include <vector>

namespace mars
{

	class ParquetReaderImpl;

	class ParquetMergeReader
	{
	public:
		ParquetMergeReader(const Scanner &scanner);

		void BeginScan(std::shared_ptr<ParquetReaderImpl> &&impl);
		void EndScan();
		bool IsEnd() const;
		void Rewind(::ScanDirection scandir);
		bool SeekBlock(::ScanDirection scandir, ::BlockNumber blockno);
		bool NextBlock(::ScanDirection scandir);
		const std::list<const footer::Footer *> &GetFooters() const;

	protected:
		void Clear();
		bool Overlap(::ScanDirection scandir, const footer::Footer *max_min) const;

	protected:
		struct Context
		{
			Context(const Scanner &scanner);

			bool Overlap(const footer::Footer *a, const footer::Footer *b) const;

			const Scanner &scanner_;
			std::vector<::MarsDimensionAttr> mergekeys_;
		};

	protected:
		const Context context_;
		std::shared_ptr<ParquetReaderImpl> impl_;
		std::list<footer::Footer> footers_;
		std::list<const footer::Footer *> group_; //!< the overlapping group
	};

	/*!
	 * The Toplevel MARS Parquet Reader.
	 *
	 * It scans from all the visible parquet logical files.
	 *
	 * @see #BlockReader for the concepts of parquet logical files and blocks.
	 */
	class ParquetReader
	{
	public:
		/*!
		 * The ctor.
		 *
		 * @param rel The postgres relation.
		 * @param is_analyze Whether we are scanning for the ANALYZE command.
		 */
		ParquetReader(::Relation rel, ::Snapshot snapshot, bool is_analyze = false);
		virtual ~ParquetReader();

		/*!
		 * Meet query requirement, set projection column, data order and filter
		 */
		void BeginScan(::ScanState *ss);

		/*!
		 * The same with BeginScan(::ScanState), but without knowledge of ss.
		 *
		 * This should be called by seqscan.
		 */
		void BeginScan(::List *qual = nullptr);

		/*!
		 * Set the projected columns.
		 *
		 * @see mars::Scanner::SetProjectedColumns().
		 */
		void SetProjectedColumns(const std::vector<bool> &projs)
		{
			scanner_.SetProjectedColumns(projs);
		}

		/*!
		 * Set the orderkeys, the output order of the result.
		 *
		 * @see mars::Scanner::SetOrderKeys().
		 */
		void SetOrderKeys(::ScanState *ss,
						  const std::vector<::AttrNumber> &orderkeys,
						  const std::unordered_set<::AttrNumber> &equalkeys);

		void SetTableStoreOrder(const std::vector<::AttrNumber> &table_store_order)
		{
			scanner_.SetTableStoreOrder(table_store_order);
		}

		/*!
		 * Set the time bucket information.
		 *
		 * @see mars::Scanner::SetTimeBucket().
		 */
		void SetTimeBucket(::AttrNumber attr, const ::Interval &interval)
		{
			scanner_.SetTimeBucket(attr, interval);
		}

		/*!
		 * Rewind to the first block.
		 */
		void Rewind(::ScanDirection direction);

		/*!
		 * Rescan from the first block, also reset the arraykey.
		 *
		 * Compared to Rewind(), this method also resets the arraykey iterator, the
		 * scankeys will then be re-optimized and classified.
		 *
		 * This is to be called by the AM rescan api.
		 */
		void Rescan();

		/*!
		 * Read next tuple.
		 *
		 * Only the tuples that match the #scanner_ scankeys will be returned.
		 *
		 * @param direction The scan direction.
		 * @param slot The slot to store the result.
		 *
		 * @return true if one tuple is returned.
		 * @return false if no tuple is returned, which means end of the block.
		 *
		 * @todo The \p direction is currently not checked, we are assuming that
		 *       the scandir is always ForwardScanDirection.  This is true at
		 *       present because we are always called by SeqScan.  However later we
		 *       will be called by a CustomScan which is possible to run backward
		 *       scan.
		 */
		bool ReadNext(::ScanDirection direction, ::TupleTableSlot *slot);

		/*!
		 * Seek the next block.
		 *
		 * @param direction The scan direction.
		 *
		 * @return false if no match or end of the table.
		 */
		bool NextBlock(::ScanDirection direction);

		/*!
		 * Load the current block.
		 *
		 * The current block can be adjusted by NextBlock(), Rewind(), etc., but it
		 * is not automatically loaded, this LoadBlock() must be called before
		 * reading the block.
		 */
		void LoadBlock();

		/*!
		 * Random seek to a block.
		 *
		 * @param blockno the block id to seek.
		 */
		bool SetScanTargetBlock(::BlockNumber blockno);

		/*!
		 * pushdown: Set targetlist for aggscan.
		 *
		 * @todo can tlist be very large?  If that is possible then we'd better
		 *       transfer the ownership directly.
		 */
		void SetFooterScanTlist(std::vector<std::shared_ptr<mars::TLE>> tlist)
		{
			footerscan_tlist_ = std::move(tlist);
		}

		/*!
		 * pushdown: Set group targetlist for aggscan.
		 *
		 * @todo can tlist be very large?  If that is possible then we'd better
		 *       transfer the ownership directly.
		 */
		void SetFooterScanGroupTlist(std::vector<std::shared_ptr<mars::TLE>> group_tlist_)
		{
			footerscan_group_tlist_ = std::move(group_tlist_);
		}

		/*!
		 * pushdown: Try to read the pre-aggregate info in the current footer.
		 *
		 * They are valid only when current footer is unique on the group key and
		 * all tuples are fully matching the filter.
		 *
		 * @param direction The scan direction.
		 * @param slot The slot store the result.
		 * @param plain_agg True if the agg strategy is AGG_PLAIN.
		 *
		 * @return false if no match or end of the current footer.
		 */
		bool FooterRead(::ScanDirection direction,
						::TupleTableSlot *slot, bool plain_agg);

		/*!
		 * pushdown: Read one tuple in the current row group.
		 *
		 * @param direction The scan direction.
		 * @param slot The slot store the result.
		 *
		 * @return false if no match or end of the current footer.
		 */
		bool ReadNextInRowGroup(::ScanDirection direction, ::TupleTableSlot *slot);

		/*!
		 * pushdown: Calculate the footerscan ratio among all.
		 *
		 * @return The ratio of footerscan.
		 * @return 0.0 if no tuple is scanned.
		 */
		double GetFooterScanRate() const
		{
			if (footerscan_ntuples_ || contentscan_ntuples_)
			{
				return footerscan_ntuples_ / (contentscan_ntuples_ +
											  footerscan_ntuples_);
			}

			return 0.0;
		}

		/*!
		 * Get the scanner.
		 *
		 * Use with caution, this is designed for random accessing only, and only
		 * modify the scanner if you understand what you are doing.
		 *
		 * @return The scanner.
		 */
		mars::Scanner &GetScanner()
		{
			return scanner_;
		}

	private:
		/*!
		 * Extract ScanState projection column information.
		 */
		void ExtractColumns(::ScanState *ss);

		/*!
		 * Extract ScanState data order information.
		 */
		std::vector<::AttrNumber> ExtractOrderKeys(::ScanState *ss);

		/*!
		 * Extract ScanState filter information.
		 *
		 * @param scankeys out parameter
		 * @param arraykeys out parameter
		 * @param runtimekeys out parameter
		 */
		void ExtractScanKeys(::ScanState *ss,
							 std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
							 std::shared_ptr<std::vector<::IndexArrayKeyInfo>> &arraykeys,
							 std::shared_ptr<std::vector<::IndexRuntimeKeyInfo>> &runtimekeys);

		/*!
		 * Extract filter information for SeqScan.
		 *
		 * When called by SeqScan we cannot see a valid ScanState, so arraykeys and
		 * runtimekeys cannot be extracted, but at least we could extract the
		 * scankeys.
		 */
		void ExtractScanKeys(::List *qual,
							 std::vector<std::shared_ptr<mars::ScanKey>> &scankeys);

	private:
		/*!
		 * This MemoryContext reset after each read block finish and released at
		 * the end of the top transaction.
		 */
		::MemoryContext per_table_mcxt_;

		/*!
		 * This MemoryContext reset after each read block finish and released at
		 * the end of the top transaction.
		 */
		::MemoryContext per_block_mcxt_;

		mars::Scanner scanner_; //!< The scanner
		ParquetMergeReader mreader_;
		BlockMerger merger_;

		::Relation rel_;

		/*!
		 * Whether we have seeked the first block.
		 *
		 * When a scan begins, nothing has been done, all the actual work are done
		 * during the reading: seeking the first matching block, exhausting it, and
		 * moving to the next.
		 *
		 * When this flag is false, we know the scan has began, but no block has
		 * been seeked.  Once we run the seeking logic, no matter there are matches
		 * or not, this flag is set to true.
		 */
		bool seeked_;

		std::vector<std::shared_ptr<mars::TLE>> footerscan_tlist_;
		//!< pushdown: tlist for footerscan
		std::vector<std::shared_ptr<mars::TLE>> footerscan_group_tlist_;
		//!< pushdown: group tlist for footerscan
		double footerscan_ntuples_;	 //!< pushdown: # of tuples for footerscan
		double contentscan_ntuples_; //!< pushdown: # of tuples for contentscan

		bool can_aggscan_; // !< true means mars currently only partially supports query conditions
	};

	/*!
	 * The block scanner interface.
	 */
	class ParquetReaderImpl
	{
	public:
		/*!
		 * Make an actual block scanner according to the scanner.
		 *
		 * This should be called as early as possible, usually right after the
		 * decision of the aux index.
		 *
		 * The return block scanner is not began yet, use Rewind() for the purpose.
		 */
		static std::shared_ptr<ParquetReaderImpl> Make(::MemoryContext per_table_mcxt,
													   ::MemoryContext per_block_mcxt,
													   Scanner &scanner);

	protected:
		ParquetReaderImpl(::MemoryContext per_table_mcxt,
						  ::MemoryContext per_block_mcxt,
						  Scanner &scanner)
			: per_table_mcxt_(per_table_mcxt), per_block_mcxt_(per_block_mcxt), scanner_(scanner)
		{
		}

	public:
		virtual ~ParquetReaderImpl() = default;

		/*!
		 * Return true if the end is reached.
		 *
		 * @note Do not call GetFooter() in such a case.
		 */
		virtual bool IsEnd() const = 0;

		/*!
		 * Rescan from the first matching block.
		 *
		 * Implementations must recheck the table scankeys in Rewind(), it is
		 * allowed to be different in each round.
		 */
		virtual void Rewind(::ScanDirection scandir) = 0;

		/*!
		 * Seek to the given block.
		 *
		 * This is used by the analyze mode, the blockno is guaranteed to change in
		 * the scan direction.
		 *
		 * @return true if the block exists.
		 * @return false if the block does not exist.
		 */
		virtual bool SeekBlock(::ScanDirection scandir, ::BlockNumber blockno) = 0;

		/*!
		 * Move to the next matching block.
		 *
		 * The seeked block must match all the table-level scankeys, no matter it
		 * is a full match or partial match.
		 *
		 * @return false if the end is reached.
		 */
		virtual bool NextBlock(::ScanDirection scandir) = 0;

		/*!
		 * Get the footer of the current block.
		 *
		 * The block must be valid, the caller must check IsEnd() to ensure this.
		 */
		virtual const footer::Footer &GetFooter() = 0;

		virtual footer::Footer TransferFooter() = 0;

	protected:
		::MemoryContext per_table_mcxt_;
		::MemoryContext per_block_mcxt_;

		Scanner &scanner_;
	};

	/*!
	 * The legacy block scanner.
	 *
	 * It loads all the footers and do a binary search for fast seeking.
	 *
	 * It consumes a lot of memory to store the footers, especially when there are
	 * lots of blocks in the table.  This wil be retired in the future.
	 */
	class ParquetReaderImplLegacy : public ParquetReaderImpl
	{
	public:
		ParquetReaderImplLegacy(::MemoryContext per_table_mcxt,
								::MemoryContext per_block_mcxt,
								Scanner &scanner);
		~ParquetReaderImplLegacy() override;

		bool IsEnd() const override;
		void Rewind(::ScanDirection scandir) override;
		bool SeekBlock(::ScanDirection scandir, ::BlockNumber blockno) override;
		bool NextBlock(::ScanDirection scandir) override;
		const footer::Footer &GetFooter() override;
		footer::Footer TransferFooter() override;

	protected:
		footer::Footer::footers_type footers_; //!< All the footers
		BlockSlice blocks_;					   //!< The block slice
		bool loaded_footers_;				   //!< Footers are loaded or not
	};

	/*!
	 * The lazy block scanner.
	 *
	 * It scans the blocks by using the aux index, only the current block is
	 * loaded.
	 *
	 * @todo At the moment it requres an index for the scanning, for the case that
	 * the mars relation contains no table order or groupkeys we still have to use
	 * the ParquetReaderImplLegacy.  We should let ParquetReaderImplLazy handle the
	 * no index case by doing a full table scan on the aux relation, this is
	 * necessary to retire ParquetReaderImplLegacy, which is important to reduce
	 * the memory usage of mars.
	 */
	class ParquetReaderImplLazy : public ParquetReaderImpl
	{
	public:
		ParquetReaderImplLazy(::MemoryContext per_table_mcxt,
							  ::MemoryContext per_block_mcxt,
							  Scanner &scanner);
		~ParquetReaderImplLazy() override = default;

		bool IsEnd() const override;
		void Rewind(::ScanDirection scandir) override;
		bool SeekBlock(::ScanDirection scandir, ::BlockNumber blockno) override;
		bool NextBlock(::ScanDirection scandir) override;
		const footer::Footer &GetFooter() override;
		footer::Footer TransferFooter() override;

	protected:
		footer::AuxScan auxscan_;
	};

	/*!
	 * A simple wrapper for the Table AM apis.
	 */
	struct ScanWrapper
	{
		TableScanDescData scan_; //!< The AM scan body, must be the first
		ParquetReader *preader_; //!< The actual reader
	};

} // namespace mars

#endif /* MARS_PARQUETREADER_H */
