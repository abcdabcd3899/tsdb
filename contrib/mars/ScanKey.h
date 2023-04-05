/*-------------------------------------------------------------------------
 *
 * ScanKey.h
 *	  The MARS ScanKey
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_SCANKEY_H
#define MARS_SCANKEY_H

#include "type_traits.hpp"

#include "BlockSlice.h"
#include "Compare.h"
#include "Footer.h"

#include <memory>
#include <unordered_set>

namespace mars
{

	/*!
	 * The packaged ::ScanKeyData.
	 */
	class ScanKey
	{
	public:
		/*!
		 * The policy when merging two scankeys.
		 *
		 * @todo Maybe we also need a KEEP_EITHER.
		 */
		enum MergePolicy
		{
			KEEP_A,	   //!< Keeps A, the old one.
			KEEP_B,	   //!< Keeps B, the new one.
			KEEP_BOTH, //!< Keeps both.
			KEEP_NONE, //!< Keeps none, which means paradox keys.
		};

		/*!
		 * The matching type of a block or a row.
		 */
		enum MatchType
		{
			/*!
			 * All the checked keys match.
			 *
			 * For blocks, this means that some of the tuples in a block might
			 * match the keys.
			 */
			MATCH,

			/*!
			 * A block fully matches all the checked keys.
			 *
			 * This is only meaningful on blocks, and is only returned by
			 * MatchBlock().
			 */
			MATCH_FULL,

			/*!
			 * Mismatch on a filter.
			 *
			 * The caller should move to the next row and try again.
			 */
			MISMATCH_FILTER,

			/*!
			 * Mismatch on the third or following ordered key.
			 *
			 * The caller could treat this the same with MISMATCH_FILTER.
			 */
			MISMATCH_ORDERKEY,

			/*!
			 * Mismatch on the first ordered key.
			 *
			 * The caller can skip the current block.
			 */
			MISMATCH_FIRST_ORDERKEY,

			/*!
			 * Mismatch on the second ordered key.
			 *
			 * The caller could try to skip to the next first-order-key value.
			 *
			 * For example, if the orderkeys are (tag, time), and current tag is
			 * 100, it is time to seek to the first position with tag > 100.
			 */
			MISMATCH_SECOND_ORDERKEY,

			//! The alias to #MATCH_FULL, only used in MatchBlock().
			BLOCK_FULL_MATCH = MATCH_FULL,
			//! The alias to #MATCH, only used in MatchBlock().
			BLOCK_MAYBE_MATCH = MATCH,
			//! The alias to #MISMATCH_FILTER, only used in MatchBlock().
			BLOCK_MISMATCH = MISMATCH_FILTER,
		};

		/*!
		 * The arraykey type.
		 */
		enum ArrayKeyType
		{
			NO_ARRAY,	  /*!< Not having an array at all. */
			ARRAY_FILTER, /*!< An array filter, a value matches the filter if it
						   * matches any of the array values. */
			ARRAY_KEY,	  /*!< An array key, only one of the array values is in
						   * use, so a value matches iff it matches this specific
						   * array value. */
		};

	public:
		using const_footer_iterator = std::vector<mars::footer::Footer>::const_iterator;

		ScanKey() = delete;

		/*!
		 * The Ctor.
		 *
		 * @param lefttype The datum type.
		 * @param righttype The scankey type.
		 * @param collation The collation.
		 */
		ScanKey(::Oid lefttype, ::Oid righttype, ::Oid collation)
			: lr_comp_(lefttype, righttype, collation), rl_comp_(righttype, lefttype, collation), ll_comp_(lefttype, lefttype, collation), rr_comp_(righttype, righttype, collation), arraykey_(nullptr), runtimeKey_(nullptr)
		{
		}

		/*!
		 * Make a new TypedScanKey based on the \p lefttype and \p righttype.
		 *
		 * Below types are supported as \p righttype:
		 * - INT32OID
		 * - INT64OID
		 * - FLOAT4OID
		 * - FLOAT8OID
		 *
		 * @param lefttype The column type oid.
		 * @param righttype The scankey type oid.
		 * @param collation The collation, can be InvalidOid.
		 *
		 * @return The TypedScanKey.
		 * @return nullptr if the type is not supported.
		 */
		static std::shared_ptr<mars::ScanKey> Make(::Oid lefttype,
												   ::Oid righttype,
												   ::Oid collation);

		/*!
		 * Merge two ScanKey's.
		 *
		 * This function does not do the merging, it only suggests how to merge.
		 *
		 * @param scankey1,scankey2 The ScanKey's to be merged.
		 *
		 * @return The policy on how to merge.
		 */
		static MergePolicy Merge(const mars::ScanKey &scankey1,
								 const mars::ScanKey &scankey2);

		/*!
		 * Optimize the scankeys by reducing the duplicates.
		 *
		 * Scankeys on the same attribute are merged into one.
		 *
		 * @param[in] original_scankeys The original scankeys, must be ordered by
		 *                              attrnum.
		 * @param[out] scankeys The resulting scankeys.
		 *
		 * @return true if everything is ok, the result can be empty though, which
		 *         means no useful scankey is left;
		 * @return false if no match is possible, which means paradox keys are
		 *         detected, for example, `= 1` and `> 1`.
		 */
		static bool Reduce(const std::vector<std::shared_ptr<mars::ScanKey>> &original_scankeys,
						   std::vector<std::shared_ptr<mars::ScanKey>> &scankeys);

		/*!
		 * Decide the global/final strategy of the scankeys.
		 *
		 * The global/final strategy is the highest strategy before any `<` or
		 * `<=` (for forward scan direction), for example (suppose the order keys
		 * are (c1, c2)):
		 *
		 * - `c1 = 1 and c2 = 2`: final strategy is `= (1, 2)`;
		 * - `c1 > 1 and c2 = 2`: final strategy is `> (1, 2)`;
		 * - `c1 = 1 and c2 > 2`: final strategy is `> (1, 2)`;
		 * - `c1 > 1 and c2 >= 2`: final strategy is `>= (1, 2)`;
		 * - `c1 > 1 and c2 < 2`: final strategy is `> (1)`;
		 * - `c1 < 1 and c2 = 2`: final strategy is empty;
		 *
		 * @param scandir The scan direction.
		 * @param scankeys The scankeys.
		 *
		 * @return The global/final strategy and corresponding keys.
		 */
		static std::pair<::StrategyNumber, std::vector<std::shared_ptr<mars::ScanKey>>>
		DecideStrategy(::ScanDirection scandir,
					   const std::vector<std::shared_ptr<mars::ScanKey>> &scankeys);

		bool IsEqualKey() const
		{
			if (GetArrayKeyType() != mars::ScanKey::NO_ARRAY)
				// matrixdb does not strip off "=" arraykey, neither should we
				return false;
			else if (runtimeKey_)
				// matrixdb does not strip off "=" runtimekey, neither should we
				return false;
			else
				return scankey_.sk_strategy == BTEqualStrategyNumber;
		}

		/*!
		 * Bind an arraykey to the scankey.
		 *
		 * An example of binding:
		 *
		 * @code{.cpp}
		 * mars::ScanKey scankey;
		 * ::IndexArrayKeyInfo arraykey;
		 *
		 * arraykey.scan_key = &scankey;
		 * scankey.BindArrayKey(&arraykey0;
		 * @endcode
		 *
		 * @param arraykey The arraykey, its ::IndexArrayKeyInfo::scan_key must be
		 *        set to the scankey before calling this.
		 */
		void BindArrayKey(::IndexArrayKeyInfo *arraykey)
		{
			Assert(arraykey->scan_key == &scankey_);
			Assert(arraykey_ == nullptr);
			arraykey_ = arraykey;
		}

		/*!
		 * Bind a runtimekey to the scankey.
		 *
		 * An example of binding:
		 *
		 * @code{.cpp}
		 * mars::ScanKey scankey;
		 * ::IndexRuntimeKeyInfo runtimekey;
		 *
		 * arraykey.scan_key = &scankey;
		 * scankey.BindArrayKey(&runtimekey);
		 * @endcode
		 *
		 * @param runtimekey The tuntimekey, its ::IndexRuntimeKeyInfo::scan_key must be
		 *        set to the scankey before calling this.
		 */
		void BindRuntimeKey(::IndexRuntimeKeyInfo *runtimekey)
		{
			Assert(runtimekey->scan_key == &scankey_);
			Assert(runtimeKey_ == nullptr);
			runtimeKey_ = runtimekey;
		}

		/*!
		 * Check whether the scankey is an array key or an array filter.
		 *
		 * @return ArrayKeyType::#NO_ARRAY if it has no arraykey at all.
		 * @return ArrayKeyType::#ARRAY_FILTER if it is an array filter.
		 * @return ArrayKeyType::#ARRAY_KEY if it is an array key.
		 */
		ArrayKeyType GetArrayKeyType() const
		{
			return (arraykey_ == nullptr ? ArrayKeyType::NO_ARRAY : arraykey_->next_elem < 0 ? ArrayKeyType::ARRAY_FILTER
																							 : ArrayKeyType::ARRAY_KEY);
		}

		/*!
		 * Begin the arraykey scan.
		 *
		 * This should be called no matter an arraykey is bound or not.
		 *
		 * @param scandir The scan dir.
		 *
		 * @return false if there are an arraykey, but it contains only null
		 *         values, so no match is possible, the caller should stop asap.
		 * @return true otherwise, there are no arraykey, or a non-null value, the
		 *         scan should continue.
		 */
		bool ArrayKeyScanBegin(::ScanDirection scandir);

		/*!
		 * Move to the next arraykey value.
		 *
		 * @param scandir The scan dir.
		 *
		 * @return true if moved to a new arraykey value, or false if no more.
		 */
		bool ArrayKeyScanNext(::ScanDirection scandir);

		/*!
		 * Narrow down \p begin and \p end with the scankey.
		 *
		 * It will move \p blocks to the first matching block according to the scan
		 * direction.  No movement is made when it is already at a matching block.
		 *
		 *
		 * @param scandir The scan direction.
		 * @param[in,out] blocks The block slice to scan.
		 */
		void FirstMatchBlock(BlockSlice &blocks) const;

		/*!
		 * Matches a block on a specific column.
		 *
		 * @param footer The block.
		 * @param column The column index.
		 * @param logical Test as a logical block or not.
		 *
		 * @return MatchType::#BLOCK_MAYBE_MATCH if either the min or max, not
		 *         both, matches the scankey, which means that some of the tuples
		 *         in the block match the scankey, so the tuples in the block needs
		 *         to be matched one by one.
		 * @return MatchType::#BLOCK_FULL_MATCH if both the min and max match the
		 *         scankey, which means that all the tuples in the block match the
		 *         scankey, so no need to match the tuples one by one.
		 * @return MatchType::#BLOCK_MISMATCH if neither the min nor the max
		 *         matches the scankey, which means that no tuple in the block can
		 *         match, the block could be skipped directly.
		 */
		MatchType MatchBlock(const mars::footer::Footer &footer,
							 int column, bool logical = false) const;

		/*!
		 * Checks whether \p datum matches the scankey.
		 *
		 * @param datum The datum to check.
		 *
		 * @return true if it matches, or false if not.
		 */
		bool MatchPoint(::Datum datum) const;

		/*!
		 * Preprocess the arraykeys.
		 *
		 * - Identify array keys and filters;
		 * - Also convert the array values to a set for fast matching;
		 */
		void PreprocessArrayKeys();

		/*!
		 * Test "datum OP scankey".
		 */
		bool Compare(::Datum datum, mars::OP op, const mars::ScanKey &key) const
		{
			return lr_comp_(datum, op, key.scankey_.sk_argument);
		}

		// we allow public assess to this field
		::ScanKeyData scankey_;

	protected:
		/*!
		 * Test "key1.sk_argument < key2.sk_argument".
		 */
		bool operator<(const mars::ScanKey &key) const
		{
			return rr_comp_(scankey_.sk_argument, LT, key.scankey_.sk_argument);
		}

		/*!
		 * Test "key1.sk_argument <= key2.sk_argument".
		 */
		bool operator<=(const mars::ScanKey &key) const
		{
			return rr_comp_(scankey_.sk_argument, LE, key.scankey_.sk_argument);
		}

		/*!
		 * Test "key1.sk_argument == key2.sk_argument".
		 */
		bool operator==(const mars::ScanKey &key) const
		{
			return rr_comp_.equal(scankey_.sk_argument, key.scankey_.sk_argument);
		}

		/*!
		 * Test "key1.sk_argument >= key2.sk_argument".
		 */
		bool operator>=(const mars::ScanKey &key) const
		{
			return rr_comp_(scankey_.sk_argument, GE, key.scankey_.sk_argument);
		}

		/*!
		 * Test "key1.sk_argument > key2.sk_argument".
		 */
		bool operator>(const mars::ScanKey &key) const
		{
			return rr_comp_(scankey_.sk_argument, GT, key.scankey_.sk_argument);
		}

	protected:
		/*!
		 * Helper functions to compare (datum, op, arg).
		 */
		const DatumCompare lr_comp_;

		/*!
		 * Helper functions to compare (arg, op, datum).
		 */
		const DatumCompare rl_comp_;

		/*!
		 * Helper functions to compare (datum, op, datum).
		 */
		const DatumCompare ll_comp_;

		/*!
		 * Helper functions to compare (arg, op, arg).
		 */
		const DatumCompare rr_comp_;

		/*!
		 * The arraykey bound to the scankey, nullptr if no bound one.
		 *
		 * @note it is not owned by the scankey, do not delete it.
		 * @todo replace raw pointer
		 */
		::IndexArrayKeyInfo *arraykey_;

		/*!
		 * In deed, ScanKey class object does not touch runtimeKey_. However, the
		 * runtimeKey_ update our private scankey_ value for each rescan loop.
		 * The real update scanKey_ is placing at customrescan routine.
		 */
		::IndexRuntimeKeyInfo *runtimeKey_;
	};

} // namespace mars

#endif /* MARS_SCANKEY_H */
