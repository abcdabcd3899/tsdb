/*-------------------------------------------------------------------------
 *
 * Footer.h
 *	  MARS footer Aux table operator
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/Footer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FOOTER_H
#define FOOTER_H

#include "type_traits.hpp"

#include "Compare.h"
#include "Lazy.h"

#include <parquet/metadata.h>

#include <cstdint>
#include <string>
#include <vector>

extern "C"
{

	/*!
	 * The V1 mars footer catalog layout.
	 */
	typedef struct pg_attribute_packed() FormData_mars_footer_v1
	{
		int64 segno;
		int64 batch;
		::bytea *content;
		int32 rowgroup_count; //<! always 1
#ifdef MARS_VARLEN
		int64 rowcount[rowgroup_count];				//<! store as array of bytea
		int64 null_count[rowgroup_count][ncolumns]; //<! store as array of bytea
		::Datum128 sum[rowgroup_count][ncolumns];	//<! store as array of bytea
		int32 min_at[rowgroup_count][ncolumns];		//<! store as array of bytea
		int32 max_at[rowgroup_count][ncolumns];		//<! store as array of bytea
		::Datum min[rowgroup_count][ncolumns];		//<! store as array of bytea
		::Datum max[rowgroup_count][ncolumns];		//<! store as array of bytea
#endif												// MARS_VARLEN
	}
	pg_attribute_packed() FormData_mars_footer_v1;

	typedef FormData_mars_footer_v1 *Form_mars_footer_v1;

	/*!
	 * The V2 mars footer catalog layout.
	 */
	typedef struct pg_attribute_packed() FormData_mars_footer_v2
	{
		int64 segno;
		int64 batch;
		::bytea *content;
		int32 rowgroup_count; //<! always 1
#ifdef MARS_VARLEN
		int64 rowcount;
		int64 null_count[ncolumns]; //<! store as bytea
		::Datum128 sum[ncolumns];	//<! store as bytea
		int64 min_at[ncolumns];		//<! store as bytea
		int64 max_at[ncolumns];		//<! store as bytea
		::Datum min[ncolumns];		//<! store as bytea
		::Datum max[ncolumns];		//<! store as bytea
		::Datum edges[ncolumns];	//<! store as bytea
#endif								// MARS_VARLEN
	}
	pg_attribute_packed() FormData_mars_footer_v2;

	typedef FormData_mars_footer_v2 *Form_mars_footer_v2;

} // extern "C"

namespace mars
{

	class PreAgg;
	class ScanKey;

	namespace footer
	{

		/*!
		 * The attrnums of aux tables.
		 *
		 * @note Removing any of them breaks backward compatibility, NEVERY DO THIS.
		 */
		enum
		{
			Anum_mars_footer_segno = 1,
			Anum_mars_footer_batch,
			Anum_mars_footer_content,
			Anum_mars_footer_rowgroup_count,
			Anum_mars_footer_rowcount,
			Anum_mars_footer_null_count, /*!< per-column null count */
			Anum_mars_footer_sum,
			Anum_mars_footer_min_at, /*<! per-column row-id of min values */
			Anum_mars_footer_max_at, /*<! per-column row-id of max values */
			Anum_mars_footer_min,
			Anum_mars_footer_max,

			// end of v1 columns

			Anum_mars_footer_edges, /*<! first & last values in physical
									 * store order */

			// end of v2 columns
		};

		static constexpr int Natts_mars_footer_v1 = Anum_mars_footer_max;
		static constexpr int Natts_mars_footer_v2 = Anum_mars_footer_edges;

		static constexpr int64 ALLBATCH = -1;

		enum IndexType
		{
			INVALID_INDEX_TYPE,
			ORDER_KEY_MIN_FIRST,
			ORDER_KEY_MAX_FIRST,
			GROUP_KEY,
			GROUP_PARAM_KEY,

			// end of dynamic indexes, below are all __fixed__ indexes

			__FIRST_FIXED = 1000,
			BATCH = __FIRST_FIXED,
		};

		static constexpr bool pg_attribute_unused()
			aux_index_is_on_groupkeys(IndexType indextype)
		{
			return (indextype == IndexType::GROUP_KEY ||
					indextype == IndexType::GROUP_PARAM_KEY);
		}

		static constexpr bool pg_attribute_unused()
			aux_index_is_dynamic(IndexType indextype)
		{
			return indextype < IndexType::__FIRST_FIXED;
		}

		static constexpr bool pg_attribute_unused()
			aux_index_is_fixed(IndexType indextype)
		{
			return indextype >= IndexType::__FIRST_FIXED;
		}

		/*!
		 * The aux table schema version.
		 *
		 * The version information is not directly stored as a field anywhere, it can
		 * be detected by checking the tupdesc, see footer_version_from_tupdesc() for
		 * details.
		 */
		enum Version
		{
			V1, //<! store as Form_mars_footer_v1
			V2, //<! store as Form_mars_footer_v2
		};

		/*!
		 * The default aux table schema version.
		 *
		 * It is always the latest version.
		 */
		static constexpr Version DEFAULT_VERSION = Version::V2;

		/*!
		 * The default aux table schema version at runtime.
		 *
		 * Can be set via the mars.default_auxfile_format GUC.
		 */
		extern Version default_version;

		/*!
		 * Detect aux table schema version from tupdesc.
		 *
		 * The aux table schema version is decided during table creation, all the
		 * operations after that, such as SELECT and INSERT, should detect the version
		 * with this function and determine the schema to follow.
		 *
		 * At the moment there are 2 versions, V1 and V2, described by
		 * Form_mars_footer_v1 and Form_mars_footer_v2.
		 */
		static inline Version pg_attribute_unused()
			footer_version_from_tupdesc(const ::TupleDesc aux_tupdesc)
		{
			// the "rowcount" type is changed from int[]@v1 to int@v2
			auto &attr = aux_tupdesc->attrs[Anum_mars_footer_rowcount - 1];

			switch (attr.atttypid)
			{
			case INT8ARRAYOID:
				return Version::V1;
			case INT8OID:
				return Version::V2;
			default:
				elog(ERROR, "unsupported mars aux table layout");
			}
		}

		static constexpr int pg_attribute_unused()
			get_fixed_footer_nattrs(Version version)
		{
			return version == Version::V1 ? Natts_mars_footer_v1 : Natts_mars_footer_v2;
		}

		/*!
		 * The preagg information in a parquet row group.
		 *
		 * The mars::PreAgg struct maintains per-column preagg information in a row
		 * group, while RowGroupPreAgg combines them by preagg attribute, for example
		 * the min values of all the columns are stored in one vector.
		 */
		struct RowGroupPreAgg
		{
			RowGroupPreAgg(int ncols, int nrows)
				: physical_rowcount_(nrows), logical_rowcount_(0)
			{
				// reserve double size for both the physical and logical preagg
				min_.reserve(ncols * 2);
				max_.reserve(ncols * 2);
				sum_.reserve(ncols * 2);
				null_count_.reserve(ncols * 2);
				min_at_.reserve(ncols * 2);
				max_at_.reserve(ncols * 2);
				// edges store twice points than others, so double double
				edges_.reserve(ncols * 2 * 2);
			}

			RowGroupPreAgg(const RowGroupPreAgg &) = delete;
			RowGroupPreAgg(RowGroupPreAgg &&) noexcept = default;

			/*!
			 * Push the preagg info of a column.
			 *
			 * Make sure to push them from columns[0] to columns[ncols-1];
			 */
			void Push(const PreAgg &preagg);

			void SetLogicalRowCount(int nrows)
			{
				logical_rowcount_ = nrows;
			}

			/*!
			 * Pack the preagg info into datums.
			 *
			 * @param[in] tupdesc The tupdesc describes the aux table schema.
			 * @param[out] values,isnull The datums buffer to store the pack result.
			 *
			 * The datums buffer must be large enough to contain tupdesc->nattrs
			 * datums, however not every column is filled by us, we only pack the
			 * preagg columns, other columns are not touched.
			 */
			void Pack(const ::TupleDesc data_tupdesc,
					  const ::TupleDesc aux_tupdesc,
					  ::Datum *values, bool *isnull) const
			{
				auto version = footer_version_from_tupdesc(aux_tupdesc);

				if (version == Version::V1)
				{
					PackV1(data_tupdesc, aux_tupdesc, values, isnull);
				}
				else
				{
					Assert(version == Version::V2);
					PackV2(data_tupdesc, aux_tupdesc, values, isnull);
				}
			}

			void PackV1(const ::TupleDesc data_tupdesc,
						const ::TupleDesc aux_tupdesc,
						::Datum *values, bool *isnull) const;
			void PackV2(const ::TupleDesc data_tupdesc,
						const ::TupleDesc aux_tupdesc,
						::Datum *values, bool *isnull) const;

			uint32 physical_rowcount_;
			uint32 logical_rowcount_;
			std::vector<::Datum> min_;
			std::vector<::Datum> max_;
			std::vector<mars::Datum128> sum_;
			std::vector<int64> null_count_; /*!< per-column null count */
			std::vector<int32> min_at_;		/*<! per-column row id of min values */
			std::vector<int32> max_at_;		/*<! per-column row id of max values */

			// below are new preagg in v2

			std::vector<::Datum> edges_; /*<! per-column first & last values in
										  * physical store order */

		protected:
			int64 EncodeRowCount() const
			{
				uint64 v = (uint64(logical_rowcount_) << 32) | physical_rowcount_;
				Assert(int64(v) >= 0);
				return int64(v);
			}

		protected:
			static ::Datum package_any(const char *data, size_t datalen)
			{
				auto fulllen = datalen + VARHDRSZ;
				auto bytes = (::bytea *)palloc(fulllen);
				SET_VARSIZE(bytes, fulllen);
				::memcpy(VARDATA(bytes), data, datalen);
				return PointerGetDatum(bytes);
			}

			static ::Datum package_int64(int64 value)
			{
				return Int64GetDatum(value);
			};

			static ::Datum package_string(const std::string &str)
			{
				return package_any(str.data(), str.size());
			};

			static ::Datum package_int32_vector(const std::vector<int32> &values)
			{
				return package_any((const char *)values.data(),
								   sizeof(values[0]) * values.size());
			};

			static ::Datum package_int64_vector(const std::vector<int64> &values)
			{
				return package_any((const char *)values.data(),
								   sizeof(values[0]) * values.size());
			};

			static ::Datum package_datum_vector(const std::vector<::Datum> &values)
			{
				return package_any((const char *)values.data(),
								   sizeof(values[0]) * values.size());
			};

			static ::Datum package_varlen_vector(const std::vector<::Datum> &values,
												 const ::TupleDesc data_tupdesc)
			{
				auto data_attrs = data_tupdesc->attrs;
				auto data_natts = data_tupdesc->natts;

				int datalen = SIZEOF_DATUM * values.size() + VARHDRSZ;
				for (int j = 0; j < (int)values.size(); ++j)
				{
					auto attoff = j < data_natts ? j : j - data_natts;
					const auto data_attr = &data_attrs[attoff];
					if (!data_attr->attbyval && DatumGetPointer(values[j]))
					{
						datalen = att_align_nominal(datalen, data_attr->attalign);

						datalen += byptr_value_get_length(data_attr, values[j]);
					}
				}

				// Refer to README -> Auxiliary table -> Data layout
				char *bytes = (char *)palloc0(datalen);
				SET_VARSIZE(bytes, datalen);
				char *data = VARDATA(bytes);
				int offset = SIZEOF_DATUM * values.size() + VARHDRSZ;
				for (int j = 0; j < (int)values.size(); ++j)
				{
					auto attoff = j < data_natts ? j : j - data_natts;
					const auto data_attr = &data_attrs[attoff];
					if (!data_attr->attbyval && DatumGetPointer(values[j]))
					{
						offset = att_align_nominal(offset, data_attr->attalign);

						::Datum offset_wo_bytea = offset - VARHDRSZ;
						memcpy(data, &offset_wo_bytea, SIZEOF_DATUM);

						int length = byptr_value_get_length(data_attr, values[j]);
						memcpy(bytes + offset, DatumGetPointer(values[j]), length);
						offset += length;
					}
					else
						memcpy(data, &values[j], SIZEOF_DATUM);

					data += SIZEOF_DATUM;
				}

				return PointerGetDatum(bytes);
			};

			static ::Datum package_datum128_vector(const std::vector<mars::Datum128> &values)
			{
				return package_any((const char *)values.data(),
								   sizeof(values[0]) * values.size());
			};
		};

		/*!
		 * The footer implementation interface.
		 *
		 * We used to store all the information in the Footer struct directly, the
		 * problem is that in FetchFooters() we load all the footers into a vector, and
		 * we do not know the total count, so the vector has to be reallocated several
		 * times.  We have improved Footer to support std::move(), however by putting
		 * many attributes into the struct it is still a lot of effort during the
		 * moving.  So we move all the attributes into FooterImpl, the Footer only
		 * stores an pointer to FooterImpl, so the moving can be very fast.
		 *
		 * This interface is implemented by FooterImplV1 and FooterImplV2.
		 */
		struct FooterImpl
		{
		protected:
			FooterImpl(::MemoryContext mcxt,
					   const ::TupleDesc data_tupdesc,
					   ::Oid relid = InvalidOid,
					   ::Oid aux_oid = InvalidOid) noexcept
				: mcxt_(mcxt), data_tupdesc_(data_tupdesc), relid_(relid), auxOid_(aux_oid), has_logical_(false)
			{
			}

			FooterImpl(FooterImpl &&) noexcept = default;

		public:
			FooterImpl(const FooterImpl &) = delete;

			virtual ~FooterImpl() = default;

			/*!
			 * Assign a tuple to the impl.
			 *
			 * The tuple can be freed after the call, all the necessary information are
			 * copied out.
			 */
			virtual void Assign(const ::TupleDesc tupdesc, const ::HeapTuple tuple)
			{
#define getattr(attnum) \
	::fastgetattr(tuple, attnum, tupdesc, &isnull)

				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				bool isnull;

				tid_ = tuple->t_self;

				segno_ = DatumGetInt64(getattr(Anum_mars_footer_segno));
				batch_ = DatumGetInt64(getattr(Anum_mars_footer_batch));

				::MemoryContextSwitchTo(oldcxt);

#undef getattr
			}

			virtual footer::Version Version() const = 0;

			/*!
			 * Get the parquet file metadata content.
			 *
			 * @param[out] size The content size.
			 *
			 * @return The content.
			 */
			virtual const char *Content(uint32 *size) const = 0;
			virtual int64 RowCount() const = 0;
			virtual int64 NullCount(int column) const = 0;
			virtual Datum128 Sum(int column) const = 0;
			virtual int64 MinAt(int column) const = 0;
			virtual int64 MaxAt(int column) const = 0;
			virtual ::Datum MinValue(int column) const = 0;
			virtual ::Datum MaxValue(int column) const = 0;
			virtual ::Datum PhysicalFirst(int column) const = 0;
			virtual ::Datum PhysicalLast(int column) const = 0;

		public:
			::MemoryContext mcxt_;
			const ::TupleDesc data_tupdesc_;

			::Oid relid_;
			::Oid auxOid_;
			::ItemPointerData tid_;
			int64_t segno_;
			int64_t batch_;
			bool has_logical_;
		};

		struct FooterImplV1 : public FooterImpl
		{
			FooterImplV1(::MemoryContext mcxt,
						 const ::TupleDesc data_tupdesc,
						 ::Oid relid = InvalidOid,
						 ::Oid aux_oid = InvalidOid) noexcept
				: FooterImpl(mcxt, data_tupdesc, relid, aux_oid), min_(data_tupdesc), max_(data_tupdesc)
			{
			}

			FooterImplV1(const FooterImplV1 &) = delete;
			FooterImplV1(FooterImplV1 &&) noexcept = default;

			void Assign(const ::TupleDesc tupdesc, const ::HeapTuple tuple) override
			{
				FooterImpl::Assign(tupdesc, tuple);

#define getattr(attnum) \
	::fastgetattr(tuple, attnum, tupdesc, &isnull)

				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				bool isnull;

				content_.Assign(getattr(Anum_mars_footer_content));
				rowcount_.Assign(getattr(Anum_mars_footer_rowcount));
				null_count_.Assign(getattr(Anum_mars_footer_null_count));
				sum_.Assign(getattr(Anum_mars_footer_sum));
				min_at_.Assign(getattr(Anum_mars_footer_min_at));
				max_at_.Assign(getattr(Anum_mars_footer_max_at));
				min_.Assign(getattr(Anum_mars_footer_min));
				max_.Assign(getattr(Anum_mars_footer_max));

				::MemoryContextSwitchTo(oldcxt);

#undef getattr
			}

			footer::Version Version() const override
			{
				return footer::Version::V1;
			}

			const char *Content(uint32 *size) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto data = content_.GetData(size);
				::MemoryContextSwitchTo(oldcxt);
				return data;
			}

			int64 RowCount() const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = rowcount_.GetInt64();
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			int64 NullCount(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = null_count_.GetInt64(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			Datum128 Sum(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto datum = sum_.GetDatum128(column);
				::MemoryContextSwitchTo(oldcxt);
				return datum;
			}

			int64 MinAt(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = min_at_.GetInt32(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			int64 MaxAt(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = max_at_.GetInt32(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum MinValue(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = min_.GetVariableDatum(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum MaxValue(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = max_.GetVariableDatum(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum PhysicalFirst(int column) const override
			{
				elog(ERROR, "physical first value is unavailable in mars V1 aux table");
			}

			::Datum PhysicalLast(int column) const override
			{
				elog(ERROR, "physical last value is unavailable in mars V1 aux table");
			}

		private:
			LazyBytea content_;
			LazyArrayOfInt64 rowcount_;
			LazyArrayOfInt64Vector null_count_;
			LazyArrayOfDatum128Vector sum_;
			LazyArrayOfInt32Vector min_at_;
			LazyArrayOfInt32Vector max_at_;
			LazyArrayOfVarlenVector min_;
			LazyArrayOfVarlenVector max_;
		};

		struct FooterImplV2 : public FooterImpl
		{
			FooterImplV2(::MemoryContext mcxt,
						 const ::TupleDesc data_tupdesc,
						 ::Oid relid = InvalidOid,
						 ::Oid aux_oid = InvalidOid) noexcept
				: FooterImpl(mcxt, data_tupdesc, relid, aux_oid), min_(data_tupdesc), max_(data_tupdesc)
			{
			}

			void Assign(const ::TupleDesc tupdesc, const ::HeapTuple tuple) override
			{
				FooterImpl::Assign(tupdesc, tuple);

#define getattr(attnum) \
	::fastgetattr(tuple, attnum, tupdesc, &isnull)

				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				bool isnull;

				rowcount_ = DatumGetInt64(getattr(Anum_mars_footer_rowcount));

				content_.Assign(getattr(Anum_mars_footer_content));
				null_count_.Assign(getattr(Anum_mars_footer_null_count));
				sum_.Assign(getattr(Anum_mars_footer_sum));
				min_at_.Assign(getattr(Anum_mars_footer_min_at));
				max_at_.Assign(getattr(Anum_mars_footer_max_at));
				min_.Assign(getattr(Anum_mars_footer_min));
				max_.Assign(getattr(Anum_mars_footer_max));

				edges_.Assign(getattr(Anum_mars_footer_edges));

				::MemoryContextSwitchTo(oldcxt);

#undef getattr
			}

			FooterImplV2(const FooterImplV2 &) = delete;
			FooterImplV2(FooterImplV2 &&) noexcept = default;

			footer::Version Version() const override
			{
				return footer::Version::V2;
			}

			const char *Content(uint32 *size) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				*size = content_.GetDataSize();
				auto data = content_.GetData();
				::MemoryContextSwitchTo(oldcxt);
				return data;
			}

			int64 RowCount() const override
			{
				return rowcount_;
			}

			int64 NullCount(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = null_count_.GetInt64(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			Datum128 Sum(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto datum = sum_.GetDatum128(column);
				::MemoryContextSwitchTo(oldcxt);
				return datum;
			}

			int64 MinAt(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = min_at_.GetInt64(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			int64 MaxAt(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = max_at_.GetInt64(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum MinValue(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = min_.GetVariableDatum(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum MaxValue(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = max_.GetVariableDatum(column);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum PhysicalFirst(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = edges_.GetDatum(column * 2 + 0);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

			::Datum PhysicalLast(int column) const override
			{
				auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
				auto result = edges_.GetDatum(column * 2 + 1);
				::MemoryContextSwitchTo(oldcxt);
				return result;
			}

		private:
			LazyBytea content_;
			int64 rowcount_;
			LazyInt64Vector null_count_;
			LazyDatum128Vector sum_;
			LazyInt64Vector min_at_;
			LazyInt64Vector max_at_;
			LazyVarlenVector min_;
			LazyVarlenVector max_;
			LazyDatumVector edges_;
		};

		struct Footer
		{
			typedef std::vector<Footer, Allocator<Footer>> footers_type;

			/*!
			 * This ctor initializes a footer for writing.
			 */
			explicit Footer(Version version, ::MemoryContext mcxt,
							const ::TupleDesc data_tupdesc,
							::Oid relid, ::Oid aux_oid) noexcept
			{
				Assert(version == V1 || version == V2);
				if (version == V1)
				{
					impl_ = new (::MemoryContextAlloc(mcxt, sizeof(FooterImplV1)))
						FooterImplV1(mcxt, data_tupdesc, relid, aux_oid);
				}
				else if (version == V2)
				{
					impl_ = new (::MemoryContextAlloc(mcxt, sizeof(FooterImplV2)))
						FooterImplV2(mcxt, data_tupdesc, relid, aux_oid);
				}
			}

			/*!
			 * This ctor initializes a footer for reading.
			 *
			 * @param tuple The tuple to assign to the footer.
			 * @param data_tupdesc The tuple description of the data tuple.
			 * @param aux_tupdesc The tuple description of the auxiliary tuple.
			 *
			 * The tuple is allowed to be null, in such a case the caller must call
			 * Assign() explicitly to assign an tuple, and nothing else should be
			 * called before that, the behavior of an unassigned footer is undefined.
			 */
			explicit Footer(Version version, ::MemoryContext mcxt,
							const ::TupleDesc data_tupdesc,
							const ::TupleDesc aux_tupdesc,
							const ::HeapTuple tuple)
				: Footer(version, mcxt, data_tupdesc, InvalidOid, InvalidOid)
			{
				if (tuple)
				{
					Assert(aux_tupdesc != nullptr);
					Assign(aux_tupdesc, tuple);
				}
			}

			Footer(Footer &&rhs) noexcept
			{
				impl_ = rhs.impl_;
				rhs.impl_ = nullptr;
			}

			~Footer()
			{
				if (impl_)
				{
					impl_->~FooterImpl();
					::pfree(impl_);
					impl_ = nullptr;
				}
			}

			Footer(const Footer &) = delete;

			/*!
			 * Transfer the data from rhs to this.
			 */
			Footer Transfer() noexcept
			{
				Footer tmp(impl_->Version(), impl_->mcxt_, impl_->data_tupdesc_,
						   impl_->relid_, impl_->auxOid_);
				std::swap(tmp.impl_, impl_);
				return tmp;
			}

			/*!
			 * Assign an tuple to the footer.
			 *
			 * @param tuple The tuple to assign to the footer.
			 * @param tupdesc The tuple description of the tuple.
			 *
			 * A footer can be assigned multiple times.
			 *
			 * The tuple and the tupdesc must not be null.  The tuple, as well as the
			 * underlying buffer page, can be released after the call, all the
			 * necessary data are copied within the call.
			 *
			 * @todo Provide a bind mode, which assumes that the tuple is alive long
			 *       enough, at least longer than the footer cycle, so memory can be
			 *       bound instead of copied, which should be more efficient.
			 */
			void Assign(const ::TupleDesc tupdesc, const ::HeapTuple tuple)
			{
				Assert(tupdesc != nullptr);
				Assert(tuple != nullptr);

				impl_->Assign(tupdesc, tuple);
				impl_->has_logical_ = (impl_->RowCount() >> 32) > 0;
			}

			void StoreFooterToAux(const std::string &footer_content,
								  const RowGroupPreAgg &preagg,
								  std::vector<::MarsDimensionAttr> &groupkeys,
								  std::vector<::MarsDimensionAttr> &orderkeys,
								  std::vector<::ItemPointerData> retired = {});

			bool HasLogicalInfo() const
			{
				return impl_->has_logical_;
			}

			/*!
			 * Return the row count.
			 *
			 * @param column The column id, counting from 0; -1 means count(*).
			 *
			 * @return The count(*) if \p column < 0, null values are also counted.
			 * @return The count(column) otherwise, null values are not counted.
			 */
			int64_t GetRowCount(int column = -1, bool logical = false) const
			{
				auto count = impl_->RowCount();

				// get the logical / physical count
				if (logical)
					count >>= 32;
				else
					count &= 0xffffffff;

				if (column >= 0)
				{
					count -= GetNullCount(column, logical);
				}

				return count;
			}

			/*!
			 * Get the null count of the column.
			 *
			 * @param column The column id, counting from 0.
			 *
			 * @return The null count.
			 */
			int64_t GetNullCount(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->NullCount(column);
			}

			// TODO:: For these three funcs, we need to check column param boundary.
			// But num_col_ does not always set to correct value.

			/*!
			 * Check if the column has only one single value.
			 *
			 * @param column The column id.
			 * @param comp The comparator for the column.
			 *
			 * @return true if the column's min and max values values are the same.
			 */
			bool SingleValue(int column, const DatumCompare &comp,
							 bool logical = false) const
			{
				if (GetRowCount(column, logical) == 0)
				{
					// Report all-null as single-value, so the caller does not need to
					// scan the rows.
					return true;
				}

				auto min = GetMinDatum(column, logical);
				auto max = GetMaxDatum(column, logical);
				return comp.equal(min, max);
			}

			/*!
			 * Get the row id where the min value of the column is on.
			 *
			 * @param column The column id.
			 *
			 * @return The row id.
			 */
			int32_t GetMinAt(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->MinAt(column);
			}

			/*!
			 * Get the row id where the max value of the column is on.
			 *
			 * @param column The column id.
			 *
			 * @return The row id.
			 */
			int32_t GetMaxAt(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->MaxAt(column);
			}

			/*!
			 * Get the first value of the column in physical store order.
			 *
			 * @param column The column id.
			 *
			 * @return The datum.
			 */
			::Datum GetPhysicalFirstDatum(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->PhysicalFirst(column);
			}

			/*!
			 * Get the last value of the column in physical store order.
			 *
			 * @param column The column id.
			 *
			 * @return The datum.
			 */
			::Datum GetPhysicalLastDatum(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->PhysicalLast(column);
			}

			/*!
			 * @return true if the row is the physical first row of the column.
			 *
			 * @note This is not the "physical" or "logical", but the edges.
			 */
			bool IsPhysicalFirst(int column, int row, bool logical = false) const
			{
				switch (impl_->Version())
				{
				case Version::V1:
					return false; // physical edges are not supported in v1
				case Version::V2:
					return row == 0;
				default:
					pg_unreachable();
				}
			}

			/*!
			 * @return true if the row is the physical last row of the column.
			 *
			 * @note This is not the "physical" or "logical", but the edges.
			 */
			bool IsPhysicalLast(int column, int row, bool logical = false) const
			{
				switch (impl_->Version())
				{
				case Version::V1:
					return false; // physical edges are not supported in v1
				case Version::V2:
					return row == GetRowCount(column, logical) - 1;
				default:
					pg_unreachable();
				}
			}

			/*!
			 * Get the MIN value of the column, as a typed value.
			 *
			 * @param column The column id.
			 *
			 * @return The typed value.
			 */
			::Datum GetMinDatum(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->MinValue(column);
			}

			/*!
			 * Get the MAX value of the column, as a typed value.
			 *
			 * @param column The column id.
			 *
			 * @return The typed value.
			 */
			::Datum GetMaxDatum(int column, bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				return impl_->MaxValue(column);
			}

			/*!
			 * Get the SUM of the column, in a proper type.
			 *
			 * @param column The column id.
			 *
			 * @return The sum in a proper type.
			 */
			template <parquet::Type::type T>
			typename type_traits<T>::sum_type FooterGetSum(int column,
														   bool logical = false) const
			{
				if (logical)
					column += impl_->data_tupdesc_->natts;
				using sum_type = typename type_traits<T>::sum_type;
				auto datum = impl_->Sum(column);
				return *reinterpret_cast<sum_type *>(&datum);
			}

			const char *GetFooterContent(uint32 *size) const
			{
				return impl_->Content(size);
			}

			static bool CompareBatch(const Footer *a, const Footer *b)
			{
				return a->impl_->batch_ < b->impl_->batch_;
			}

			FooterImpl *impl_;
		};

		void CreateFooterCatalog(::Relation rel);
		::Oid RelationGetFooterRelid(::Relation rel);
		void FetchFooters(::MemoryContext mcxt, Footer::footers_type &footers,
						  ::Relation rel, ::Snapshot snapshot);
		bool FetchFooter(::MemoryContext mcxt, Footer &footer,
						 ::Relation datarel, ::Snapshot snapshot,
						 const ::ItemPointer auxctid);
		void SeeLocalChanges();

		void
		GetListOfOrderKeys(::Relation rel, ::List **orderkey, ::List **local_orderkey);

		std::pair<::Oid, std::vector<::AttrNumber>>
		DecideAuxIndex(::Relation rel, const std::vector<::AttrNumber> &orderkeys,
					   const std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
					   const std::unordered_set<::AttrNumber> &equalkeys);

		/*!
		 * Scan the aux table via aux indexscan.
		 *
		 * There are several concepts used by this class:
		 *
		 * - data relation: the mars relation directly created by user;
		 * - aux relation: an automatically created catalog relation, every data
		 *   relation has an aux relation;
		 * - aux index(es): the index(es) automatically created on an aux relation,
		 *   there can be one or several, or even none, depends on the table-level
		 *   order and the grouping policy of the data relation;
		 *
		 * When we talk about scankeys or somthing in this class, they are for the aux
		 * relation unless explicitly specified.
		 */
		class AuxScan
		{
		public:
			/*!
			 * The ctor.
			 *
			 * @param mcxt The memory context, should be a per-table one.
			 * @param datarel The data relation.
			 *
			 * @todo A per-block memory context should be enough, to be verified.
			 *
			 * The scan is not yet began, the caller must call BeginScan() for the
			 * purpose.
			 */
			AuxScan(::MemoryContext mcxt, ::Relation datarel, ::Snapshot snapshot);
			~AuxScan();

			/*!
			 * Begin the scan.
			 *
			 * @param auxindexoid The aux index oid, must be valid.
			 * @param data_scankeys The table-level scankeys on the data relation.
			 *
			 * No row is scanned yet, call GetNext() for the purpose.
			 *
			 * An AuxScan can be used to scan multiple times, on different indexes or
			 * with different scankeys, the caller just need to ensure that EndScan()
			 * is called after each round.
			 */
			void BeginScan(::Oid auxindexoid,
						   const std::vector<std::shared_ptr<mars::ScanKey>> &data_scankeys);

			/*!
			 * Scan one aux row.
			 *
			 * @param scandir The scan direction.
			 * @param load Do not load the footer unless this arg is true.
			 *
			 * @return true if one row is scanned or skipped.
			 * @return false if the end is reached, nothing is scanned or skipped.
			 */
			bool GetNext(::ScanDirection scandir, bool load = true);

			/*!
			 * End the scan.
			 */
			void EndScan();

			/*!
			 * @return true if BeginScan() is called, no matter IsEnd() or not.
			 */
			bool Scanning() const
			{
				return scan_ != nullptr;
			}

			/*!
			 * Test whether it is end of the scan.
			 *
			 * This method should only be called after a GetNext().  In fact it can not
			 * distinguish the begin of the scan (between BeginScan() and the first
			 * GetNext()) with the end of the scan (the last GetNext() returns false).
			 *
			 * @return true if it is end of the scan, or false if not.
			 */
			bool IsEnd() const
			{
				return !HeapTupleIsValid(tuple_);
			}

			/*!
			 * Get the footer of the current row.
			 *
			 * Only call this on a valid row, for example:
			 *
			 *		if (auxscan.GetNext(scandir)) {
			 *			auto &footer = auxscan.GetFooter();
			 *		}
			 *
			 * @todo refactor GetNext() and GetFooter(), assign the tuple in
			 *       GetFooter() instead of GetNext(), so there is no need to pass a
			 *       "skip" argument to GetNext().  Just need a flag to record whether
			 *       the assignment is made or not.
			 */
			const Footer &GetFooter() const
			{
				Assert(HeapTupleIsValid(tuple_));
				return footer_;
			}

			Footer TransferFooter()
			{
				Assert(HeapTupleIsValid(tuple_));
				return footer_.Transfer();
				;
			}

			void LoadFooter()
			{
				footer_.Assign(RelationGetDescr(auxrel_), tuple_);
			}

			/*!
			 * Get the physical batch number of the current row.
			 */
			int64 GetBatchNumber() const
			{
				Assert(HeapTupleIsValid(tuple_));
				Assert(auxrel_);

				bool isnull;
				auto batch = ::fastgetattr(tuple_, Anum_mars_footer_batch,
										   RelationGetDescr(auxrel_), &isnull);
				Assert(!isnull);
				return batch;
			}

		protected:
			/*!
			 * Helper function to open the aux relation.
			 *
			 * The mcxt_ and datarel_ must be set before calling this.
			 *
			 * @return The aux relation, must call EndScan() to close it.
			 */
			::Relation OpenAuxRel();

			/*!
			 * Build one aux scankey with the given data scankey.
			 *
			 * The result is pushed to scankeys_ directly.
			 */
			void BuildScanKey(const ::ScanKeyData *data_scankey,
							  ::StrategyNumber strategy, ::Datum argument,
							  IndexType indextype, bool is_min);

			/*!
			 * Convert a data scankey to an aux scankey.
			 *
			 * @param data_scankey The data scankey.
			 * @param indextype The type of the aux index.
			 * @param interval The bucket interval of the key, <= 0 if no interval.
			 *
			 * The result is pushed to scankeys_ directly.
			 */
			void ConvertScanKey(const ::ScanKey data_scankey,
								IndexType indextype, int64 interval);

			/*!
			 * Convert the data scankeys to aux ones.
			 *
			 * The results are saved in scankeys_.
			 */
			void ConvertScanKeys(const std::vector<std::shared_ptr<mars::ScanKey>> &data_scankeys);

		protected:
			::MemoryContext mcxt_;
			const ::Relation datarel_;
			const ::Snapshot snapshot_;

			::Relation auxrel_;
			::Relation indexrel_;

			::SysScanDesc scan_;
			::HeapTuple tuple_;

			Footer footer_;

			std::vector<::ScanKeyData> scankeys_;
		};

		/*!
		 * Fetch the matching blocks with the given scankeys.
		 */
		class AuxFetch
		{
		public:
			AuxFetch(::LOCKMODE lockmode);
			~AuxFetch();

			/*!
			 * Open the aux & aux index relations to match full groupkeys.
			 */
			bool OpenIndexForFullGroupKeys(::Oid datareli, const ::Snapshot snapshotd);
			void CloseIndex();

			void BeginFetch(const ::TupleTableSlot *slot);
			bool FetchNext();
			void EndFetch();
			void GetFooter(Footer &footer) const;

			bool IsEnd() const
			{
				return !HeapTupleIsValid(tuple_);
			}

			std::vector<::AttrNumber> ReduceMergeKey(const std::vector<::AttrNumber> &mergeby);

		protected:
			::Snapshot snapshot_;

			::Relation auxrel_;
			::Relation auxindexrel_;
			::LOCKMODE aux_rel_lockmode_;

			::SysScanDesc scan_;
			::HeapTuple tuple_;

			std::vector<::ScanKeyData> scankeys_;
			std::vector<int64> intervals_;

			/*!
			 * Translate table to convert aux attnum to data attnum.
			 */
			std::vector<::AttrNumber> trans_;

			std::vector<::AttrNumber> single_value_group_key_;
			std::vector<::AttrNumber> range_group_key_;
		};

		/*!
		 * Helper class to compare a block with a all-null target.
		 */
		struct ColumnCompareAllNull
		{
			ColumnCompareAllNull(int column)
				: column_(column)
			{
			}

			/*!
			 * @return true if block is not all-null.
			 */
			bool operator()(const Footer &footer, int) const
			{
				bool all_nulls = footer.GetRowCount(column_) == 0;
				return !all_nulls;
			}

			/*!
			 * @return true if block is all-null.
			 */
			bool operator()(int, const Footer &footer) const
			{
				bool all_nulls = footer.GetRowCount(column_) == 0;
				return all_nulls;
			}

			int column_;
		};

	} // namespace footer
} // namespace mars

#endif /* FOOTER_H */
