/*-------------------------------------------------------------------------
 *
 * contrib/mars/Lazy.h
 *	  MARS lazy detoasting.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/Lazy.h
 *
 * This file provides many LazyXXX classes to hold varlen data, the data are
 * not detoasted until being used, so it helps to reduce useless detoasting.
 *
 * To use the lazy detoasting first create a lazy object according to the
 * actualy type, for example:
 * - LazyBytea is for varlen types, such as text, varchar, json, etc.;
 * - LazyArrayOfXXX is for pg array types, such as int[], double[], etc.;
 * - LazyXXXVector is for internal vectors, they are stored as bytea, but
 *   the content is an C int/float vector, they are not pg arrays;
 *
 * Then assign the data with Assign().  It might not always be possible to
 * delay the detoasting, or maybe it is possible but just is not cheap enough,
 * the Assign() function can handle all the scenarios automatically.
 *
 * Now we could use GetXXX() functions to get the values.  The assigned will
 * be detoasted if not yet, and detoasted data will be cached.
 *
 * It is important to prevent useless data copying during the process, this is
 * done by using the Buffer class.  It has the concept of data ownership, data
 * can be transfered or bound without copying, and data can be automatically
 * freed after use.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_LAZY_H
#define MARS_LAZY_H

#include "type_traits.hpp"

namespace mars
{
	namespace footer
	{

		/*!
		 * Helper function to read one unaligned value of type T.
		 *
		 * When reading an aligned pointer of type T, we could safely do it like below
		 *
		 *		T value = *(T *) ptr;
		 *
		 * When the pointer is unaligned, we should use memcpy() otherwise it might
		 * crash on unaligned MOV instructions.  The memcpy() can usually be optimized
		 * to an MOV instruction which supports unaligned loading, it is usually
		 * MOVSLQ.
		 *
		 * However we observed that clang (actually clang++) might optimize memcpy()
		 * to MOVQ which crashes on unaligned pointer.
		 */
		template <class T>
		static T
		unaligned_read(const T *ptr)
		{
			T value;

#if defined(__clang__)
			auto data = (const char *)ptr;
			std::copy(data, data + sizeof(T), (char *)&value);
#elif defined(__GNUC__) || defined(__GNUG__)
			::memcpy(&value, ptr, sizeof(T));
#else
#error "mars only compiles with gcc/clang"
#endif
			return value;
		}

		/*!
		 * The lazy buffer, used by the lazy operations.
		 *
		 * The buffer is managed with the concept of ownership, which specifies
		 * whether the buffer owns the data or not.
		 *
		 * It allocates with palloc(), all the memory passed to it must also be
		 * palloc()'ed.
		 */
		class Buffer
		{
		public:
			enum Ownership
			{
				COPY,
				BIND,
				TRANSFER,
			};

		public:
			Buffer() noexcept
				: data_(nullptr), free_(false)
			{
			}

			Buffer(const Buffer &rhs) = delete;

			Buffer(Buffer &&rhs) noexcept
				: data_(rhs.data_), free_(rhs.free_)
			{
				// we want to use std::exchange(), which is available since c++14,
				// however we are compiled with c++11, so we have to do it manually
				rhs.data_ = nullptr;
				rhs.free_ = false;
			}

			~Buffer()
			{
				Free();
			}

			void Clear()
			{
				Free();

				data_ = nullptr;
				free_ = false;
			}

			bool Empty() const
			{
				return data_ == nullptr;
			}

			/*!
			 * Assign data to the buffer.
			 *
			 * The old data is released automatically.
			 */
			void Assign(char *data, size_t size, Ownership ownership)
			{
				switch (ownership)
				{
				case COPY:
					Copy(data, size);
					break;
				case BIND:
					Bind(data);
					break;
				case TRANSFER:
					Transfer(data);
					break;
				}
			}

			/*!
			 * Copy the data.
			 */
			void Copy(const char *data, size_t size)
			{
				Free();

				Assert(data != nullptr);
				Assert(size > 0);

				free_ = true;
				data_ = (char *)::palloc(size);
				::memmove(data_, data, size);
			}

			/*!
			 * Bind the data.
			 *
			 * The data is still owned by the caller.
			 */
			void Bind(char *data)
			{
				Free();

				data_ = data;
				free_ = false;
			}

			/*!
			 * Transfer the ownership of data.
			 *
			 * The data must be palloc()'ed.
			 */
			void Transfer(char *data)
			{
				Free();

				data_ = data;
				free_ = true;
			}

			const char *Data() const
			{
				return data_;
			}

			char *Data()
			{
				return data_;
			}

		protected:
			void Free()
			{
				if (free_)
				{
					::pfree(data_);
				}
			}

		private:
			char *data_;
			bool free_;
		};

		/*!
		 * The base of lazy data.
		 */
		class LazyBase
		{
		protected:
			LazyBase() noexcept = default;
			LazyBase(LazyBase &&) noexcept = default;

		public:
			LazyBase(const LazyBase &) = delete;

			virtual void Reset()
			{
				toasted_.Clear();
			}

			/*!
			 * Assign data.
			 *
			 * @param datum The data pointer, must not be byval.
			 * @param ownership The data ownership.
			 */
			void Assign(::Datum datum, Buffer::Ownership ownership = Buffer::COPY)
			{
				Reset();

				// XXX: do not use DatumGetByteaP(), which detoasts immediately,
				// however what we want is a lazy detoasting.
				if (ownership == Buffer::TRANSFER)
				{
					// the data is transfered to us, it is always cheap enough to do a
					// lazy detoasting;
					const auto data = ::DatumGetCString(datum);
					toasted_.Transfer(data);
				}
				else if (ownership == Buffer::BIND)
				{
					// the data is bound to us, it is always cheap enough to do a lazy
					// detoasting;
					const auto data = ::DatumGetCString(datum);
					toasted_.Bind(data);
				}
				else if (VARATT_IS_EXTERNAL_ONDISK(datum))
				{
					// the data is stored in toast relation, we only need to save the
					// toast header, with it we could detoast later.
					//
					// TODO: maybe we should also compare toast_datum_size() and
					// toast_raw_datum_size(), the lazy detoasting is not worthy unless
					// the raw size is large enough compared to the toast data or toast
					// header.
					const auto data = ::DatumGetCString(datum);
					auto size = VARSIZE_EXTERNAL(datum);

					Assert(size > 0);
					toasted_.Copy(data, size);
				}
				else
				{
					// the data is uncompressed or in-place compressed:
					// - for uncompressed data we must copy the data out immediately;
					// - for in-place compressed data we could do a lazy decompression,
					//   but that requires copying the uncompressed data out now, so
					//   we would rather decompress immediately.
					auto data = (char *)::pg_detoast_datum((::bytea *)DatumGetPointer(datum));
					bool should_free = VARATT_IS_EXTENDED(datum);

					if (should_free)
					{
						// transfer the detoasted data down
						ownership = Buffer::TRANSFER;
					}

					AssignDetoasted(data, ownership);
				}
			}

		protected:
			/*!
			 * Assign detoasted data.
			 *
			 * The toasted_ is never used again after this call, so the implementation
			 * has the choice to clear it or use it for any other purpose.
			 */
			virtual void AssignDetoasted(char *data, Buffer::Ownership ownership) = 0;

		public:
			/*!
			 * Should detoast or not.
			 *
			 * Detoasting should typically happen if detoasting result is empty.  It
			 * does not matter if there is nothing to detoast, this should not happen
			 * in the workflow.
			 *
			 * @return true if detoasting is needed.
			 */
			virtual bool ShouldDetoast() const = 0;

		protected:
			/*!
			 * Detoast the data if not yet.
			 *
			 * This is actually not a const function, however we have to mark it const
			 * to allow getting the value from a const lazy object.
			 */
			void Detoast() const
			{
				if (unlikely(ShouldDetoast()))
				{
					auto datum = PointerGetDatum(toasted_.Data());
					auto data = (char *)::pg_detoast_datum((::bytea *)DatumGetPointer(datum));
					bool should_free = VARATT_IS_EXTENDED(datum);
					Buffer::Ownership ownership;

					if (should_free)
					{
						// the original data is useless since now on
						const_cast<Buffer *>(&toasted_)->Clear();

						// transfer the detoasted data down
						ownership = Buffer::TRANSFER;
					}
					else
					{
						// the original data can be used without detoasting, bind it
						// down, we will keep the ownership.
						ownership = Buffer::BIND;
					}

					const_cast<LazyBase *>(this)->AssignDetoasted(data, ownership);
				}
			}

		protected:
			Buffer toasted_;
		};

		class LazyBytea : public LazyBase
		{
		public:
			LazyBytea() noexcept = default;
			LazyBytea(LazyBytea &&) noexcept = default;
			LazyBytea(const LazyBytea &) = delete;

			void Reset() override
			{
				detoasted_.Clear();
			}

			const ::bytea *GetBytes() const
			{
				Detoast();
				return reinterpret_cast<const ::bytea *>(detoasted_.Data());
			}

			const char *GetData() const
			{
				auto bytes = GetBytes();
				return VARDATA_ANY(bytes);
			}

			const char *GetData(uint32 *size) const
			{
				auto bytes = GetBytes();
				*size = VARSIZE_ANY_EXHDR(bytes);
				return VARDATA_ANY(bytes);
			}

			uint32 GetDataSize() const
			{
				auto bytes = GetBytes();
				return VARSIZE_ANY_EXHDR(bytes);
			}

			bool Empty() const
			{
				return detoasted_.Empty() && toasted_.Empty();
			}

		protected:
			void AssignDetoasted(char *data, Buffer::Ownership ownership) override
			{
				auto size = VARSIZE_ANY(data);
				Assert(size > 0);

				detoasted_.Assign(data, size, ownership);
			}

		public:
			bool ShouldDetoast() const override
			{
				return detoasted_.Empty();
			}

		protected:
			Buffer detoasted_;
		};

		class LazyInt64Vector : public LazyBytea
		{
		public:
			using vtype = int64;

			LazyInt64Vector() noexcept = default;
			LazyInt64Vector(LazyInt64Vector &&) noexcept = default;
			LazyInt64Vector(const LazyInt64Vector &) = delete;

			vtype GetInt64(int nth_value = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetData());
				Assert((size_t)nth_value < GetDataSize() / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyDatumVector : public LazyBytea
		{
		public:
			using vtype = ::Datum;

			LazyDatumVector() noexcept = default;
			LazyDatumVector(LazyDatumVector &&) noexcept = default;
			LazyDatumVector(const LazyDatumVector &) = delete;
			vtype GetDatum(int nth_value = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetData());
				Assert((size_t)nth_value < GetDataSize() / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyVarlenVector : public LazyDatumVector
		{
		public:
			using vtype = ::Datum;

			LazyVarlenVector() noexcept = delete;
			LazyVarlenVector(LazyVarlenVector &&) noexcept = default;
			LazyVarlenVector(const LazyVarlenVector &) = delete;

			LazyVarlenVector(const ::TupleDesc data_tupdesc) noexcept
				: data_tupdesc_(data_tupdesc)
			{
			}

			vtype GetVariableDatum(int nth_value) const
			{
				int natts = data_tupdesc_->natts;
				// nth could be logical
				auto attoff = nth_value < natts ? nth_value : nth_value - natts;
				auto attr = &data_tupdesc_->attrs[attoff];

				uint32 size;
				auto values = reinterpret_cast<const vtype *>(GetData(&size));
				Assert((size_t)nth_value < GetDataSize() / sizeof(vtype));

				vtype data = unaligned_read(values + nth_value);
				int64 offset = DatumGetInt64(data);

				// If size is greater than n * sizeof ::Datum, varlen data is appended.
				// Refer to README -> Auxiliary table -> Data layout
				if (!attr->attbyval && offset > 0 && natts * sizeof(vtype) < size)
				{
					Assert(offset >= natts * (int64)sizeof(vtype) && offset < size);
					data = PointerGetDatum((char *)DatumGetPointer(values) + offset);
				}

				return data;
			}

		private:
			const ::TupleDesc data_tupdesc_;
		};

		class LazyDatum128Vector : public LazyBytea
		{
		public:
			using vtype = Datum128;

			LazyDatum128Vector() noexcept = default;
			LazyDatum128Vector(LazyDatum128Vector &&) noexcept = default;
			LazyDatum128Vector(const LazyDatum128Vector &) = delete;

			vtype GetDatum128(int nth_value = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetData());
				Assert((size_t)nth_value < GetDataSize() / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyArrayBase : public LazyBase
		{
		protected:
			LazyArrayBase(::Oid elmtype, int elmlen,
						  bool elmbyval, char elmalign) noexcept
				: elmtype_(elmtype), elmlen_(elmlen), elmbyval_(elmbyval), elmalign_(elmalign)
			{
			}

			LazyArrayBase(LazyArrayBase &&) noexcept = default;

		public:
			LazyArrayBase(const LazyArrayBase &) = delete;

		protected:
			/*!
			 * Assign detoasted data.
			 *
			 * The detoasted data is further deconstructed.
			 */
			void AssignDetoasted(char *data, Buffer::Ownership ownership) override
			{
				::Datum *values;
				int nelems;

				auto array = reinterpret_cast<::ArrayType *>(data);
				::deconstruct_array(array,
									elmtype_, elmlen_, elmbyval_, elmalign_,
									&values, nullptr /* isnull */, &nelems);
				Assert(nelems > 0);

				// we assume that the deconstructed values have the same ownership with
				// the input data.
				if (ownership == Buffer::TRANSFER)
				{
					// the input data is transfered to me, hold it.
					//
					// XXX: this is a bit tricky, we save the data to toasted_ instead
					// of sth. like "transfered_buffer_", the purpose is to save some
					// bytes in the struct, we must keep the struct as small as
					// possible to get better performance.
					Assert(toasted_.Data() != data);
					toasted_.Transfer(data);

					// now we could bind the elems directly to the data.
					ownership = Buffer::BIND;
				}
				else
				{
					toasted_.Clear();
				}

				AssignArrayValues(nelems, values, ownership);
			}

			/*!
			 * Assign deconstructed array values.
			 *
			 * @param ownership The ownership of the values[*], must not be
			 *                  Buffer::TRANSFER.
			 *
			 * The values pointer is also palloc()'ed, and is always transfered, the
			 * implementations should take the responsibility to free it.
			 */
			virtual void AssignArrayValues(int nelems, ::Datum *values,
										   Buffer::Ownership ownership) = 0;

		protected:
			const ::Oid elmtype_;
			const int elmlen_;
			const bool elmbyval_;
			const char elmalign_;
		};

		class LazyArrayByVal : public LazyArrayBase
		{
		public:
			LazyArrayByVal(::Oid elmtype, int elmlen,
						   bool elmbyval, char elmalign) noexcept
				: LazyArrayBase(elmtype, elmlen, elmbyval, elmalign)
			{
			}

			LazyArrayByVal(LazyArrayByVal &&) noexcept = default;
			LazyArrayByVal(const LazyArrayByVal &) = delete;

		public:
			void Reset() override
			{
				elems_.Clear();
			}

			::Datum GetElem(int nth_elem = 0) const
			{
				Detoast();

				Assert(!elems_.Empty());
				auto datums = reinterpret_cast<const ::Datum *>(elems_.Data());
				return datums[nth_elem];
			}

			void AssignArrayValues(int nelems, ::Datum *values,
								   Buffer::Ownership ownership) override
			{
				// in a byval array the data are stored in the values directly, and
				// is transfered to us.
				ownership = Buffer::TRANSFER;
				auto size = sizeof(::Datum) * nelems;
				elems_.Assign((char *)values, size, ownership);
			}

			bool ShouldDetoast() const override
			{
				return elems_.Empty();
			}

		protected:
			Buffer elems_;
		};

		class LazyArrayByPtr : public LazyArrayBase
		{
		public:
			LazyArrayByPtr() noexcept
				: LazyArrayBase(BYTEAOID, -1, false, 'i')
			{
			}

			LazyArrayByPtr(LazyArrayByPtr &&) noexcept = default;
			LazyArrayByPtr(const LazyArrayByPtr &) = delete;

		public:
			void Reset() override
			{
				elem_.Reset();
			}

			const LazyBytea &GetElem(int nth_elem = 0) const
			{
				Detoast();

				Assert(!elem_.Empty());
				Assert(nth_elem == 0);
				return elem_;
			}

			void AssignArrayValues(int nelems, ::Datum *values,
								   Buffer::Ownership ownership) override
			{
				// in mars we only store 1 row group in each parquet file
				Assert(nelems == 1);
				elem_.Assign(values[0], ownership);

				// this is the datum array of the values, it is transfered to us, and
				// should always be freed.
				::pfree(values);
			}

			bool ShouldDetoast() const override
			{
				return elem_.Empty();
			}

		protected:
			LazyBytea elem_;
		};

		class LazyArrayOfInt64 : public LazyArrayByVal
		{
		public:
			LazyArrayOfInt64() noexcept
				: LazyArrayByVal(INT8OID, 8, true, 'd')
			{
			}

			LazyArrayOfInt64(LazyArrayOfInt64 &&) noexcept = default;
			LazyArrayOfInt64(const LazyArrayOfInt64 &) = delete;

			int64 GetInt64(int nth_elem = 0) const
			{
				return ::DatumGetInt64(GetElem(nth_elem));
			}
		};

		class LazyArrayOfBytea : public LazyArrayByPtr
		{
		protected:
			LazyArrayOfBytea() noexcept = default;
			LazyArrayOfBytea(LazyArrayOfBytea &&) noexcept = default;

		public:
			LazyArrayOfBytea(const LazyArrayOfBytea &) = delete;

		protected:
			const ::bytea *GetElemBytes(int nth_elem = 0) const
			{
				auto &bytes = GetElem(nth_elem);
				return bytes.GetBytes();
			}

			const char *GetElemData(int nth_elem = 0) const
			{
				auto bytes = GetElemBytes();
				return VARDATA_ANY(bytes);
			}

			const char *GetElemData(uint32 *size, int nth_elem = 0) const
			{
				auto bytes = GetElemBytes();
				*size = VARSIZE_ANY_EXHDR(bytes);
				return VARDATA_ANY(bytes);
			}

			uint32 GetElemDataSize(int nth_elem = 0) const
			{
				auto bytes = GetElemBytes();
				return VARSIZE_ANY_EXHDR(bytes);
			}
		};

		class LazyArrayOfInt32Vector : public LazyArrayOfBytea
		{
		public:
			using vtype = int32;

			LazyArrayOfInt32Vector() noexcept = default;
			LazyArrayOfInt32Vector(LazyArrayOfInt32Vector &&) noexcept = default;
			LazyArrayOfInt32Vector(const LazyArrayOfInt32Vector &) = delete;

			vtype GetInt32(int nth_value, int nth_elem = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetElemData(nth_elem));
				Assert((size_t)nth_value < GetElemDataSize(nth_elem) / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyArrayOfInt64Vector : public LazyArrayOfBytea
		{
		public:
			using vtype = int64;

			LazyArrayOfInt64Vector() noexcept = default;
			LazyArrayOfInt64Vector(LazyArrayOfInt64Vector &&) noexcept = default;
			LazyArrayOfInt64Vector(const LazyArrayOfInt64Vector &) = delete;

			vtype GetInt64(int nth_value, int nth_elem = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetElemData(nth_elem));
				Assert((size_t)nth_value < GetElemDataSize(nth_elem) / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyArrayOfDatumVector : public LazyArrayOfBytea
		{
		public:
			using vtype = ::Datum;

			LazyArrayOfDatumVector() noexcept = default;
			LazyArrayOfDatumVector(LazyArrayOfDatumVector &&) noexcept = default;
			LazyArrayOfDatumVector(const LazyArrayOfDatumVector &) = delete;

			vtype GetDatum(int nth_value, int nth_elem = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetElemData(nth_elem));
				Assert((size_t)nth_value < GetElemDataSize(nth_elem) / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

		class LazyArrayOfVarlenVector : public LazyArrayOfDatumVector
		{
		public:
			using vtype = ::Datum;

			LazyArrayOfVarlenVector() noexcept = delete;
			LazyArrayOfVarlenVector(LazyArrayOfVarlenVector &&) noexcept = default;
			LazyArrayOfVarlenVector(const LazyArrayOfVarlenVector &) = delete;

			LazyArrayOfVarlenVector(const ::TupleDesc data_tupdesc) noexcept
				: data_tupdesc_(data_tupdesc)
			{
			}

			vtype GetVariableDatum(int nth_value, int nth_elem = 0) const
			{
				int natts = data_tupdesc_->natts;
				// nth could be logical
				auto attoff = nth_value < natts ? nth_value : nth_value - natts;
				auto attr = &data_tupdesc_->attrs[attoff];

				uint32 size;
				auto values = reinterpret_cast<const vtype *>(GetElemData(&size, nth_elem));
				Assert((size_t)nth_value < GetElemDataSize(nth_elem) / sizeof(vtype));

				vtype data = unaligned_read(values + nth_value);
				int64 offset = DatumGetInt64(data);

				// If size is greater than n * sizeof ::Datum, varlen data is appended.
				// Refer to README -> Auxiliary table -> Data layout
				if (!attr->attbyval && offset > 0 && natts * sizeof(vtype) < size)
				{
					Assert(offset >= natts * (int64)sizeof(vtype) && offset < size);
					data = PointerGetDatum((char *)DatumGetPointer(values) + offset);
				}

				return data;
			}

		private:
			const ::TupleDesc data_tupdesc_;
		};

		class LazyArrayOfDatum128Vector : public LazyArrayOfBytea
		{
		public:
			using vtype = Datum128;

			LazyArrayOfDatum128Vector() noexcept = default;
			LazyArrayOfDatum128Vector(LazyArrayOfDatum128Vector &&) noexcept = default;
			LazyArrayOfDatum128Vector(const LazyArrayOfDatum128Vector &) = delete;

			vtype GetDatum128(int nth_value, int nth_elem = 0) const
			{
				auto values = reinterpret_cast<const vtype *>(GetElemData(nth_elem));
				Assert((size_t)nth_value < GetElemDataSize(nth_elem) / sizeof(vtype));

				return unaligned_read(values + nth_value);
			}
		};

	} // namespace footer
} // namespace mars

#endif // MARS_LAZY_H
