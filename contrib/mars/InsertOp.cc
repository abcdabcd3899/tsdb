/*-------------------------------------------------------------------------
 *
 * InsertOp.cc
 *	  MARS Column data writer
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#include "InsertOp.h"

#include <parquet/types.h>

#include <memory>

namespace mars
{
	std::shared_ptr<InsertOp>
	InsertOp::Make(coder::Version version, const ::Form_pg_attribute attr,
				   bool enable_column_encode, coder::Encoding compression)
	{
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

		switch (type_oid)
		{
		case INT4OID:
			return std::make_shared<TypedInsertOp<parquet::Type::INT32>>(version, attr);
		case INT8OID:
			return std::make_shared<TypedInsertOp<parquet::Type::INT64>>(version, attr);
		case FLOAT4OID:
			return std::make_shared<TypedInsertOp<parquet::Type::FLOAT>>(version, attr);
		case FLOAT8OID:
			return std::make_shared<TypedInsertOp<parquet::Type::DOUBLE>>(version, attr);
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
			// timestamps are not values, no need to maintain their sums.
			return std::make_shared<TypedInsertOp<parquet::Type::INT64>>(version, attr);

		default:
			if (!byval)
			{
				// variable-length type
				return std::make_shared<TypedInsertOp<
					parquet::Type::BYTE_ARRAY>>(version,
												attr,
												enable_column_encode,
												compression);
			}

			return nullptr;
		}
	}

} // namespace mars
