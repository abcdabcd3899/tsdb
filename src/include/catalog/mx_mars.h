/*-------------------------------------------------------------------------
 *
 * mx_mars.h
 *	  internal specifications of the MARS relation storage.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/mx_mars.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MX_MARS_H
#define MATRIXDB_MX_MARS_H

#include "postgres.h"

#include "access/attnum.h"
#include "access/tupdesc.h"
#include "catalog/genbki.h"
#include "catalog/mx_mars_d.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 * mx_mars definition.
 */
CATALOG(mx_mars, 6107, MARSRelationId)
{
	Oid relid;		  /* relation id */
	Oid segrelid;	  /* OID of aoseg table; 0 if none */
#ifdef CATALOG_VARLEN /* variable-length fields start here */
	int2 localOrder[1] BKI_DEFAULT(_null_);
	int2 globalOrder[1] BKI_DEFAULT(_null_);
	int2 grpOpt[1] BKI_DEFAULT(_null_);
	int2 grpWithParamOpt[1] BKI_DEFAULT(_null_);
	int8 grpParam[1] BKI_DEFAULT(_null_);
#endif
}
FormData_mx_mars;

FOREIGN_KEY(relid REFERENCES pg_class(oid));

/* ----------------
 *		Form_mx_mars corresponds to a pointer to a tuple with
 *		the format of mx_mars relation.
 * ----------------
 */
typedef FormData_mx_mars *Form_mx_mars;

typedef enum MarsDimensionAttrType
{
	MARS_DIM_ATTR_TYPE_DEFAULT,
	MARS_DIM_ATTR_TYPE_WITH_PARAM
} MarsDimensionAttrType;

/*
 * MarsDimensionAttr stores all metadata of Attribute representing dimension
 * Attribute can be group key, global order, etc.
 */
typedef struct MarsDimensionAttr
{
	NodeTag type;
	MarsDimensionAttrType dimensiontype;
	AttrNumber attnum;
	Datum param;
} MarsDimensionAttr;

/* TODO: create a MARS table ModifyTableNode to store it. */
extern List *mars_infer_elems;

extern List *mx_monotonic_procs;

extern void
InsertMARSEntryWithValidation(Oid relOid, Datum reloptions, TupleDesc tupleDesc);

extern void
GetMARSEntryAuxOids(Oid relid,
					Snapshot metaDataSnapshot,
					Oid *segrelid);

void UpdateMARSEntryAuxOids(Oid relid,
							Oid newSegrelid);

extern void
RemoveMARSEntry(Oid relid);

extern void
GetMARSEntryOption(Oid relid, List **groupkeys,
				   List **global_orderkeys, List **local_orderkeys);

static inline AttrNumber pg_attribute_unused()
	GetAttrNumByName(TupleDesc desc, char *name)
{
	int attr_num = 1;
	for (attr_num = 1; attr_num <= desc->natts; attr_num++)
	{
		if (strcmp(NameStr(desc->attrs[attr_num - 1].attname), name) == 0)
			break;
	}

	return attr_num;
}

static inline bool
InByvalTypeAllowedList(Oid typid)
{
	switch (typid)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
	case FLOAT4OID:
	case FLOAT8OID:
		return true;
	default:
		return false;
	}
}

static inline bool
InVarlenTypeAllowedList(Oid typid)
{
	switch (typid)
	{
	case TEXTOID:
	case VARCHAROID:
	case NAMEOID:
	case NUMERICOID:
	case INTERVALOID:
		return true;
	default:
		return false;
	}
}

extern bool AmIsMars(Oid amoid);
extern bool IsMarsAM(Oid reloid);
extern bool RelationIsMars(Relation rel);

extern bool mars_monotonic_function_catalog_init(bool *try_load);

extern void SwapMarsAuxEntries(Oid datarelid1, Oid datarelid2);

#endif /* MATRIXDB_MX_MARS_H */
