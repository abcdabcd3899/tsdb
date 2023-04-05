#include "Footer_test.h"

#include "../Footer.cc"

// FIXME: to verify the GetMin() & GetMax() functions we have to mock so many
// functions, any better way?
extern "C" {

int			gp_safefswritesize; /* set for safe AO writes in non-mature fs */

Oid
get_relname_relid(const char *relname, Oid relnamespace)
{
	return InvalidOid;
}

TupleDesc
CreateTemplateTupleDesc(int natts)
{
	return NULL;
}

void
TupleDescInitEntry(TupleDesc desc,
				   AttrNumber attributeNumber,
				   const char *attributeName,
				   Oid oidtypeid,
				   int32 typmod,
				   int attdim)
{
}

Oid
heap_create_with_catalog(const char *relname,
						 Oid relnamespace,
						 Oid reltablespace,
						 Oid relid,
						 Oid reltypeid,
						 Oid reloftypeid,
						 Oid ownerid,
						 Oid accessmtd,
						 TupleDesc tupdesc,
						 List *cooked_constraints,
						 char relkind,
						 char relpersistence,
						 bool shared_relation,
						 bool mapped_relation,
						 OnCommitAction oncommit,
						 const struct GpPolicy *policy,
						 Datum reloptions,
						 bool use_user_acl,
						 bool allow_system_table_mods,
						 bool is_internal,
						 Oid relrewrite,
						 ObjectAddress *typaddress,
						 bool valid_opts)
{
	return InvalidOid;
}

HeapTuple
heaptuple_form_to(TupleDesc tupleDescriptor, Datum *values, bool *isnull,
				  HeapTuple dst, uint32 *dstlen)
{
	return NULL;
}

void
AlterTableCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode)
{
}

void
CommandCounterIncrement(void)
{
}

void
InsertInitialFastSequenceEntries(Oid objid)
{
}

void
InsertAppendOnlyEntry(Oid relid, 
					  int blocksize, 
					  int safefswritesize, 
					  int compresslevel,
					  bool checksum,
					  bool columnstore,
					  char* compresstype,
					  Oid segrelid,
					  Oid blkdirrelid,
					  Oid blkdiridxid,
					  Oid visimaprelid,
					  Oid visimapidxid)
{
}

void
GetAppendOnlyEntryAuxOids(Oid relid,
						  Snapshot appendOnlyMetaDataSnapshot,
						  Oid *segrelid,
						  Oid *blkdirrelid,
						  Oid *blkdiridxid,
						  Oid *visimaprelid,
						  Oid *visimapidxid)
{
}

void
recordDependencyOn(const ObjectAddress *depender,
				   const ObjectAddress *referenced,
				   DependencyType behavior)
{
}

Relation
relation_open(Oid relationId, LOCKMODE lockmode)
{
	return NULL;
}

void
relation_close(Relation relation, LOCKMODE lockmode)
{
}

void
RelationSetNewRelfilenode(Relation relation, char persistence)
{
}

Snapshot
GetTransactionSnapshot(void)
{
	return NULL;
}

SysScanDesc
systable_beginscan(Relation heapRelation,
				   Oid indexId,
				   bool indexOK,
				   Snapshot snapshot,
				   int nkeys, ScanKey key)
{
	return NULL;
}

HeapTuple
systable_getnext(SysScanDesc sysscan)
{
	return NULL;
}

void
systable_endscan(SysScanDesc sysscan)
{
}

void
ScanKeyInit(ScanKey entry,
			AttrNumber attributeNumber,
			StrategyNumber strategy,
			RegProcedure procedure,
			Datum argument)
{
}

Datum
nocachegetattr(HeapTuple tuple,
			   int attnum,
			   TupleDesc tupleDesc)
{
	return (Datum) 0;
}

ArrayType *
construct_array(Datum *elems, int nelems,
				Oid elmtype,
				int elmlen, bool elmbyval, char elmalign)
{
	return NULL;
}

void
deconstruct_array(ArrayType *array,
				  Oid elmtype,
				  int elmlen, bool elmbyval, char elmalign,
				  Datum **elemsp, bool **nullsp, int *nelemsp)
{
}

struct varlena *
pg_detoast_datum(struct varlena *datum)
{
	return NULL;
}

char *
GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
				int backendId, ForkNumber forkNumber)
{
	return NULL;
}

void
CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
}

void
heap_freetuple(HeapTuple htup)
{
}

int64
GetFastSequences(Oid objid, int64 objmod,
				 int64 minSequence, int64 numSequences)
{
	return 0;
}

void *
palloc(Size size)
{
	return NULL;
}

void
pfree(void *pointer)
{
}

} // extern "C"

namespace mars {
namespace test {

using FooterTypes = ::testing::Types<int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(FooterTypedTest, FooterTypes, mars::test::type_names);

TYPED_TEST(FooterTypedTest, MinMax)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using FooterTest = FooterTypedTest<typename T::c_type>;

	mars::footer::Footer footer{0};
	auto column = 0;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;

	FooterTest::SetMinMax(footer, column, x1, x2);
	ASSERT_EQ(footer.FooterGetMin<typename T::c_type>(column), x1);
	ASSERT_EQ(footer.FooterGetMax<typename T::c_type>(column), x2);
}

} // namespace test
} // namespace mars
