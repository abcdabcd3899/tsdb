/*-------------------------------------------------------------------------
 *
 * Footer.cc
 *	  MARS footer Aux table operator
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/Footer.cc
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"
#include "type_traits.hpp"

#include "Footer.h"

#include "FileManager.h"
#include "PreAgg.h"
#include "ScanKey.h"
#include "guc.h"
#include "ts_func.h"
#include "utils.h"

#include <parquet/api/writer.h>

#include <cstring>

namespace mars
{
	namespace footer
	{

		Version default_version = DEFAULT_VERSION;

		// Truncates name by multibyte character boundary
		static void
		TruncateName(char name[NAMEDATALEN])
		{
			int len = pg_mbcliplen(name, strlen(name), NAMEDATALEN - 1);
			name[len] = 0;
		}

		static void
		OrderKeyGetAuxName(char name[NAMEDATALEN], ::TupleDesc tupdesc,
						   ::AttrNumber orderkey, bool is_min)
		{
			::Form_pg_attribute attr = &tupdesc->attrs[orderkey - 1];
			const char *attname = NameStr(attr->attname);
			const char *prefix = is_min ? "min" : "max";

			snprintf(name, NAMEDATALEN, "attr%d__%s__%s", orderkey, prefix, attname);

			TruncateName(name);
		}

		static void
		GroupKeyGetAuxName(char aux_attname[NAMEDATALEN], const char *data_attname,
						   ::AttrNumber groupkey)
		{
			snprintf(aux_attname, NAMEDATALEN, "attr%d__group_key__%s",
					 groupkey, data_attname);

			TruncateName(aux_attname);
		}

		static void
		GroupKeyGetAuxName(char name[NAMEDATALEN], ::TupleDesc tupdesc,
						   ::AttrNumber group_key)
		{
			::Form_pg_attribute attr = &tupdesc->attrs[group_key - 1];
			const char *attname = NameStr(attr->attname);

			GroupKeyGetAuxName(name, attname, group_key);
		}

		static void
		GroupParamKeyGetAuxName(char aux_attname[NAMEDATALEN],
								const char *data_attname, ::AttrNumber groupkey)
		{
			snprintf(aux_attname, NAMEDATALEN, "attr%d__group_param_key__%s",
					 groupkey, data_attname);

			TruncateName(aux_attname);
		}

		static void
		GroupParamKeyGetAuxName(char name[NAMEDATALEN], ::TupleDesc tupdesc,
								::AttrNumber group_param_key)
		{
			::Form_pg_attribute attr = &tupdesc->attrs[group_param_key - 1];
			const char *attname = NameStr(attr->attname);

			GroupParamKeyGetAuxName(name, attname, group_param_key);
		}

		static void
		GroupKeyGetAuxName(char aux_attname[NAMEDATALEN], const char *data_attname,
						   const ::MarsDimensionAttr *groupkey)
		{
			auto data_attnum = groupkey->attnum;

			if (groupkey->dimensiontype == MARS_DIM_ATTR_TYPE_DEFAULT)
			{
				GroupKeyGetAuxName(aux_attname, data_attname, data_attnum);
			}
			else
			{
				GroupParamKeyGetAuxName(aux_attname, data_attname, data_attnum);
			}
		}

		static void
		BuildAuxIndexName(char indexname[NAMEDATALEN * 2],
						  const char *auxname, IndexType indextype)
		{
			/*
			 * We believe the aux relation name is short enough, so the aux index name
			 * will never be truncated with a buffer of size NAMEDATALEN.
			 *
			 * However the compiler does not think so, gcc would raise below warning:
			 *
			 *		Footer.cc:124:39: warning: ‘_idx’ directive output may be truncated
			 *		writing 4 bytes into a region of size between 1 and 64
			 *		[-Wformat-truncation=]
			 *
			 *		  124 |   snprintf(indexname, NAMEDATALEN, "%s_idx", relname);
			 *		      |                                       ^~~~
			 *
			 * To suppress this warning we give indexname more space.
			 */
			snprintf(indexname, NAMEDATALEN * 2, "idx%d__%s", indextype, auxname);
			Assert(strlen(indexname) < NAMEDATALEN);
		}

		/*!
		 * Build an aux index.
		 *
		 * @param auxrel The aux relation.
		 * @param auxattrs The aux attrnums to build the index on.
		 * @param indextype The aux index type.
		 */
		static void
		BuildAuxIndex(::Relation auxrel, const std::vector<::AttrNumber> &auxattrs,
					  IndexType indextype)
		{
			int nattrs = auxattrs.size();
			::TupleDesc tupdesc = RelationGetDescr(auxrel);
			::List *colnames = nullptr;
			::IndexInfo *indexinfo;
			::Oid indexoid;
			::Oid collationObjectId[nattrs];
			::Oid classObjectId[nattrs];
			::int16 coloptions[nattrs];

			char indexname[NAMEDATALEN + NAMEDATALEN];
			BuildAuxIndexName(indexname, NameStr(auxrel->rd_rel->relname), indextype);

			memset(coloptions, 0, sizeof(coloptions));

			indexinfo = (IndexInfo *)newNode(sizeof(IndexInfo), T_IndexInfo);
			indexinfo->ii_NumIndexAttrs = nattrs;
			indexinfo->ii_NumIndexKeyAttrs = nattrs;
			indexinfo->ii_Expressions = nullptr;
			indexinfo->ii_ExpressionsState = nullptr;
			indexinfo->ii_Predicate = nullptr;
			indexinfo->ii_PredicateState = NULL;
			indexinfo->ii_Unique = false;
			indexinfo->ii_Concurrent = false;

			for (int i = 0; i < nattrs; ++i)
			{
				auto attrnum = auxattrs[i];
				auto attr = &tupdesc->attrs[attrnum - 1];

				indexinfo->ii_IndexAttrNumbers[i] = attrnum;
				classObjectId[i] = ::GetDefaultOpClass(attr->atttypid, BTREE_AM_OID);
				collationObjectId[i] = attr->attcollation;
				colnames = ::lappend(colnames, pstrdup(NameStr(attr->attname)));
			}

			indexoid = ::index_create(auxrel,
									  indexname,
									  InvalidOid /* indexRelationId */,
									  InvalidOid /* parentIndexRelid */,
									  InvalidOid /* parentConstraintId */,
									  InvalidOid /* relFileNode */,
									  indexinfo,
									  colnames,
									  BTREE_AM_OID,
									  auxrel->rd_rel->reltablespace,
									  collationObjectId,
									  classObjectId,
									  coloptions,
									  (::Datum)0 /* reloptions */,
									  0 /* flags */,
									  0 /* constr_flags */,
									  true /* allow_system_table_mods */,
									  true /* is_internal */,
									  NULL /* constraintId */);

			/* Unlock the index -- no one can see it anyway */
			::UnlockRelationOid(indexoid, AccessExclusiveLock);

			::list_free_deep(colnames);
		}

		static void
		BuildAuxIndexForBatch(::Relation auxrel)
		{
			IndexType indextype = IndexType::BATCH;
			int nattrs = 1; /* there is only one "batch" column */
			::TupleDesc tupdesc = RelationGetDescr(auxrel);
			::List *colnames = nullptr;
			::IndexInfo *indexinfo;
			::Oid indexoid;
			::Oid collationObjectId[nattrs];
			::Oid classObjectId[nattrs];
			::int16 coloptions[nattrs];

			char indexname[NAMEDATALEN + NAMEDATALEN];
			BuildAuxIndexName(indexname, NameStr(auxrel->rd_rel->relname), indextype);

			memset(collationObjectId, 0, sizeof(collationObjectId));
			memset(coloptions, 0, sizeof(coloptions));

			indexinfo = (IndexInfo *)newNode(sizeof(IndexInfo), T_IndexInfo);
			indexinfo->ii_NumIndexAttrs = nattrs;
			indexinfo->ii_NumIndexKeyAttrs = nattrs;
			indexinfo->ii_Expressions = nullptr;
			indexinfo->ii_ExpressionsState = nullptr;
			indexinfo->ii_Predicate = nullptr;
			indexinfo->ii_PredicateState = NULL;
			indexinfo->ii_Unique = false;
			indexinfo->ii_Concurrent = false;

			{
				::AttrNumber attrnum = Anum_mars_footer_batch;
				::Form_pg_attribute attr = &tupdesc->attrs[attrnum - 1];

				indexinfo->ii_IndexAttrNumbers[0] = attrnum;
				classObjectId[0] = ::GetDefaultOpClass(attr->atttypid, BTREE_AM_OID);
				colnames = ::lappend(colnames, pstrdup(NameStr(attr->attname)));
			}

			indexoid = ::index_create(auxrel,
									  indexname,
									  InvalidOid /* indexRelationId */,
									  InvalidOid /* parentIndexRelid */,
									  InvalidOid /* parentConstraintId */,
									  InvalidOid /* relFileNode */,
									  indexinfo,
									  colnames,
									  BTREE_AM_OID,
									  auxrel->rd_rel->reltablespace,
									  collationObjectId,
									  classObjectId,
									  coloptions,
									  (::Datum)0 /* reloptions */,
									  0 /* flags */,
									  0 /* constr_flags */,
									  true /* allow_system_table_mods */,
									  true /* is_internal */,
									  NULL /* constraintId */);

			/* Unlock the index -- no one can see it anyway */
			::UnlockRelationOid(indexoid, AccessExclusiveLock);

			::list_free_deep(colnames);
		}

		void
		CreateFooterCatalog(::Relation rel)
		{
			::Oid relid = RelationGetRelid(rel);
			::TupleDesc reldesc = RelationGetDescr(rel);

			TupleDesc tupdesc;
			Oid namespaceid;
			Oid aux_relid;
			std::string aux_relname;
			ObjectAddress baseobject;
			ObjectAddress aoauxiliaryobject;
			std::string compresstype = "";
			Version version = default_version;
			int fixed_nattrs = get_fixed_footer_nattrs(version);

			auto get_footer_rel_name = [](::Relation rel)
			{
				return std::to_string(RelationGetRelid(rel)) + "_footer";
			};

			aux_relname = get_footer_rel_name(rel);
			namespaceid = PG_AOSEGMENT_NAMESPACE;

			if (::isTempNamespace(rel->rd_rel->relnamespace))
			{
				// if the main table is temp, also should be the aux table.
				namespaceid = rel->rd_rel->relnamespace;
			}

			bool existing_relid = get_relname_relid(aux_relname.c_str(), namespaceid);

			if (!existing_relid)
			{
				std::vector<::MarsDimensionAttr> orderkeys{};
				std::vector<::MarsDimensionAttr> groupkeys{};

				/* make sure we can see the mx_mars record */
				::CommandCounterIncrement();

				/*
				 * When there are global order keys and group keys,
				 * we need to store them.
				 * Refer to README.md
				 */

				int natts = fixed_nattrs;

				if (mars_enable_index_on_auxtable)
				{
					utils::GetRelOptions(relid, &groupkeys, &orderkeys, nullptr);

					/*
					 * TODO: if a global orderkey is also a groupkey, only need to
					 * treat it as group key, this saves two aux columns.
					 */

					/* every global orderkey adds two aux columns, min and max */
					natts += orderkeys.size() * 2;
					/* every groupkey adds one aux column */
					natts += groupkeys.size();
				}

				tupdesc = ::CreateTemplateTupleDesc(natts);
				::TupleDescInitEntry(tupdesc, (AttrNumber)Anum_mars_footer_segno,
									 "segno", INT8OID, -1, 0);
				::TupleDescInitEntry(tupdesc, (AttrNumber)Anum_mars_footer_batch,
									 "batch", INT8OID, -1, 0);
				::TupleDescInitEntry(tupdesc, (AttrNumber)Anum_mars_footer_content,
									 "content", BYTEAOID, -1, 0);
				::TupleDescInitEntry(tupdesc, (AttrNumber)Anum_mars_footer_rowgroup_count,
									 "rowgroupcount", INT4OID, -1, 0);

				/* version specific attrs */
				switch (version)
				{
				case V1:
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_rowcount,
										 "rowcount", INT8ARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_null_count,
										 "null_count", BYTEAARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_sum,
										 "sum", BYTEAARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_min_at,
										 "min_at", BYTEAARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_max_at,
										 "max_at", BYTEAARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_min,
										 "min", BYTEAARRAYOID, -1, 1);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_max,
										 "max", BYTEAARRAYOID, -1, 1);
					break;

				case V2:
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_rowcount,
										 "rowcount", INT8OID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_null_count,
										 "null_count", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_sum,
										 "sum", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_min_at,
										 "min_at", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_max_at,
										 "max_at", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_min,
										 "min", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_max,
										 "max", BYTEAOID, -1, 0);
					::TupleDescInitEntry(tupdesc, Anum_mars_footer_edges,
										 "physical_edges", BYTEAOID, -1, 0);

					/*
					 * the varlen columns are 'x', extended, by default, so they
					 * can be in-place compressed.  to better cooperate with the
					 * lazy detoasting we change them to 'e', external.
					 *
					 * this is not made on V1 to keep the original V1 layout.
					 *
					 * TODO: can we force them to be always toasted?
					 */
					TupleDescAttr(tupdesc, Anum_mars_footer_content - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_rowcount - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_null_count - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_sum - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_min_at - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_max_at - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_min - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_max - 1)->attstorage = 'e';
					TupleDescAttr(tupdesc, Anum_mars_footer_edges - 1)->attstorage = 'e';

					break;
				}

				if (mars_enable_index_on_auxtable)
				{
					::AttrNumber position = fixed_nattrs;

					/* global order related preagg */
					for (auto &orderkey : orderkeys)
					{
						::Form_pg_attribute attr = &reldesc->attrs[orderkey.attnum - 1];
						::Oid atttype = attr->atttypid;
						bool is_min;
						char name[NAMEDATALEN];

						/* min[i] */
						is_min = true;
						OrderKeyGetAuxName(name, reldesc, orderkey.attnum, is_min);
						position++;
						::TupleDescInitEntry(tupdesc,
											 position,
											 name,
											 atttype,
											 -1,
											 1);

						/* max[i] */
						is_min = false;
						OrderKeyGetAuxName(name, reldesc, orderkey.attnum, is_min);
						position++;
						::TupleDescInitEntry(tupdesc,
											 position,
											 name,
											 atttype,
											 -1,
											 1);
					}

					/* grouping related preagg */
					for (auto &groupAttr : groupkeys)
					{
						char name[NAMEDATALEN];
						::AttrNumber group_key = groupAttr.attnum;
						::Form_pg_attribute attr = &reldesc->attrs[AttrNumberGetAttrOffset(group_key)];

						switch (groupAttr.dimensiontype)
						{
						case MARS_DIM_ATTR_TYPE_DEFAULT:
							GroupKeyGetAuxName(name, reldesc, group_key);
							break;
						case MARS_DIM_ATTR_TYPE_WITH_PARAM:
							GroupParamKeyGetAuxName(name, reldesc, group_key);
							break;
						default:
							elog(ERROR, "Unknown groupAttr");
						}

						position++;
						::TupleDescInitEntry(tupdesc,
											 position,
											 name,
											 attr->atttypid,
											 -1,
											 1);
					}
				}

				aux_relid = ::heap_create_with_catalog(aux_relname.c_str(),
													   namespaceid,
													   rel->rd_rel->reltablespace,
													   InvalidOid,
													   InvalidOid,
													   InvalidOid,
													   rel->rd_rel->relowner,
													   HEAP_TABLE_AM_OID,
													   tupdesc,
													   (List *)NULL,
													   RELKIND_RELATION,
													   rel->rd_rel->relpersistence,
													   rel->rd_rel->relisshared,
													   RelationIsMapped(rel),
													   ONCOMMIT_NOOP,
													   NULL, /* GP Policy */
													   InvalidOid,
													   false, /* use_user_acl */
													   true,
													   true,
													   InvalidOid,
													   NULL, /* typeaddress */
													   false /* valid_opts */);

				::AlterTableCreateToastTable(aux_relid, (Datum)0, AccessExclusiveLock);

				/* Make this table visible, else index creation will fail */
				::CommandCounterIncrement();

				::Relation auxrel = relation_open(aux_relid, ShareLock);

				/* Build the fixed aux index(es) */
				BuildAuxIndexForBatch(auxrel);

				/* Derived from CreateAOAuxiliaryTable() */
				if (mars_enable_index_on_auxtable)
				{
					auto position = AttrOffsetGetAttrNumber(fixed_nattrs);
					auto norderkeys = orderkeys.size();
					auto ngroupkeys = groupkeys.size();

					/* global order columns index */
					if (!orderkeys.empty())
					{
						std::vector<::AttrNumber> min_max_attrs;
						std::vector<::AttrNumber> max_min_attrs;

						min_max_attrs.reserve(norderkeys * 2);
						max_min_attrs.reserve(norderkeys * 2);

						for (std::size_t i = 0; i < norderkeys; ++i, position += 2)
						{
							min_max_attrs.push_back(position);
							min_max_attrs.push_back(position + 1);

							max_min_attrs.push_back(position + 1);
							max_min_attrs.push_back(position);
						}

						BuildAuxIndex(auxrel, min_max_attrs, IndexType::ORDER_KEY_MIN_FIRST);
						BuildAuxIndex(auxrel, max_min_attrs, IndexType::ORDER_KEY_MAX_FIRST);
					}

					/* grouping columns index */
					if (!groupkeys.empty())
					{
						std::vector<::AttrNumber> plain_attrs;
						std::vector<::AttrNumber> param_attrs;

						plain_attrs.reserve(ngroupkeys);
						param_attrs.reserve(ngroupkeys);

						for (std::size_t i = 0; i < ngroupkeys; ++i, ++position)
						{
							const auto &groupkey = groupkeys[i];

							switch (groupkey.dimensiontype)
							{
							case MARS_DIM_ATTR_TYPE_DEFAULT:
								plain_attrs.push_back(position);
								break;

							case MARS_DIM_ATTR_TYPE_WITH_PARAM:
								param_attrs.push_back(position);
								break;

							default:
								pg_unreachable();
							}
						}

						Assert(plain_attrs.size() + param_attrs.size() == ngroupkeys);

						/* build the param-last aux index */
						if (!plain_attrs.empty())
						{
							std::vector<::AttrNumber> attrs(ngroupkeys);

							std::copy(plain_attrs.begin(), plain_attrs.end(),
									  attrs.begin());
							std::copy(param_attrs.begin(), param_attrs.end(),
									  attrs.begin() + plain_attrs.size());

							BuildAuxIndex(auxrel, attrs, IndexType::GROUP_KEY);
						}

						/* build the param-first aux index */
						if (!param_attrs.empty())
						{
							std::vector<::AttrNumber> attrs(ngroupkeys);

							std::copy(param_attrs.begin(), param_attrs.end(),
									  attrs.begin());
							std::copy(plain_attrs.begin(), plain_attrs.end(),
									  attrs.begin() + param_attrs.size());

							BuildAuxIndex(auxrel, attrs, IndexType::GROUP_PARAM_KEY);
						}
					}
				}

				/* Unlock target table -- no one can see it */
				table_close(auxrel, ShareLock);

				::InsertInitialFastSequenceEntries(aux_relid);

				::UpdateMARSEntryAuxOids(relid, aux_relid);

				baseobject.classId = RelationRelationId;
				baseobject.objectId = relid;
				baseobject.objectSubId = 0;
				aoauxiliaryobject.classId = RelationRelationId;
				aoauxiliaryobject.objectId = aux_relid;
				aoauxiliaryobject.objectSubId = 0;

				::recordDependencyOn(&aoauxiliaryobject,
									 &baseobject, ::DEPENDENCY_INTERNAL);
			}
			else
			{
				// Seg Aux table exists if in truncate command. truncate Aux also
				auto segRelId = RelationGetFooterRelid(rel);
				auto segRel = relation_open(segRelId, AccessExclusiveLock);

				// truncate the aux relation, as well as all its associated relations,
				// such as the toast relations, indexes, etc..
				heap_truncate_one_rel(segRel);

				relation_close(segRel, NoLock);
			}
		}

		::Oid
		RelationGetFooterRelid(::Relation rel)
		{
			::Oid footerOid;

			::GetMARSEntryAuxOids(RelationGetRelid(rel), NULL, &footerOid);

			return footerOid;
		}

		static ::Oid
		GetColumnType(TupleDesc desc, ::AttrNumber attnum)
		{
			return TupleDescAttr(desc, AttrNumberGetAttrOffset(attnum))->atttypid;
		}

		static ::AttrNumber
		GetAttrNumOnAuxTableByAttrName(::TupleDesc aux_desc, char expected[NAMEDATALEN])
		{
			auto version = footer_version_from_tupdesc(aux_desc);
			auto fixed_nattrs = get_fixed_footer_nattrs(version);

			for (int i = fixed_nattrs; i < aux_desc->natts; i++)
			{
				char *actual = NameStr(TupleDescAttr(aux_desc, i)->attname);
				if (strncmp(expected, actual, NAMEDATALEN) == 0)
				{
					return TupleDescAttr(aux_desc, i)->attnum;
				}
			}

			return -1;
		}

		static ::AttrNumber
		GetAttrNumOnAuxTable(::TupleDesc data_tupdesc, ::AttrNumber data_attnum,
							 ::TupleDesc aux_tupdesc, IndexType indextype, bool is_min)
		{
			char expected[NAMEDATALEN];
			::AttrNumber aux_attrnum = InvalidAttrNumber;

			Assert(aux_index_is_dynamic(indextype));

			if (aux_index_is_on_groupkeys(indextype))
			{
				GroupKeyGetAuxName(expected, data_tupdesc, data_attnum);
				aux_attrnum = GetAttrNumOnAuxTableByAttrName(aux_tupdesc, expected);
				if (AttrNumberIsForUserDefinedAttr(aux_attrnum))
				{
					return aux_attrnum;
				}

				GroupParamKeyGetAuxName(expected, data_tupdesc, data_attnum);
				aux_attrnum = GetAttrNumOnAuxTableByAttrName(aux_tupdesc, expected);
				if (AttrNumberIsForUserDefinedAttr(aux_attrnum))
				{
					return aux_attrnum;
				}
			}
			else
			{
				OrderKeyGetAuxName(expected, data_tupdesc, data_attnum, is_min);
				aux_attrnum = GetAttrNumOnAuxTableByAttrName(aux_tupdesc, expected);
				if (AttrNumberIsForUserDefinedAttr(aux_attrnum))
				{
					return aux_attrnum;
				}
			}

			return InvalidAttrNumber;
		}

		static ::AttrNumber
		GetAttrNumberOnDataRelation(const char *attname)
		{
			::AttrNumber attnum;

			if (sscanf(attname, "attr%hd__", &attnum) != 1)
				elog(ERROR, "invalid attname: %s", attname);

			return attnum;
		}

		static ::AttrNumber
		AttrNumFromDataToAuxIndex(const ::Relation irel, ::AttrNumber data_attrnum)
		{
			auto natts = IndexRelationGetNumberOfAttributes(irel);
			auto &indkey = irel->rd_index->indkey;

			for (::AttrNumber attrnum = 1; attrnum <= natts; ++attrnum)
			{
				if (data_attrnum == indkey.values[attrnum - 1])
				{
					return attrnum;
				}
			}

			elog(ERROR, "cannot find aux index attr");
		}

		static IndexType
		GetIndexTypeByIndexName(const char *indexname)
		{
			int indextype;
			int nmatch = sscanf(indexname, "idx%d__", &indextype);

			if (nmatch != 1)
			{
				elog(ERROR, "invalid indexname: %s", indexname);
			}

			return (IndexType)indextype;
		}

		static void
		FillFooters(::MemoryContext mcxt, Footer::footers_type &footers,
					::SysScanDesc scan, const ::TupleDesc data_tupdesc,
					const ::TupleDesc aux_tupdesc)
		{
			auto version = footer_version_from_tupdesc(aux_tupdesc);
			auto oldcxt = ::MemoryContextSwitchTo(mcxt);
			::HeapTuple tuple;

			footers.clear();

			while (HeapTupleIsValid((tuple = ::systable_getnext(scan))))
				footers.emplace_back(version, mcxt, data_tupdesc, aux_tupdesc, tuple);

			::MemoryContextSwitchTo(oldcxt);
		}

		void
		FetchFooters(::MemoryContext mcxt, Footer::footers_type &footers,
					 ::Relation rel, ::Snapshot snapshot)
		{
			Oid footerOid = RelationGetFooterRelid(rel);
			Relation segRel;
			SysScanDesc scanDesc;
			TupleDesc segDesc;

			if (!OidIsValid(footerOid))
				elog(ERROR, "could not find footer table for mars table \"%s\"",
					 RelationGetRelationName(rel));

			segRel = ::relation_open(footerOid, AccessShareLock);

			segDesc = RelationGetDescr(segRel);
			scanDesc = ::systable_beginscan(segRel,
											InvalidOid, false, snapshot, 0, NULL);

			FillFooters(mcxt, footers, scanDesc, RelationGetDescr(rel), segDesc);

			systable_endscan(scanDesc);
			relation_close(segRel, AccessShareLock);
		}

		bool
		FetchFooter(::MemoryContext mcxt, Footer &footer,
					::Relation datarel, ::Snapshot snapshot,
					const ::ItemPointer auxctid)
		{
			auto oldcxt = ::MemoryContextSwitchTo(mcxt);
			auto auxrelid = RelationGetFooterRelid(datarel);
			auto auxrelname = ::get_rel_name(auxrelid);
			auto indextype = IndexType::BATCH;
			char indexrelname[NAMEDATALEN * 2];
			bool result;
			::Relation auxrel;
			::Oid indexrelid;
			::SysScanDesc scan;
			::ScanKeyData keys[1];
			::HeapTuple tuple;

			if (!auxrelname)
				goto fail_old_format;

			BuildAuxIndexName(indexrelname, auxrelname, indextype);

			indexrelid = ::get_relname_relid(indexrelname,
											 ::get_rel_namespace(auxrelid));
			if (!OidIsValid(indexrelid))
				goto fail_old_format;

			::ScanKeyInit(keys, Anum_mars_footer_batch, BTEqualStrategyNumber,
						  F_INT8EQ, ItemPointerGetBlockNumber(auxctid));

			auxrel = ::relation_open(auxrelid, AccessShareLock);

			scan = ::systable_beginscan(auxrel, indexrelid, true /* indexOK */,
										snapshot, 1 /* nkeys */, keys);

			tuple = ::systable_getnext(scan);
			result = HeapTupleIsValid(tuple);
			if (result)
				footer.Assign(RelationGetDescr(auxrel), tuple);

			::systable_endscan(scan);
			::relation_close(auxrel, AccessShareLock);
			::MemoryContextSwitchTo(oldcxt);

			return result;

		fail_old_format:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mars: relation \"%s\" was created in the old format, "
							"which does not support UPDATE/DELETE yet",
							RelationGetRelationName(datarel)),
					 errhint("Recreate it to enable UPDATE/DELETE")));
		}

		void
		Footer::StoreFooterToAux(const std::string &footer_content,
								 const RowGroupPreAgg &preagg,
								 std::vector<::MarsDimensionAttr> &groupkeys,
								 std::vector<::MarsDimensionAttr> &orderkeys,
								 std::vector<::ItemPointerData> retired)
		{
			if (impl_->auxOid_ == InvalidOid)
			{
				elog(ERROR, "StorageFooterToAux only worked in write routine, after Aux"
							"relation Oid is set");
			}

			int64 segno;
			int64 batch;
			int contentLen;
			Relation segRel;
			bytea *content_p;
			TupleDesc tupleDesc;
			HeapTuple tuple;
			// there is only 1 row group in each mars block (parquet file)
			auto nrowgroups = 1;

			segno = 0; // FIXME: no hard-coding
			batch = GetFastSequences(impl_->auxOid_, segno, 1, 1);

			contentLen = footer_content.size();
			content_p = (bytea *)::palloc(contentLen + VARHDRSZ);
			::memcpy(VARDATA(content_p), footer_content.data(), contentLen);
			SET_VARSIZE(content_p, contentLen + VARHDRSZ);

			segRel = ::relation_open(impl_->auxOid_, RowExclusiveLock);
			tupleDesc = RelationGetDescr(segRel);

			Datum values[tupleDesc->natts];
			bool isnull[tupleDesc->natts];

			values[Anum_mars_footer_segno - 1] = Int64GetDatum(segno);
			isnull[Anum_mars_footer_segno - 1] = false;
			values[Anum_mars_footer_batch - 1] = Int64GetDatum(batch);
			isnull[Anum_mars_footer_batch - 1] = false;
			values[Anum_mars_footer_content - 1] = PointerGetDatum(content_p);
			isnull[Anum_mars_footer_content - 1] = false;
			values[Anum_mars_footer_rowgroup_count - 1] = Int32GetDatum(nrowgroups);
			isnull[Anum_mars_footer_rowgroup_count - 1] = false;

			::Relation rel = relation_open(impl_->relid_, AccessShareLock);
			preagg.Pack(RelationGetDescr(rel), tupleDesc, values, isnull);
			relation_close(rel, AccessShareLock);

			/* now we store the min & max columns of the order keys */
			auto version = footer_version_from_tupdesc(tupleDesc);
			auto fixed_nattrs = get_fixed_footer_nattrs(version);
			if (tupleDesc->natts > fixed_nattrs)
			{
				auto mins = preagg.min_.data();
				auto maxs = preagg.max_.data();
				AttrNumber position = fixed_nattrs;

				/* global order min/max */
				for (size_t i = 0; i < orderkeys.size(); i++)
				{
					/* min[i] */
					values[position] =
						mins[AttrNumberGetAttrOffset(orderkeys.at(i).attnum)];
					isnull[position] = false;

					/* max[i] */
					values[position + 1] =
						maxs[AttrNumberGetAttrOffset(orderkeys.at(i).attnum)];
					isnull[position + 1] = false;

					position += 2;
				}

				/* grouping columns */
				for (size_t i = 0; i < groupkeys.size(); i++)
				{
					if (groupkeys.at(i).dimensiontype == MARS_DIM_ATTR_TYPE_DEFAULT)
					{
						values[position] = mins[AttrNumberGetAttrOffset(groupkeys.at(i).attnum)];
						isnull[position] = false;
					}
					else if (groupkeys.at(i).dimensiontype == MARS_DIM_ATTR_TYPE_WITH_PARAM)
					{
						::Datum v = mins[AttrNumberGetAttrOffset(groupkeys.at(i).attnum)];
						::Datum param = groupkeys.at(i).param;

						values[position] = ts_timestamp_bucket(param, v);
						isnull[position] = false;
					}
					position++;
				}
			}

			tuple = ::heap_form_tuple(tupleDesc, values, isnull);

			if (retired.size() == 1)
			{
				/*
				 * when there is only 1 old aux tuple to be retired, perform an update
				 * instead of insert+delete.
				 */
				::CatalogTupleUpdate(segRel, &retired.front(), tuple);
			}
			else
			{
				/*
				 * - it is a totally new aux tuple, insert directly;
				 * - otherwise, it is replacing more than one old tuples, perform
				 *   separate insert+deletes.
				 */
				::CatalogTupleInsert(segRel, tuple);

				/* multiple old aux tuples are replaced, delete them one by one */
				for (auto &tid : retired)
				{
					::CatalogTupleDelete(segRel, &tid);
				}
			}

			/* record the tid */
			impl_->tid_ = tuple->t_self;

			::heap_freetuple(tuple);
			::pfree(content_p);

			::relation_close(segRel, RowExclusiveLock);
		}

		void
		SeeLocalChanges()
		{
			::CommandCounterIncrement();
		}

		void
		GetListOfOrderKeys(::Relation rel, ::List **global_orderkeys, ::List **local_orderkeys)
		{
			::ListCell *lc;

			::List *groupkeys = NULL;
			::List *grp_param_keys = NULL;
			::List *grp_keys = NULL;
			*global_orderkeys = NULL;
			*local_orderkeys = NULL;

			::GetMARSEntryOption(RelationGetRelid(rel),
								 &groupkeys,
								 nullptr /* global_orderkeys */,
								 local_orderkeys);

			::Oid footeroid = RelationGetFooterRelid(rel);
			::Relation aux_rel = ::relation_open(footeroid, AccessShareLock);
			::List *aux_index_ids = ::RelationGetIndexList(aux_rel);

			// Compatible with old table without index
			if (::list_length(aux_index_ids) == 0)
			{
				::relation_close(aux_rel, AccessShareLock);
				::list_free_deep(groupkeys);

				return;
			}

			foreach (lc, groupkeys)
			{
				if (lfirst_node(MarsDimensionAttr, lc)->dimensiontype == MARS_DIM_ATTR_TYPE_DEFAULT)
					grp_keys = lappend(grp_keys, lfirst_node(MarsDimensionAttr, lc));

				if (lfirst_node(MarsDimensionAttr, lc)->dimensiontype == MARS_DIM_ATTR_TYPE_WITH_PARAM)
					grp_param_keys = lappend(grp_param_keys, lfirst_node(MarsDimensionAttr, lc));
			}

			if (grp_keys)
			{
				*global_orderkeys = lappend(*global_orderkeys, list_concat(list_copy(grp_keys), list_copy(grp_param_keys)));
			}

			if (grp_param_keys)
			{
				*global_orderkeys = lappend(*global_orderkeys, list_concat(list_copy(grp_param_keys), list_copy(grp_keys)));
			}

			list_free(grp_keys);
			list_free(grp_param_keys);
			list_free(groupkeys);

			::relation_close(aux_rel, AccessShareLock);
		}

		static ::Oid
		GetAuxIndexOfGroupKeys(::Relation auxrel)
		{
			::Oid result = InvalidOid;
			::List *indexoids = ::RelationGetIndexList(auxrel);
			::ListCell *lc;

			foreach (lc, indexoids)
			{
				auto indexoid = lfirst_oid(lc);
				auto indexname = ::get_rel_name(indexoid);

				auto indextype = GetIndexTypeByIndexName(indexname);
				::pfree(indexname);

				if (indextype == IndexType::GROUP_KEY ||
					indextype == IndexType::GROUP_PARAM_KEY)
				{
					result = indexoid;
					break;
				}
			}

			::list_free(indexoids);
			return result;
		}

		static const std::shared_ptr<mars::ScanKey>
		GetScankeyByAttNum(const std::vector<std::shared_ptr<mars::ScanKey>> scankeys,
						   ::AttrNumber attnum)
		{
			for (const auto &scankey : scankeys)
			{
				if (scankey->scankey_.sk_attno == attnum)
				{
					return scankey;
				}
			}

			return nullptr;
		}

		static bool
		NotBestMatch(IndexType indextype, ::StrategyNumber strategy)
		{
			if (indextype == IndexType::ORDER_KEY_MIN_FIRST && strategy > mars::EQ)
				// when there is a min-first aux index, there must also be a max-first
				// one, the max-first one is more efficient on ">=" conditions, so
				// leave the chance to it.
				return true;

			if (indextype == IndexType::ORDER_KEY_MAX_FIRST && strategy < mars::EQ)
				// similar as above.
				return true;

			return false;
		}

		std::pair<::Oid, std::vector<::AttrNumber>>
		DecideAuxIndex(::Relation rel, const std::vector<AttrNumber> &orderkeys,
					   const std::vector<std::shared_ptr<mars::ScanKey>> &scankeys,
					   const std::unordered_set<::AttrNumber> &equalkeys)
		{
			::Oid footeroid = RelationGetFooterRelid(rel);
			::Relation aux_rel = ::relation_open(footeroid, AccessShareLock);
			::List *aux_index_oids = ::RelationGetIndexList(aux_rel);
			::relation_close(aux_rel, NoLock);

			::ListCell *lc;
			foreach (lc, aux_index_oids)
			{
				auto indexoid = lfirst_oid(lc);
				auto indexrel = ::index_open(indexoid, AccessShareLock);
				auto indexdesc = RelationGetDescr(indexrel);
				auto indexname = NameStr(indexrel->rd_rel->relname);
				auto indextype = GetIndexTypeByIndexName(indexname);

				if (!aux_index_is_dynamic(indextype))
				{
					/* not a dynamic aux index */
					::index_close(indexrel, NoLock);
					continue;
				}

				Assert(indexdesc->natts > 0);

				// collect the aux index keys, note, they are the keys on the data rel,
				// not on the aux index itself.
				std::vector<::AttrNumber> indexkeys;  // index order w/o "=" keys
				std::vector<::AttrNumber> indexorder; // full index order
				indexkeys.reserve(indexdesc->natts);
				indexorder.reserve(indexdesc->natts);
				for (int i = 0; i < indexdesc->natts; ++i)
				{
					auto attname = NameStr(indexdesc->attrs[i].attname);
					auto attnum = GetAttrNumberOnDataRelation(attname);

					if (indexorder.empty() || indexorder.back() != attnum)
						// the min/max keys are merged into one indexkey
						indexorder.push_back(attnum);

					auto is_equalkey = equalkeys.find(attnum) != equalkeys.end();
					if (is_equalkey)
						// ignore the "=" keys
						continue;

					if (indexkeys.empty() || indexkeys.back() != attnum)
						// the min/max keys are merged into one indexkey
						indexkeys.push_back(attnum);
				}

				::index_close(indexrel, NoLock);

				auto scankey = GetScankeyByAttNum(scankeys, indexorder.front());
				if (scankey && NotBestMatch(indextype, scankey->scankey_.sk_strategy))
				{
					// we believe there can be a more efficient aux index than this.
					continue;
				}
				else if (!orderkeys.empty())
				{
					// the indexkeys and orderkeys are incompatible iff they have
					// diversion, so we can use an aux index as long as its indexkeys
					// is the prefix of the orderkeys, or the reverse.
					auto mismatch = std::mismatch(orderkeys.begin(), orderkeys.end(),
												  indexkeys.begin());
					if (mismatch.first != orderkeys.end() &&
						mismatch.second != indexkeys.end())
						// this aux index is incompatible due to diversion
						continue;
				}
				else if (scankeys.empty())
				{
					// there are no orderkey and no scankey, any aux index can be used,
					// so the current one is good.
				}
				else if (!scankey)
				{
					// when there are no orderkey we could use any aux index that have
					// matching scankeys on the primary indexkey, so the current one
					// cannot be used because it does not.
					continue;
				}

				// a useable aux index is found
				return std::make_pair(indexoid, std::move(indexorder));
			}

			// no suitable aux index
			return std::make_pair(InvalidOid, std::vector<::AttrNumber>());
		}

		AuxScan::AuxScan(::MemoryContext mcxt, ::Relation datarel, ::Snapshot snapshot)
			: mcxt_(mcxt), datarel_(datarel), snapshot_(snapshot), auxrel_(OpenAuxRel()), indexrel_(nullptr), scan_(nullptr), tuple_(nullptr), footer_(footer_version_from_tupdesc(RelationGetDescr(auxrel_)),
																																					   mcxt, RelationGetDescr(datarel), nullptr, nullptr),
			  scankeys_()
		{
		}

		AuxScan::~AuxScan()
		{
			EndScan();

			::relation_close(auxrel_, AccessShareLock);
		}

		void
		AuxScan::BeginScan(::Oid auxindexoid,
						   const std::vector<std::shared_ptr<mars::ScanKey>> &data_scankeys)
		{
			auto oldcxt = ::MemoryContextSwitchTo(mcxt_);

			indexrel_ = ::index_open(auxindexoid, AccessShareLock);

			ConvertScanKeys(data_scankeys);

			scan_ = ::systable_beginscan_ordered(auxrel_, indexrel_, snapshot_,
												 scankeys_.size(), scankeys_.data());

			::MemoryContextSwitchTo(oldcxt);
		}

		bool
		AuxScan::GetNext(::ScanDirection scandir, bool load)
		{
			Assert(scan_);

			auto oldcxt = ::MemoryContextSwitchTo(mcxt_);
			tuple_ = ::systable_getnext_ordered(scan_, scandir);
			::MemoryContextSwitchTo(oldcxt);

			if (HeapTupleIsValid(tuple_))
			{
				if (load)
				{
					footer_.Assign(RelationGetDescr(auxrel_), tuple_);
				}

				return true;
			}
			else
			{
				return false;
			}
		}

		void
		AuxScan::EndScan()
		{
			tuple_ = nullptr;

			if (scan_)
			{
				::systable_endscan_ordered(scan_);
				scan_ = nullptr;
			}

			if (indexrel_)
			{
				::index_close(indexrel_, AccessShareLock);
				indexrel_ = nullptr;
			}
		}

		::Relation
		AuxScan::OpenAuxRel()
		{
			auto oldcxt = ::MemoryContextSwitchTo(mcxt_);

			auto auxrelid = RelationGetFooterRelid(datarel_);
			auto auxrel = ::relation_open(auxrelid, AccessShareLock);

			::MemoryContextSwitchTo(oldcxt);

			return auxrel;
		}

		void
		AuxScan::BuildScanKey(const ::ScanKeyData *data_scankey,
							  ::StrategyNumber strategy, ::Datum argument,
							  IndexType indextype, bool is_min)
		{
			auto data_tupdesc = RelationGetDescr(datarel_);
			auto aux_tupdesc = RelationGetDescr(auxrel_);
			auto attrnum = GetAttrNumOnAuxTable(data_tupdesc, data_scankey->sk_attno,
												aux_tupdesc, indextype, is_min);

			if (!AttrNumberIsForUserDefinedAttr(attrnum))
			{
				// no match attr in the auxrel
				return;
			}

			auto lefttype = GetColumnType(aux_tupdesc, attrnum);
			auto righttype = data_scankey->sk_subtype;
			auto opcode = CompareBase::GetCompareOpcode(strategy, lefttype, righttype);

			scankeys_.emplace_back();
			auto &aux_scankey = scankeys_.back();

			::ScanKeyEntryInitialize(&aux_scankey,
									 data_scankey->sk_flags,
									 attrnum,
									 strategy,
									 data_scankey->sk_subtype,
									 data_scankey->sk_collation,
									 opcode,
									 argument);
		}

		void
		AuxScan::ConvertScanKey(const ::ScanKey data_scankey,
								IndexType indextype, int64 interval)
		{
			auto strategy = data_scankey->sk_strategy;
			auto argument = data_scankey->sk_argument;

			Assert(aux_index_is_dynamic(indextype));

			if (aux_index_is_on_groupkeys(indextype) && interval > 0)
			{
				argument = ts_timestamp_bucket(interval, data_scankey->sk_argument);

				// to include current block, we need to adjust strategy
				if (strategy == BTLessStrategyNumber)
				{
					strategy = BTLessEqualStrategyNumber;
				}
				else if (strategy == BTGreaterStrategyNumber)
				{
					strategy = BTGreaterEqualStrategyNumber;
				}
			}

			if (strategy == BTEqualStrategyNumber)
			{
				BuildScanKey(data_scankey, BTLessEqualStrategyNumber, argument,
							 indextype, true /* is_min */);
				BuildScanKey(data_scankey, BTGreaterEqualStrategyNumber, argument,
							 indextype, false /* is_min */);
			}
			else
			{
				bool is_min = strategy < BTEqualStrategyNumber;

				BuildScanKey(data_scankey, strategy, argument, indextype, is_min);
			}
		}

		void
		AuxScan::ConvertScanKeys(const std::vector<std::shared_ptr<mars::ScanKey>> &data_scankeys)
		{
			// extract some info about group key
			auto indexname = RelationGetRelationName(indexrel_);
			auto indextype = GetIndexTypeByIndexName(indexname);
			std::vector<::MarsDimensionAttr> groupkeys;
			const MarsDimensionAttr *param_group = nullptr;

			Assert(aux_index_is_dynamic(indextype));

			if (aux_index_is_on_groupkeys(indextype))
			{
				utils::GetRelOptions(RelationGetRelid(datarel_),
									 &groupkeys, nullptr, nullptr);

				for (auto &groupkey : groupkeys)
				{
					if (groupkey.dimensiontype == MARS_DIM_ATTR_TYPE_WITH_PARAM)
					{
						param_group = &groupkey;
						break;
					}
				}
			}

			scankeys_.clear();
			scankeys_.reserve(data_scankeys.size() * 2);

			for (auto &key : data_scankeys)
			{
				auto data_scankey = &key->scankey_;
				int64 interval = -1;

				if (param_group && data_scankey->sk_attno == param_group->attnum)
				{
					interval = DatumGetInt64(param_group->param);
				}

				ConvertScanKey(data_scankey, indextype, interval);
			}

			// systable_getnext() requires scankeys to be sorted in indexkey order.
			std::sort(scankeys_.begin(), scankeys_.end(),
					  [this](const ::ScanKeyData &a, const ::ScanKeyData &b) -> bool
					  {
						  return (AttrNumFromDataToAuxIndex(indexrel_, a.sk_attno) <
								  AttrNumFromDataToAuxIndex(indexrel_, b.sk_attno));
					  });
		}

		AuxFetch::AuxFetch(::LOCKMODE lockmode)
			: auxrel_(nullptr), auxindexrel_(nullptr), aux_rel_lockmode_(lockmode), scan_(nullptr), tuple_(nullptr)
		{
		}

		AuxFetch::~AuxFetch()
		{
			CloseIndex();
		}

		bool
		AuxFetch::OpenIndexForFullGroupKeys(::Oid datarelid, const ::Snapshot snapshot)
		{
			::Oid auxrelid;
			::GetMARSEntryAuxOids(datarelid, NULL, &auxrelid);

			snapshot_ = snapshot;

			auxrel_ = ::relation_open(auxrelid, aux_rel_lockmode_);
			auto auxtupdesc = RelationGetDescr(auxrel_);

			// open the groupkey index
			{
				auto auxindexoid = GetAuxIndexOfGroupKeys(auxrel_);
				if (!OidIsValid(auxindexoid))
				{
					CloseIndex();
					return false;
				}

				auxindexrel_ = index_open(auxindexoid, AccessShareLock);
			}

			// build the map for aux => data attnum translation according to the index
			{
				trans_.resize(auxtupdesc->natts + 1, InvalidOid);

				auto indnkeys = IndexRelationGetNumberOfAttributes(auxindexrel_);
				auto &indkeys = auxindexrel_->rd_index->indkey;
				for (int16 i = 0; i < indnkeys; ++i)
				{
					::AttrNumber aux_attnum = indkeys.values[i];
					const auto aux_attr = &auxtupdesc->attrs[aux_attnum - 1];
					const auto aux_attname = NameStr(aux_attr->attname);
					auto data_attnum = GetAttrNumberOnDataRelation(aux_attname);

					trans_.at(aux_attnum) = data_attnum;
				}
			}

			// build the scankeys template
			{
				auto indnkeys = IndexRelationGetNumberOfAttributes(auxindexrel_);
				scankeys_.resize(indnkeys);

				// on a groupkey aux index, the scankey can be "col = time_bucket(val)"
				::StrategyNumber strategy = BTEqualStrategyNumber;
				auto &indkeys = auxindexrel_->rd_index->indkey;
				for (int16 i = 0; i < indnkeys; ++i)
				{
					::AttrNumber aux_attnum = indkeys.values[i];
					auto type = auxtupdesc->attrs[aux_attnum - 1].atttypid;
					auto opcode = CompareBase::GetCompareOpcode(strategy, type, type);

					::ScanKeyInit(&scankeys_.at(i), aux_attnum, strategy, opcode, 0);
				}
			}

			// extract the groupkey intervals
			{
				std::vector<::MarsDimensionAttr> groupkeys;
				utils::GetRelOptions(datarelid, &groupkeys, nullptr, nullptr);

				intervals_.resize(RelationGetDescr(auxrel_)->natts, 0);
				for (auto &groupkey : groupkeys)
				{
					auto data_attnum = groupkey.attnum;
					char *data_attname = ::get_attname(datarelid, data_attnum,
													   false /* missing_ok */);

					char aux_attname[NAMEDATALEN];
					GroupKeyGetAuxName(aux_attname, data_attname, &groupkey);
					::pfree(data_attname);

					auto aux_attnum = ::get_attnum(auxrelid, aux_attname);
					Assert(AttrNumberIsForUserDefinedAttr(aux_attnum));
					intervals_.at(aux_attnum - 1) = groupkey.param;

					if (groupkey.dimensiontype == MARS_DIM_ATTR_TYPE_WITH_PARAM)
						range_group_key_.push_back(groupkey.attnum);
					else
						single_value_group_key_.push_back(groupkey.attnum);
				}
			}

			return true;
		}

		void
		AuxFetch::CloseIndex()
		{
			EndFetch();

			if (auxindexrel_)
			{
				index_close(auxindexrel_, NoLock);
				auxindexrel_ = nullptr;
			}

			if (auxrel_)
			{
				::relation_close(auxrel_, NoLock);
				auxrel_ = nullptr;
			}

			snapshot_ = nullptr;

			{
				auto tmp(std::move(scankeys_));
			}
			{
				auto tmp(std::move(intervals_));
			}
			{
				auto tmp(std::move(trans_));
			}
		}

		void
		AuxFetch::BeginFetch(const ::TupleTableSlot *slot)
		{
			for (auto &scankey : scankeys_)
			{
				auto aux_attnum = scankey.sk_attno; // aux column

				auto data_attnum = trans_.at(aux_attnum);
				Assert(AttrNumberIsForUserDefinedAttr(data_attnum));

				auto arg = slot->tts_values[data_attnum - 1];
				Assert(slot->tts_isnull[data_attnum - 1] == false);

				auto interval = intervals_.at(aux_attnum - 1);
				if (interval > 0)
				{
					// align to time bucket.
					//
					// unlike ConvertScanKey(), we don't need to adjust the strategy,
					// as explained in OpenIndexForFullGroupKeys(), the scankey is
					// always "col = time_bucket(val)".
					arg -= arg % interval;
				}

				scankey.sk_argument = arg;
			}

			// must copy the scankeys as systable_beginscan() do the attnum translation
			// in-place.
			std::vector<::ScanKeyData> scankeys(scankeys_);

			auto auxindexoid = RelationGetRelid(auxindexrel_);
			scan_ = ::systable_beginscan(auxrel_, auxindexoid, true /* indexOK */,
										 snapshot_, scankeys.size(), scankeys.data());
		}

		bool
		AuxFetch::FetchNext()
		{
			Assert(scan_);

			tuple_ = ::systable_getnext(scan_);

			return HeapTupleIsValid(tuple_);
		}

		void
		AuxFetch::EndFetch()
		{
			tuple_ = nullptr;

			if (scan_)
			{
				::systable_endscan(scan_);
				scan_ = nullptr;
			}
		}

		void
		AuxFetch::GetFooter(Footer &footer) const
		{
			auto auxtupdesc = RelationGetDescr(auxrel_);
			footer.Assign(auxtupdesc, tuple_);
		}

		std::vector<::AttrNumber>
		AuxFetch::ReduceMergeKey(const std::vector<::AttrNumber> &mergeby)
		{
			std::vector<::AttrNumber> ret;

			for (auto attr : mergeby)
			{
				if (std::find(range_group_key_.begin(), range_group_key_.end(), attr) != range_group_key_.end())
				{
					ret.push_back(attr);
				}
				else if (std::find(single_value_group_key_.begin(), single_value_group_key_.end(), attr) == single_value_group_key_.end())
				{
					elog(ERROR, "Attr %d is not MARS group key, does not support INSERT ON CONFLICT.", attr);
				}
			}

			if (ret.size() > 1)
			{
				elog(ERROR, "So far MARS does not support multi merge key.");
			}

			return ret;
		}

		void
		RowGroupPreAgg::Push(const PreAgg &preagg)
		{
			min_.push_back(preagg.min_);
			max_.push_back(preagg.max_);
			sum_.push_back(preagg.sum_);
			null_count_.push_back(preagg.null_count_);
			min_at_.push_back(preagg.min_at_);
			max_at_.push_back(preagg.max_at_);
			edges_.push_back(preagg.first_);
			edges_.push_back(preagg.last_);
		}

		void
		RowGroupPreAgg::PackV1(const ::TupleDesc data_tupdesc,
							   const ::TupleDesc aux_tupdesc,
							   ::Datum *values, bool *isnull) const
		{
			// there is only 1 row group in each mars block (parquet file)
			auto rowcount = EncodeRowCount();
			auto nrowgroups = 1;
			auto count_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto null_count_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto sum_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto min_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto max_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto min_at_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);
			auto max_at_array = (::Datum *)::palloc(sizeof(::Datum) * nrowgroups);

			min_array[0] = package_varlen_vector(min_, data_tupdesc);
			max_array[0] = package_varlen_vector(max_, data_tupdesc);
			sum_array[0] = package_datum128_vector(sum_);
			count_array[0] = package_int64(rowcount);
			null_count_array[0] = package_int64_vector(null_count_);
			min_at_array[0] = package_int32_vector(min_at_);
			max_at_array[0] = package_int32_vector(max_at_);

			::ArrayType *array;
			::AttrNumber attrnum;

			attrnum = Anum_mars_footer_rowcount;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == INT8ARRAYOID);
			array = construct_array(count_array, nrowgroups, INT8OID, 8, true, 'd');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_null_count;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(null_count_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_sum;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(sum_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_min_at;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(min_at_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_max_at;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(max_at_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_min;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(min_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_max;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAARRAYOID);
			array = construct_array(max_array, nrowgroups, BYTEAOID, -1, false, 'i');
			values[attrnum - 1] = PointerGetDatum(array);
			isnull[attrnum - 1] = false;
		}

		void
		RowGroupPreAgg::PackV2(const ::TupleDesc data_tupdesc,
							   const ::TupleDesc aux_tupdesc,
							   ::Datum *values, bool *isnull) const
		{
			auto rowcount = EncodeRowCount();
			::AttrNumber attrnum;

			// convert min@/max@ from int32 array to int64 array.
			//
			// FIXME: can we prevent this?
			std::vector<int64> min_at(min_at_.begin(), min_at_.end());
			std::vector<int64> max_at(max_at_.begin(), max_at_.end());

			attrnum = Anum_mars_footer_rowcount;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == INT8OID);
			values[attrnum - 1] = Int64GetDatum(rowcount);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_null_count;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_int64_vector(null_count_);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_sum;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_datum128_vector(sum_);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_min_at;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_int64_vector(min_at);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_max_at;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_int64_vector(max_at);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_min;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_varlen_vector(min_, data_tupdesc);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_max;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_varlen_vector(max_, data_tupdesc);
			isnull[attrnum - 1] = false;

			attrnum = Anum_mars_footer_edges;
			Assert(aux_tupdesc->attrs[attrnum - 1].atttypid == BYTEAOID);
			values[attrnum - 1] = package_datum_vector(edges_);
			isnull[attrnum - 1] = false;
		}

	} // namespace footer

} // namespace mars
