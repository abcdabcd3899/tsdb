/*
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbtargeteddispatchfastpath.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_operator.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "libpq/libpq.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "utils/fmgrprotos.h"
#include "utils/syscache.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbpq.h"
#include "cdb/cdbvars.h"

#include "cdb/cdbtargeteddispatchfastpath.h"

extern bool Test_print_direct_dispatch_info;

typedef struct DDPredictContext
{
	List		*contentIds;
	bool		disableDD;
	CdbHash 	*h;
	Value		*distColumnName;
	Oid			distColumnType;
} DDPredictContext;

static List * SingleDirectDispatchPredictFromParseTree(RawStmt *parsetree);
static void PerformSingleDirectDispatchFashPath(int contentId, const char *query_string);

/*
 * Function to check whether the WHERE clause can allow a
 * fast path for single direct dispatch. We only consider
 * cases like "a op 1 [and b op 1]"
 */
static bool
DirectDispatchFastPathPredict(Node *node, DDPredictContext *context)
{
	unsigned int hashCode;

	if (context->disableDD || node == NULL)
		return false;

	if (IsA(node, A_Expr))
	{
		Node		*leftop;
		Node		*rightop;
		const char	*opname;
		A_Expr		*a;
		ColumnRef	*columnRef;
		Node		*otherExpr;
		A_Const		*constExpr;
		Const		*constNode;
		Node		*colname;
		Form_pg_operator opform;
		Operator	tup;
		Oid			actual_arg_types[2];
		Oid			declared_arg_types[2];

		a = (A_Expr *) node;

		if (a->kind != AEXPR_OP)
		{
			context->disableDD = true;
			return false;
		}

		leftop = a->lexpr;
		rightop = a->rexpr;

		if (IsA(leftop, SubLink) ||
			IsA(leftop, RowExpr) ||
			IsA(rightop, SubLink) ||
			IsA(rightop, RowExpr))
		{
			context->disableDD = true;
			return false;
		}

		opname = strVal(linitial(a->name));

		if (strcmp(opname, "="))
			return false;

		if (IsA(rightop, A_Const))
		{
			otherExpr = leftop;
			constExpr = (A_Const *) rightop;
		}
		else if (IsA(leftop, A_Const))
		{
			otherExpr = rightop;
			constExpr = (A_Const *) leftop;
		}
		else
		{
			return false;
		}

		/* only care about column ref */
		if (!IsA(otherExpr, ColumnRef))
			return false;

		columnRef = (ColumnRef *)otherExpr;
		colname = llast(columnRef->fields);

		/* match the distributed key ? */
		if (!equal(colname, context->distColumnName))
			return false;

		/* try to calculate the target segment content id based on the constExpr */
		constNode = make_const(NULL, &constExpr->val, constExpr->location);

		if (context->distColumnType != constNode->consttype)
		{
			tup = oper(NULL, a->name, context->distColumnType, constNode->consttype, false, a->location);
			opform = (Form_pg_operator) GETSTRUCT(tup);

			actual_arg_types[0] = context->distColumnType;
			actual_arg_types[1] = constNode->consttype;
			declared_arg_types[0] = opform->oprleft;
			declared_arg_types[1] = opform->oprright;

			/* if the Column Var need to cast, we can not cast here */
			if (actual_arg_types[0] != declared_arg_types[0])
			{
				ReleaseSysCache(tup);
				return false;
			}

			enforce_generic_type_consistency(actual_arg_types,
											 declared_arg_types,
											 2,
											 opform->oprresult,
											 false);
			ReleaseSysCache(tup);

			if (actual_arg_types[1] != declared_arg_types[1])
			{
				constNode = (Const *)
					coerce_type(NULL,
								(Node *) constNode,
								actual_arg_types[1],
								declared_arg_types[1],
							   	-1, COERCION_IMPLICIT,
								COERCE_IMPLICIT_CAST, -1);
			}

			if (!IsA(constNode, Const))
				return false;
		}

		/* caculate the content Id */
		cdbhashinit(context->h);

		cdbhash(context->h, 1, constNode->constvalue, constNode->constisnull);

		hashCode = cdbhashreduce(context->h);

		if (list_member_int(context->contentIds, hashCode))
			return false;

		context->contentIds = lappend_int(context->contentIds, hashCode);

		return false;
	}
	else if (IsA(node, BoolExpr) &&
			((const BoolExpr *) node)->boolop == AND_EXPR)
	{
		/* follow to the args */
		(void) raw_expression_tree_walker((Node *) ((const BoolExpr *) node)->args,
										  DirectDispatchFastPathPredict,
										  (void *) context);
		return false;
	}

	context->disableDD = true;

	return false;
}

/*
 * A quick predict of direct dispatch from the parse tree
 */
static List *
SingleDirectDispatchPredictFromParseTree(RawStmt *parsetree)
{
	List		*result = NIL;
	Node		*stmt;
	Node		*node;
	Oid			relid;
	Relation 	rel = NULL;
	SelectStmt	*select;
	DDPredictContext context;
	Form_pg_attribute attr;
	ListCell	*lc;

	stmt = parsetree->stmt;

	if (!stmt)
		goto unknown_case;

	switch (nodeTag(stmt))
	{
		case T_SelectStmt:
			select = (SelectStmt *)stmt;

			/* Bellow cases are too complex for fast path */
			if (select->intoClause ||
				select->withClause ||
				select->scatterClause ||
				select->lockingClause ||
				select->havingClause)
				goto unknown_case;

			/* Might be VALUES or SETOPs or no qual clause */
			if (!select->whereClause || !select->fromClause)
				goto unknown_case;

			/* We only consider the simplest case */
			if (list_length(select->fromClause) > 1)
				goto unknown_case;

			node = linitial(select->fromClause);

			/* We only consider simple FROM RELATION case */
			if (!IsA(node, RangeVar))
				goto unknown_case;

			/*
			 * The existence of the relation is not a strong
			 * requirement, so NoLock and missingOK is used
			 * here
			 */
			relid = RangeVarGetRelid((RangeVar*) node, AccessShareLock, true);

			/* It might be a CTE, tuplestore or dropped relation */
			if (!OidIsValid(relid))
				goto unknown_case;

			/* Check targetlist */
			foreach (lc, select->targetList)
			{
				ResTarget *res = (ResTarget *) lfirst(lc);

				if (!IsA(res->val, ColumnRef) &&
					!IsA(res->val, A_Const) &&
					!IsA(res->val, FuncCall))
					goto unknown_case;
			}

			/* get the GpPolicy of the relation */
			rel = try_table_open(relid, NoLock, true);

			if (!rel)
				goto unknown_case;

			/* cannot predict direct dispatch without GpPolicy */
			if (!rel->rd_cdbpolicy ||
				rel->rd_cdbpolicy->nattrs != 1)
				goto unknown_case;

			attr = TupleDescAttr(RelationGetDescr(rel), rel->rd_cdbpolicy->attrs[0] - 1);

			context.disableDD = false;
			context.contentIds = NIL;
			context.distColumnName = makeString(pstrdup(NameStr(attr->attname)));
			context.distColumnType = attr->atttypid;
			context.h = makeCdbHashForRelation(rel);
			DirectDispatchFastPathPredict(select->whereClause, &context);
			result = context.disableDD ? NIL : context.contentIds;

			table_close(rel, AccessShareLock);
			break;
		case T_InsertStmt: /* TODO: Insert/Delete/Update */
		case T_DeleteStmt:
		case T_UpdateStmt:
		default:
			goto unknown_case;
			break;
	}

	return result;

unknown_case:
	if (rel)
		table_close(rel, NoLock);

	return result;
}

static void
PerformSingleDirectDispatchFashPath(int contentId, const char *query_string)
{
	SegmentDatabaseDescriptor *segdbDesc;
	PGconn		*conn;
	char		id;
	char		*queryText;
	int			queryTextLength;
	int			msgLength;
	int			avail;

	segdbDesc =
		cdbcomponent_allocateIdleQE(contentId, SEGMENTTYPE_EXPLICT_WRITER);

	if (!segdbDesc->conn ||
		cdbconn_isBadConnection(segdbDesc))
	{
		/* connect to new segment */
		bool		ret;
		char		gpqeid[100];
		char	   *options;

		/*
		 * Build the connection string.  Writer-ness needs to be processed
		 * early enough now some locks are taken before command line
		 * options are recognized.
		 */
		ret = build_gpqeid_param(gpqeid, sizeof(gpqeid),
								 segdbDesc->isWriter,
								 segdbDesc->identifier,
								 segdbDesc->segment_database_info->hostSegs,
								 getgpsegmentCount());

		if (!ret)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("failed to construct connectionstring")));

		options = makeOptions();

		/* start connection in blocking way */
		cdbconn_doConnectStart(segdbDesc, gpqeid, options, false);

		cdbconn_doConnectComplete(segdbDesc);

		if (!segdbDesc->conn)
			elog(ERROR, "cannot allocate a QE for direct distpatch");
	}

	conn = segdbDesc->conn;

	queryText = CdbCreateMPPQueryText(query_string, DF_WITH_SNAPSHOT, &queryTextLength);

	if (PQsendGpQuery_shared(conn, queryText, queryTextLength, false) == 0)
	{
		char	   *msg = PQerrorMessage(conn);

		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Command could not be dispatched: %s", msg ? msg : "unknown error")));
	}

	if (Test_print_direct_dispatch_info)
		elog(INFO, "Dispatch command to single content %d", contentId);

	/* wait the result and forword it to QD's client */
	while (true)
	{
		int		n;

		n = pqReadData(conn);

		/* successfully load something */
		if (n == 1)
		{
			/* consume datas until no data available */
			while (true)
			{
				conn->inCursor = conn->inStart;
				if (pqGetc(&id, conn))
					break;
				if (pqGetInt(&msgLength, 4, conn))
					break;

				msgLength -= 4;

				avail = conn->inEnd - conn->inCursor;

				/* make sure we have already received the whole message */
				if (avail < msgLength)
				{
					if (pqCheckInBufferSpace(conn->inCursor + (size_t) msgLength,
											 conn))
						elog(ERROR, "failed to enlarge PQ buffer");
					break;
				}

				/*
				 * we only care IDLE message which means the QE is ready
				 * for next query
				 */
				if (id == 'Z')
				{
					/* empty the connection buffer */
					conn->inStart = conn->inEnd;
					conn->asyncStatus = PGASYNC_IDLE;
					break;
				}
				else if (id == 'k' || id == 'x')
				{
					/*
					 * ignore mpp-specified message and
					 * donot redirect it
					 */
					conn->inStart += (1 + 4 + msgLength);
				}
				else
				{
					pq_putmessage(id, conn->inBuffer + conn->inStart + 1 + 4, msgLength);
					conn->inStart += (1 + 4 + msgLength);
				}
			}

			/* don't wait, try to read more */
			continue;
		}
		else if (n == 0)
		{
			/* if QE is ready for query, break */
			if (conn->asyncStatus == PGASYNC_IDLE)
				break;
		}
		else
			elog(ERROR, "direct distpatch QE close connections unexpected");

		/* wait some data to become available */
		if (pqWait(true, false, conn) == -1)
			elog(ERROR, "failed to wait direct distpatch QE connections unexpected");
	}

	cdbcomponent_recycleIdleQE(segdbDesc, false);
}

/*
 * Try fast path for single segment direct dispatch
 * query, QD dispatch the raw query to the QE and
 * receive the results through libpq and then directly
 * redirect the results to the client.
 */
bool
TrySingleDirectDispatchFashPath(List *parsetree_list, const char *query_string)
{
	List    *contentIds;
	RawStmt	*parsetree;

	if (Gp_role != GP_ROLE_DISPATCH ||
		!parsetree_list ||
		list_length(parsetree_list) != 1)
		return false;

	/*
	 * If we are in an aborted transaction, reject all commands
	 */
	if (IsAbortedTransactionBlockState())
		return false;

	parsetree = linitial_node(RawStmt, parsetree_list);

	contentIds = SingleDirectDispatchPredictFromParseTree(parsetree);

	if (list_length(contentIds) != 1)
		return false;

	PerformSingleDirectDispatchFashPath(linitial_int(contentIds) , query_string);

	return true;
}
