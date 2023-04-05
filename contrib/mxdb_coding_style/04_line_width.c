/*-------------------------------------------------------------------------
 *
 * 04_line_width.c
 *	  The Coding Style In MatrixDB - Line Width.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/04_line_width.c
 *
 * NOTES
 *	  Line width is documented in PostgreSQL Coding Conventions [1]:
 *
 *	  > Limit line lengths so that the code is readable in an 80-column window.
 *	  > (This doesn't mean that you must never go past 80 columns.  For
 *	  > instance, breaking a long error message string in arbitrary places just
 *	  > to keep the code within 80 columns is probably not a net gain in
 *	  > readability.)
 *
 *	  VIM TIPS: you could set ":set cc=80" to show a guard line at column 80.
 *	  And ":set nowrap" to not wrapping lines, so if yourself find it hard to
 *	  read a line, it must have been too long already.
 *
 *	  Lone lines should be broken into short lines to fit the 80-column window,
 *	  this is easy for comments, but needs more attention for other code
 *	  blocks.
 *
 *	  VIM TIPS: for comments, you could try the auto (re-)breaking by first
 *	  selecting the lines with "V", and then press "gq".
 *
 *	  For expressions, we usually leave the operator at end of the first line,
 *	  instead of beginning of the second line.
 *
 *	  TIPS: reminder, do not introduce white spaces at end of line when
 *	  breaking a line.
 *
 *	  [1]: https://www.postgresql.org/docs/12/source.html
 *
 *-------------------------------------------------------------------------
 */

/*
 * GOOD: Break long comments is easy, we can break at any position, but usually
 * we do not break words.  Also keep in mind, limiting line width to 80 does
 * not mean that the shorter the better, typically we should break only when it
 * is about to exceed the 80 column margin.
 *
 * It is also good practice to break long comments into multiple paragraphs.
 */

/*
 * BAD: Do not break
 * too early, this
 * is hard for other
 * guys to follow
 * when hacking your
 * code.
 */

/*
 * BAD: Too wide.  PostgreSQL does allow lines longer than 80, but that is only for cases that breaking the line would make the code hard to read.
 */

/*
 * GOOD: A function definition is usually broke at the argument list, and
 * usually we keep an argument the same line with its type.
 */
List *
build_index_pathkeys(PlannerInfo *root,
					 IndexOptInfo *index,
					 ScanDirection scandir)
{
	/* GOOD: The basic idea is to make the code easy to read. */
	cpathkey = make_pathkey_from_sortinfo(root,
										  indexkey,
										  NULL,
										  index->sortopfamily[i],
										  index->opcintype[i],
										  index->indexcollations[i],
										  reverse_sort,
										  nulls_first,
										  0,
										  index->rel->relids,
										  false);
}

/*
 * BAD: Do not break an argument and its type.
 */
List *
build_index_pathkeys(PlannerInfo *
						 root,
					 IndexOptInfo *index, ScanDirection scandir)
{
	/* BAD: Super bad, always try to break __naturally__. */
	cpathkey = make_pathkey_from_sortinfo(root, indexkey, NULL, index->sortopfamily[i], index->opcintype[i], index->indexcollations[i], reverse_sort, nulls_first,
										  0, index->rel->relids, false);
}

static XidStatus
TransactionLogFetch(TransactionId transactionId)
{
	/* GOOD: Keep "&&" at end of the first line. */
	if (xidstatus != TRANSACTION_STATUS_IN_PROGRESS &&
		xidstatus != TRANSACTION_STATUS_SUB_COMMITTED)
		;

	/* BAD: Do not put "&&" at beginning of the second line. */
	if (xidstatus != TRANSACTION_STATUS_IN_PROGRESS && xidstatus != TRANSACTION_STATUS_SUB_COMMITTED)
		;

	/* BAD: NEVER do this, always try to break __naturally__. */
	if (xidstatus != TRANSACTION_STATUS_IN_PROGRESS && xidstatus !=
														   TRANSACTION_STATUS_SUB_COMMITTED)
		;
}

static bool
HeapTupleSatisfiesHistoricMVCC(Relation relation, HeapTuple htup,
							   Snapshot snapshot, Buffer buffer)
{
	/* GOOD: Break at binary operators. */
	if (!HeapTupleHeaderXminCommitted(tuple) &&
		!TransactionIdDidCommit(xmin))
		;

	/* BAD: Do not break at unary operators. */
	if (!HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin))
		;
}

static ObjectAddress
ATExecAddIdentity(Relation rel, const char *colName,
				  Node *def, LOCKMODE lockmode)
{
	/* GOOD: Long strings are allowed. */
	if (!attTup->attnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" must be declared NOT NULL before identity can be added",
						colName, RelationGetRelationName(rel))));

	/* BAD: Breaking long strings breaks the readability, too, don't do it. */
	if (!attTup->attnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" must be declared NOT NULL before identity can be added",
						colName, RelationGetRelationName(rel))));
}

void TruncateMultiXact(MultiXactId newOldestMulti, Oid newOldestMultiDB)
{
	/*
	 * GOOD: It is a good practice to break long strings at sentence
	 * boundaries.
	 */
	elog(DEBUG1, "performing multixact truncation: "
				 "offsets [%u, %u), offsets segments [%x, %x), "
				 "members [%u, %u), members segments [%x, %x)",
		 oldestMulti, newOldestMulti,
		 MultiXactIdToOffsetSegment(oldestMulti),
		 MultiXactIdToOffsetSegment(newOldestMulti),
		 oldestOffset, newOldestOffset,
		 MXOffsetToMemberSegment(oldestOffset),
		 MXOffsetToMemberSegment(newOldestOffset));
}
