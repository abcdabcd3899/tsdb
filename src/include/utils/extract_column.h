/*
 * ----------------------------------------------
 *
 * extract_column.h
 *
 *  * Portions Copyright (c) 2009-2010, Greenplum Inc.
 *  * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 * ------------------------------------------------
 */
#ifndef EXTRACT_COLUMN_H
#define EXTRACT_COLUMN_H

struct ExtractcolumnContext
{
	Relation	rel;
	bool	   *cols;
	AttrNumber	natts;
	bool		found;
};

bool extractcolumns_from_node(Relation rel, Node *expr,
							  bool *cols, AttrNumber natts);
#endif //EXTRACT_COLUMN_H
