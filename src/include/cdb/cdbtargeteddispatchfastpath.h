/*
 * IDENTIFICATION
 *	    src/include/cdb/cdbtargeteddispatchfastpath.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBTARGETEDDISPATCHFASTPATH_H
#define CDBTARGETEDDISPATCHFASTPATH_H

#include "nodes/pathnodes.h"

extern bool TrySingleDirectDispatchFashPath(List *parsetree_list, const char *query_string);
#endif   /* CDBTARGETEDDISPATCHFASTPATH_H */
