/*-------------------------------------------------------------------------
 *
 * modifytable.h
 *	  MARS Custom ModifyTable node header file.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/customnodes/modifytable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MODIFYTABLE_H
#define MATRIXDB_MODIFYTABLE_H

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"

typedef struct MARS_ModifyTableState
{
	CustomScanState cscan_state;
	Plan *subplan;
	ModifyTable *mt;
	List *infer_elems;
} MARS_ModifyTableState;

typedef struct MARS_ModifyTablePath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
} MARS_ModifyTablePath;

extern Path *create_mars_modifytable_path(PlannerInfo *root, ModifyTablePath *path, int subpath_idx);
extern void mars_modifytable_tlist_fixup(Plan *plan);
extern void mars_modifytable_register(void);
#endif // MATRIXDB_MODIFYTABLE_H
