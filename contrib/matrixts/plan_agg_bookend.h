/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*-------------------------------------------------------------------------
 *
 * plan_agg_booend.h
 *	  Optimization for FIRST/LAST aggregate functions.
 *
 *
 * Portions Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef MX_PLAN_AGG_BOOEND_H
#define MX_PLAN_AGG_BOOEND_H

#include "postgres.h"

#include "nodes/pathnodes.h"

extern void ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist);

#endif /* MX_PLAN_AGG_BOOEND_H */
