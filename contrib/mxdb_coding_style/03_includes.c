/*-------------------------------------------------------------------------
 *
 * 03_includes.c
 *	  The Coding Style In MatrixDB - Includes.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/03_includes.c
 *
 * NOTES
 *	  The includes are usually put just after the license header, and the first
 *	  one is lways "postgres.h", unless there are special reasons, for example
 *	  itself is included by "postgres.h".
 *
 *	  There can be several include sections, separated by a blank line:
 *
 *	  - The "postgres.h";
 *	  - The system headers.  This section is optional, many system headers are
 *	    already included in "postgres.h", so only include a system header when
 *	    necessary;
 *	  - The other postgres headers;
 *	  - The MatrixDB headers.  This is no a standard section, but it is a good
 *	    practice to separate PostgreSQL/Greenplum code with MatrixDB code;
 *
 *	  Note that only system headers are quoted with <>, the local headers are
 *	  quoted with "".
 *
 *	  Also note that each section should be sorted in alphabetic order.
 *
 *	  BTW, whether should there be a blank line between the license header and
 *	  the "postgres.h"?  We see both in postgres code files, for example: in
 *	  src/backend/tcop/postgres.c and src/backend/access/transam/transam.c
 *	  there is such a blank line; in src/backend/utils/cache/syscache.c there
 *	  is no blank line between them.  However many recently added files are
 *	  using the later, so we'd better also follow it: no blank line between the
 *	  license header and the "postgres.h".
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include "mxdb/common.h"
