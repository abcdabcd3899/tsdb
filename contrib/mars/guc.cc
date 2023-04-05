/*-------------------------------------------------------------------------
 *
 * contrib/mars/guc.cc
 *	  MARS GUCs.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"

#include "guc.h"
#include "coder/Coder.h"
#include "Footer.h"

bool mars_enable_default_encoder = true;
bool mars_enable_customscan = true;
bool mars_debug_customscan = false;
bool mars_enable_index_on_auxtable = true;
bool mars_enable_column_encode = true;
char *mars_default_file_format = NULL;
char *mars_default_auxfile_format = NULL;

static void
mars_assign_default_file_format(const char *newval, void *extra)
{
	if (pg_strcasecmp(newval, "auto") == 0)
		mars::coder::default_version = mars::coder::DEFAULT_VERSION;
	else if (pg_strcasecmp(newval, "v0.1") == 0)
		mars::coder::default_version = mars::coder::Version::V0_1;
	else if (pg_strcasecmp(newval, "v1.0") == 0)
		mars::coder::default_version = mars::coder::Version::V1_0;
	else
		elog(ERROR, "unknown mars file format: \"%s\"",
			 newval);
}

static void
mars_assign_default_auxfile_format(const char *newval, void *extra)
{
	if (pg_strcasecmp(newval, "auto") == 0)
		mars::footer::default_version = mars::footer::DEFAULT_VERSION;
	else if (pg_strcasecmp(newval, "v1") == 0)
		mars::footer::default_version = mars::footer::Version::V1;
	else if (pg_strcasecmp(newval, "v2") == 0)
		mars::footer::default_version = mars::footer::Version::V2;
	else
		elog(ERROR, "unknown mars auxfile format: \"%s\"",
			 newval);
}

static bool
mars_check_auto_column_encode(bool *newval, void **extra, GucSource source)
{
	if (!*newval)
		elog(NOTICE, "Column code greatly improves the compression effect,"
					 "please change it carefully");
	return true;
}

void mars_guc_init(void)
{
	DefineCustomBoolVariable("mars.enable_customscan",
							 "enable mars custom scan",
							 NULL,
							 &mars_enable_customscan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("mars.enable_default_encoder",
							 "enable mars default encoder",
							 NULL,
							 &mars_enable_default_encoder,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("mars.debug_customscan",
							 "show customscan debug message",
							 NULL,
							 &mars_debug_customscan,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("mars.enable_index_on_auxtable",
							 "enable creating index on aux table",
							 NULL,
							 &mars_enable_index_on_auxtable,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("mars.enable_column_encode",
							 "enable mars auto column encode",
							 NULL,
							 &mars_enable_column_encode,
							 true,
							 PGC_USERSET,
							 0,
							 &mars_check_auto_column_encode,
							 NULL,
							 NULL);
	DefineCustomStringVariable("mars.default_file_format",
							   "the default mars file format, can be \"auto\", \"v0.1\", \"v1.0\"",
							   NULL /* long_desc */,
							   &mars_default_file_format,
							   "auto",
							   PGC_USERSET,
							   0 /* flags */,
							   NULL /* check_hook */,
							   mars_assign_default_file_format,
							   NULL /* show_hook */);
	DefineCustomStringVariable("mars.default_auxfile_format",
							   "the default mars auxiliary file format, can be \"auto\", \"v1\", \"v2\"",
							   NULL /* long_desc */,
							   &mars_default_auxfile_format,
							   "auto",
							   PGC_USERSET,
							   0 /* flags */,
							   NULL /* check_hook */,
							   mars_assign_default_auxfile_format,
							   NULL /* show_hook */);
}

void mars_guc_fini(void)
{
}
