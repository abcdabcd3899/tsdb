/*-------------------------------------------------------------------------
 *
 * contrib/mars/guc.h
 *	  MARS GUCs.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __MARS_GUC_H__
#define __MARS_GUC_H__

#include "wrapper.hpp"

extern bool mars_enable_default_encoder;
extern bool mars_debug_customscan;
extern bool mars_enable_customscan;
extern bool mars_enable_index_on_auxtable;
extern bool mars_enable_column_encode;
extern char *mars_default_file_format;

extern void mars_guc_init(void);
extern void mars_guc_fini(void);

#endif /* __MARS_GUC_H__ */
