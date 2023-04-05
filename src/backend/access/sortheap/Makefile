#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/sortheap
#
# IDENTIFICATION
#    src/backend/access/sortheap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/sortheap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = sortheapam.o sortheapam_handler.o sortheap_common.o sortheap_tapesets.o \
		sortheap_tape.o sortheap_external_sort.o sortheap_default_callbacks.o \
		sortheap_compress_callbacks.o sortheap_freepages.o sortheap_xlog.o \
		sortheap_customscan.o sortheap_btree.o sortheap_brin.o \
		sortheap_merge_worker.o sortheap_columnstore.o

sortheap_external_sort.o: qsort_tuple.c

qsort_tuple.c: gen_qsort_tuple.pl
	    $(PERL) $(srcdir)/gen_qsort_tuple.pl $< > $@

clean:
		rm -f qsort_tuple.c

include $(top_srcdir)/src/backend/common.mk
