
SUBDIRS = common/fraser

bin_PROGRAMS = trans
trans_SOURCES = bench/main.cc \
				common/allocator.cc\
				common/timehelper.cc\
				common/assert.cc\
				ostm/skiplist/stmskip.cc\
				translink/list/translist.cc\
				translink/skiplist/lftttransskip.cc\
				translink/map/transmap.cc\
				obslink/list/obslist.cc\
				obslink/skiplist/obsskip.cc

trans_CPPFLAGS = $(WARN_FLAGS) -std=c++11 -pthread\
				-DINTEL -DCACHE_LINE_SIZE=`getconf LEVEL1_DCACHE_LINESIZE`\
				 -I$(top_builddir)/src -I$(top_srcdir)/src


trans_LDADD = common/fraser/libfd.la

lib_LIBRARIES = liblftt.a
liblftt_a_SOURCES = $(trans_SOURCES)
liblftt_a_CPPFLAGS = $(trans_CPPFLAGS)
liblftt_a_LIBADD = common/fraser/libfd.la