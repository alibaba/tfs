AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB)

bin_PROGRAMS = sync_by_blk sync_by_log sync_by_file transfer_by_file sync_analyze_tool
LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a


sync_by_blk_SOURCES =  sync_by_blk.cpp sync_file_base.cpp sync_file_base.h
sync_by_log_SOURCES =  sync_by_log.cpp sync_file_base.cpp sync_file_base.h
sync_by_file_SOURCES =  sync_by_file.cpp sync_file_base.cpp sync_file_base.h

transfer_by_file_SOURCES = transfer_by_file.cpp sync_file_base.h
sync_analyze_tool_SOURCES = sync_analyze_tool.cpp
