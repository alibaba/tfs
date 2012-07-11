#AM_CPPFLAGS=-I${top_srcdir}/src \
#            -I$(TBLIB_ROOT)/include/tbsys \
#            -I$(TBLIB_ROOT)/include/tbnet \
#						-DUSE_CPP_CLIENT
#
#LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a \
#      $(top_builddir)/src/message/libtfsmessage.a \
#      $(top_builddir)/src/common/libtfscommon.a\
#      $(TBLIB_ROOT)/lib/libtbnet.a \
#      $(TBLIB_ROOT)/lib/libtbsys.a

AM_CPPFLAGS=-I${top_srcdir}/src \
            -I$(TBLIB_ROOT)/include/tbsys \
            -I$(TBLIB_ROOT)/include/tbnet -DUSE_CPP_CLIENT


LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lrt

#noinst_PROGRAMS=tfs_large_file_cases tfs_block_cache
noinst_PROGRAMS=rc_api_cases

rc_api_cases_SOURCES=rc_api_cases.cpp
rc_api_cases_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#tfs_large_file_cases_SOURCES=tfs_large_file_cases.cpp tfs_large_file_cases.h tfs_ioapi_util.cpp tfs_ioapi_util.h
#tfs_large_file_cases_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
#
#tfs_block_cache_SOURCES=tfs_block_cache.cpp tfs_block_cache.h tfs_ioapi_util.cpp tfs_ioapi_util.h
#tfs_block_cache_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
