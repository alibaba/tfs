AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -I${top_srcdir}/src/rootserver \
	    -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet -DTFS_GTEST

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -lgcov -lz

LDADD=$(top_builddir)/src/rootserver/librootserver.a \
			$(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a


noinst_PROGRAMS= build_table_test meta_server_manger_test

build_table_test_SOURCES=build_table_test.cpp
build_table_test_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

meta_server_manger_test_SOURCES=meta_server_manger_test.cpp
meta_server_manger_test_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
