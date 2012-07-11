AM_CPPFLAGS=-I${top_srcdir}/src/new_client \
            -I$(top_srcdir)/src \
			-I${top_srcdir}/src/common \
			$(TAIR_CACHE_CPPFLAGS)\
			-I$(TBLIB_ROOT)/include/tbsys \
			-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm

LDADD=$(top_builddir)/src/dataserver/libdataserver.a \
      $(top_builddir)/src/new_client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TAIR_CACHE_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS= test_batch_mix test_batch_read test_batch_write

TESTS+=test_batch_mix
check_PROGRAMS+=test_batch_mix
test_batch_mix_SOURCES=test_batch_mix.cpp util.cpp util.h thread.h
test_batch_mix_LDFLAGS=${AM_LDFLAGS} -static-libgcc

TESTS+=test_batch_read
check_PROGRAMS+=test_batch_read
test_batch_read_SOURCES=test_batch_read.cpp util.cpp util.h thread.h
test_batch_read_LDFLAGS=${AM_LDFLAGS} -static-libgcc

TESTS+=test_batch_write
check_PROGRAMS+=test_batch_write
test_batch_write_SOURCES=test_batch_write.cpp util.cpp util.h thread.h
test_batch_write_LDFLAGS=${AM_LDFLAGS} -static-libgcc


