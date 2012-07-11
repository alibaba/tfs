AM_CPPFLAGS=-I${top_srcdir}/src/new_client \
            -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
       $(TAIR_CACHE_CPPFLAGS) \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet
      

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest

LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_CACHE_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS= test_rc_save_fetch
#noinst_PROGRAMS= test_path test_rc_save_fetch
#noinst_PROGRAMS= test_ns_cache
#noinst_PROGRAMS= test_meta_client test_distance
#TESTS+=test_meta_client
#check_PROGRAMS+=test_meta_client
#
#test_meta_client_SOURCES=test_meta_client.cpp
#test_meta_client_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
#
#TESTS+=test_distance
#check_PROGRAMS+=test_distance
#test_distance_SOURCES=test_distance.cpp
#test_distance_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#TESTS+=test_ns_cache
#check_PROGRAMS+=test_ns_cache
#test_ns_cache_SOURCES=test_ns_cache.cpp
#test_ns_cache_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#TESTS+=test_path
#check_PROGRAMS+=test_path
#test_path_SOURCES=test_path.cpp
#test_path_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest -lz

TESTS+=test_rc_save_fetch
check_PROGRAMS+=test_rc_save_fetch
test_rc_save_fetch_SOURCES=test_rc_save_fetch.cpp
test_rc_save_fetch_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest -luuid -lz
