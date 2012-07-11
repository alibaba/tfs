AM_CPPFLAGS=`mysql_config --cflags`  -I${top_srcdir}/src/name_meta_server \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -luuid `mysql_config --libs`
CXXFLAGS= -g

LDADD=$(top_builddir)/src/name_meta_server/libnamemetaserver.a \
      $(top_builddir)/src/new_client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a 

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_service test_meta_info test_cache_info test_cache_helper  meta_bucket_test meta_bucket_manager_test

TESTS+=test_meta_info
check_PROGRAMS+=test_meta_info
test_meta_info_SOURCES=test_meta_info.cpp

TESTS+=test_cache_info
check_PROGRAMS+=test_cache_info
test_cache_info_SOURCES= test_cache_info.cpp 

TESTS+=test_cache_helper
check_PROGRAMS+=test_cache_helper
test_cache_helper_SOURCES= test_cache_helper.cpp 

#TESTS+=test_lru
#check_PROGRAMS+=test_lru
#test_lru_SOURCES=test_lru.cpp

TESTS+=meta_bucket_test 
check_PROGRAMS+=meta_bucket_test
meta_bucket_test_SOURCES=meta_bucket_test.cpp

TESTS+=meta_bucket_manager_test 
check_PROGRAMS+=meta_bucket_manager_test
meta_bucket_manager_test_SOURCES=meta_bucket_manager_test.cpp

TESTS+=test_service
check_PROGRAMS+=test_service
test_service_SOURCES= test_service.cpp 
