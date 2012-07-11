AM_CPPFLAGS=-I${top_srcdir}/src/rcserver \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -luuid `mysql_config --libs`
CXXFLAGS= -g

LDADD=$(top_builddir)/src/rcserver/librcserver.a \
      $(top_builddir)/src/new_client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a 

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_session_manager test_app_resource test_base_resource test_resource_manager \
								test_ip_replace_helper

TESTS+=test_session_manager
check_PROGRAMS+=test_session_manager
test_session_manager_SOURCES=test_session_manager.cpp

TESTS+=test_app_resource
check_PROGRAMS+=test_app_resource
test_app_resource_SOURCES=test_app_resource.cpp

TESTS+=test_base_resource
check_PROGRAMS+=test_base_resource
test_base_resource_SOURCES=test_base_resource.cpp

TESTS+=test_resource_manager
check_PROGRAMS+=test_resource_manager
test_resource_manager_SOURCES=test_resource_manager.cpp

TESTS+=test_ip_replace_helper
check_PROGRAMS+=test_ip_replace_helper
test_ip_replace_helper_SOURCES=test_ip_replace_helper.cpp
