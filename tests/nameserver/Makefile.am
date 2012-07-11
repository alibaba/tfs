#AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -I${top_srcdir}/src/nameserver 
AM_CPPFLAGS=-I${top_srcdir}/src/nameserver\
	    -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet

#AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -lgcov
AM_LDFLAGS=-lpthread -lrt -lm -lgtest
 
LDADD=$(top_builddir)/src/nameserver/libnameserver.a \
			$(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_PROGRAMS= test_block_collect  test_block_manager test_server_collect test_server_manager\
test_task test_task_manager test_layout_manager test_gc

test_block_collect_SOURCES=test_block_collect.cpp
test_block_collect_LDFLAGS=${AM_LDFLAGS} 

test_block_manager_SOURCES=test_block_manager.cpp
test_block_manager_LDFLAGS=${AM_LDFLAGS} 

test_server_collect_SOURCES=test_server_collect.cpp
test_server_collect_LDFLAGS=${AM_LDFLAGS} 

test_server_manager_SOURCES=test_server_manager.cpp
test_server_manager_LDFLAGS=${AM_LDFLAGS} 

test_task_SOURCES=test_task.cpp
test_task_LDFLAGS=${AM_LDFLAGS} 

test_task_manager_SOURCES=test_task_manager.cpp
test_task_manager_LDFLAGS=${AM_LDFLAGS} 

test_layout_manager_SOURCES=test_layout_manager.cpp
test_layout_manager_LDFLAGS=${AM_LDFLAGS} 

test_gc_SOURCES= test_gc.cpp
test_gc_LDFLAGS=${AM_LDFLAGS}

#integration_SOURCES= integration.cpp integration_instance.cpp integration_case.cpp
#integration_LDFLAGS=${AM_LDFLAGS}
