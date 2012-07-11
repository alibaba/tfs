AM_CPPFLAGS= -Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						$(MYSQL_CFLAGS)

 
AM_LDFLAGS= $(MYSQL_LDFLAGS) -lz -lrt -lpthread -ldl -luuid

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

noinst_LIBRARIES=librcserver.a

librcserver_a_SOURCES=database_helper.cpp mysql_database_helper.cpp app_resource.cpp\
                      resource_manager.cpp base_resource.cpp session_manager.cpp \
											ip_replace_helper.cpp \
											rc_service.cpp app_resource.h base_resource.h\
											database_helper.h i_resource_manager.h mocked_resource_manager.h\
											mysql_database_helper.h rc_service.h resource.h resource_manager.h\
											resource_server_data.h session_manager.h ip_replace_helper.h

bin_PROGRAMS = rcserver
rcserver_SOURCES=$(librcserver_a_SOURCES) main.cpp
