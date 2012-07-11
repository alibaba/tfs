AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lz -lrt -lpthread -ldl

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a


MOCKDATASERVER_SOURCE_LIST= mock_data_server_instance.cpp mock_data_server_instance.h

bin_PROGRAMS = mock_data_server

mock_data_server_SOURCES=$(MOCKDATASERVER_SOURCE_LIST) mock_data_server.cpp


