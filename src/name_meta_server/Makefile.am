AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						$(MYSQL_CFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid

AM_LDFLAGS+=$(MYSQL_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libnamemetaserver.a

metaserver_INCLUDE=database_helper.h database_pool.h lru.h mem_helper.h meta_bucket_manager.h\
									meta_cache_helper.h meta_cache_info.h meta_heart_manager.h meta_server_service.h\
									meta_store_manager.h mysql_database_helper.h

libnamemetaserver_a_SOURCES=meta_server_service.cpp meta_store_manager.cpp \
			    database_helper.cpp mysql_database_helper.cpp database_pool.cpp \
					mem_helper.cpp meta_cache_helper.cpp meta_cache_info.cpp meta_bucket_manager.cpp\
					meta_heart_manager.cpp $(metaserver_INCLUDE)

bin_PROGRAMS = metaserver 

metaserver_SOURCES=$(libnamemetaserver_a_SOURCES) main.cpp
