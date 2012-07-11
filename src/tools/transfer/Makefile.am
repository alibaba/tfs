AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			-DUSE_NEW_CLIENT

AM_LDFLAGS=-lrt -lpthread -ldl -luuid -lz

bin_PROGRAMS = transfer_block split_block_tool compare_crc compare_same_cluster \
transfer_same_cluster_block compare_same_cluster_ext  remove_block verify_file_same_cluster\
transfer_ge_dirs transfer_logo_tool 

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
        $(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

transfer_block_SOURCES = transfer_block.cpp block_console.cpp block_console.h 
split_block_tool_SOURCES = split_block_tool.cpp
compare_crc_SOURCES=compare_crc.cpp compare_crc.h
compare_same_cluster_SOURCES=compare_same_cluster.cpp
compare_same_cluster_ext_SOURCES=compare_same_cluster_ext.cpp
transfer_same_cluster_block_SOURCES=transfer_same_cluster_block.cpp
remove_block_SOURCES=remove_block.cpp
verify_file_same_cluster_SOURCES=verify_file_same_cluster.cpp
transfer_logo_tool_SOURCES=transfer_logo_tool.cpp
transfer_ge_dirs_SOURCES=transfer_ge_dirs.cpp
