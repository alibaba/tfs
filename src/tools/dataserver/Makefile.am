AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB)

bin_PROGRAMS = clear_file_system\
               ds_client\
               format_file_system\
               read_super_block\
               recover_disk_data_to_cluster\
               recover_sync_file_queue\
							 convert_name\
               reload_config\
               read_index_tool\
               read_block_prefix\
               reverse_name\
               modify_super_block\
               tfsping\
               view_local_key\
							 gen_block_prefix\
							 verify_block_to_dataserver

LDADD =	$(top_builddir)/src/tools/util/libtfstoolsutil.a\
				$(top_builddir)/src/dataserver/libdataserver.a\
				$(top_builddir)/src/new_client/.libs/libtfsclient.a\
				$(UNIQUE_STORE_LDFLAGS)\
        $(TAIR_CACHE_LDFLAGS) \
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a

clear_file_system_SOURCES = clear_file_system.cpp

read_super_block_SOURCES = read_super_block.cpp

convert_name_SOURCES = convert_name.cpp

reload_config_SOURCES = reload_config.cpp

tfsping_SOURCES = tfsping.cpp

ds_client_SOURCES = ds_client.cpp

modify_super_block_SOURCES = modify_super_block.cpp

reverse_name_SOURCES = reverse_name.cpp

format_file_system_SOURCES = format_file_system.cpp

read_index_tool_SOURCES = read_index_tool.cpp

view_local_key_SOURCES = view_local_key.cpp

recover_disk_data_to_cluster_SOURCES = recover_disk_data_to_cluster.cpp

recover_sync_file_queue_SOURCES = recover_sync_file_queue.cpp

gen_block_prefix_SOURCES = gen_block_prefix.cpp

read_block_prefix_SOURCES = read_block_prefix.cpp

verify_block_to_dataserver_SOURCES = verify_block_to_dataserver.cpp
#test_load_SOURCES = test_load.cpp
