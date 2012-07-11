AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet\
	    -fPIC

AM_LDFLAGS=-lrt -lpthread -ldl

noinst_LIBRARIES = libtfsmessage.a
HEADER=dump_plan_message.h client_cmd_message.h heart_message.h\
			 oplog_sync_message.h crc_error_message.h reload_message.h\
			 unlink_file_message.h file_info_message.h rollback_message.h\
			 rename_file_message.h replicate_block_message.h compact_block_message.h\
			 create_filename_message.h server_status_message.h dataserver_message.h\
			 close_file_message.h read_data_message.h write_data_message.h\
			 block_info_message.h admin_cmd_message.h rc_session_message.h\
			 message_factory.h get_dataserver_information_message.h\
			 meta_nameserver_client_message.h meta_nameserver_client_message.h\
			 rts_ms_heart_message.h get_tables_from_rts_message.h base_task_message.h\
			 update_table_message.h rts_rts_heart_message.h checkserver_message.h
libtfsmessage_a_SOURCES = dump_plan_message.cpp client_cmd_message.cpp heart_message.cpp\
													oplog_sync_message.cpp crc_error_message.cpp reload_message.cpp\
													unlink_file_message.cpp file_info_message.cpp rollback_message.cpp\
													rename_file_message.cpp replicate_block_message.cpp compact_block_message.cpp\
													create_filename_message.cpp server_status_message.cpp dataserver_message.cpp\
													close_file_message.cpp read_data_message.cpp write_data_message.cpp\
													block_info_message.cpp admin_cmd_message.cpp rc_session_message.cpp\
													message_factory.cpp get_dataserver_information_message.cpp \
													meta_nameserver_client_message.cpp rts_ms_heart_message.cpp\
													get_tables_from_rts_message.cpp update_table_message.cpp\
													rts_rts_heart_message.cpp checkserver_message.cpp $(HEADER)

libtfsmessage_a_CPPFLAGS = $(AM_CPPFLAGS)

install:
