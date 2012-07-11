AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
	    -fpic

AM_LDFLAGS=-lrt -lpthread -ldl

noinst_LIBRARIES = libtfscommon.a

libtfscommon_a_SOURCES = internal.cpp parameter.cpp func.cpp file_queue.cpp \
			 file_queue_thread.cpp lock.cpp directory_op.cpp base_packet.cpp\
			 file_op.cpp base_main.cpp base_service.cpp new_client.cpp client_manager.cpp\
       base_packet_streamer.cpp base_packet_factory.cpp meta_server_define.cpp session_util.cpp \
			 stream.cpp status_message.cpp rc_define.cpp mmap_file.cpp rts_define.cpp meta_hash_helper.cpp tfs_vector.ipp \
			 meta_server_define.h  tfs_vector.h\
			 atomic.h base_main.h base_packet_factory.h base_packet.h base_packet_streamer.h \
			 base_service.h buffer.h cdefine.h client_manager.h config_item.h define.h\
       directory_op.h error_msg.h file_op.h file_queue.h file_queue_thread.h func.h\
       internal.h local_packet.h lock.h new_client.h parameter.h cdefine.h rc_define.h serialization.h\
			 statistics.h status_message.h stream.h session_util.h rts_define.h mmap_file.h meta_hash_helper.h\
			 array_helper.h
include_HEADERS = define.h cdefine.h lock.h  func.h internal.h meta_server_define.h rts_define.h error_msg.h
