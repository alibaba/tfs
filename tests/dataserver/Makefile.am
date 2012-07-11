AM_CPPFLAGS=-I${top_srcdir}/src/dataserver \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet -DTFS_GTEST

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest

LDADD=$(top_builddir)/src/dataserver/libdataserver.a \
      $(top_builddir)/src/new_client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_CACHE_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_file_op test_bit_map test_mmap_file test_index_handle test_mmap_file_op \
						 test_logic_block test_meta test_blockfile_format test_logic_block_and_compact \
						 test_blockfile_manager test_physical_block test_superblock_impl test_data_handle test_sync_mirror

TESTS+=test_file_op
check_PROGRAMS+=test_file_op
test_file_op_SOURCES=test_file_op.cpp
test_file_op_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_bit_map
check_PROGRAMS+=test_bit_map
test_bit_map_SOURCES=test_bit_map.cpp
test_bit_map_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_mmap_file
check_PROGRAMS+=test_mmap_file
test_mmap_file_SOURCES=test_mmap_file.cpp
test_mmap_file_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_mmap_file_op
check_PROGRAMS+=test_mmap_file_op
test_mmap_file_op_SOURCES=test_mmap_file_op.cpp
test_mmap_file_op_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_index_handle
check_PROGRAMS+=test_index_handle
test_index_handle_SOURCES=test_index_handle.cpp
test_index_handle_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_logic_block
check_PROGRAMS+=test_logic_block
test_logic_block_SOURCES=test_logic_block.cpp
test_logic_block_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_meta
check_PROGRAMS+=test_meta
test_meta_SOURCES=test_meta.cpp
test_meta_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_blockfile_format
check_PROGRAMS+=test_blockfile_format
test_blockfile_format_SOURCES=test_blockfile_format.cpp
test_blockfile_format_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_logic_block_and_compact
check_PROGRAMS+=test_logic_block_and_compact
test_logic_block_and_compact_SOURCES=test_logic_block_and_compact.cpp
test_logic_block_and_compact_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_blockfile_manager
check_PROGRAMS+=test_blockfile_manager
test_blockfile_manager_SOURCES=test_blockfile_manager.cpp
test_blockfile_manager_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_physical_block
check_PROGRAMS+=test_physical_block
test_physical_block_SOURCES=test_physical_block.cpp
test_physical_block_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_superblock_impl
check_PROGRAMS+=test_superblock_impl
test_superblock_impl_SOURCES=test_superblock_impl.cpp
test_superblock_impl_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_data_handle
check_PROGRAMS+=test_data_handle
test_data_handle_SOURCES=test_data_handle.cpp
test_data_handle_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_sync_mirror
check_PROGRAMS+=test_sync_mirror
test_sync_mirror_SOURCES=test_sync_mirror.cpp
test_sync_mirror_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
