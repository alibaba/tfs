/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: gen_block_prefix.cpp 553 2012-03-06 15:30 linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd@taobao.com
 *      - initial release
 *
 */


#include <stdio.h>
#include "tbsys.h"
#include "common/internal.h"
#include "common/parameter.h"
#include "dataserver/version.h"
#include "dataserver/blockfile_manager.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;


void usage(const char* prog_name)
{
  fprintf(stderr, "\nUsage: %s -f conf_file -i index -h -v\n", prog_name);
  fprintf(stderr, "  -f configure file\n");
  fprintf(stderr, "  -i server_index  dataserver index number\n");
  fprintf(stderr, "  -v show version info\n");
  fprintf(stderr, "  -h help info\n");
  fprintf(stderr, "\n");
  exit(-1);
}

int main(int argc,char* argv[])
{
  char* conf_file = NULL;
  int32_t help_info = 0;
  int32_t i = 0;
  string server_index;

  // check command line args
  while ((i = getopt(argc, argv, "f:i:vh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 'i':
        server_index = optarg;
        break;
      case 'v':
        fprintf(stderr, "generate block prefix tool, version: %s\n", Version::get_build_description());
        return 0;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || 0 == server_index.size() || help_info)
  {
    usage(argv[0]);
  }

  // init fs parameter
  int32_t ret = 0;
  TBSYS_CONFIG.load(conf_file);
  if ((ret = SYSPARAM_FILESYSPARAM.initialize(server_index)) != TFS_SUCCESS)
  {
    fprintf(stderr, "SysParam::loadFileSystemParam failed, conf_file = %s\n", conf_file);
    return ret;
  }

  ret = BlockFileManager::get_instance()->load_super_blk(SYSPARAM_FILESYSPARAM);
  if (TFS_SUCCESS != ret)
  {	
    fprintf(stderr, "load tfs file system superblock fail. ret: %d\n",ret);
    return ret;
  }

  SuperBlock super_block;
  ret = BlockFileManager::get_instance()->query_super_block(super_block);
  if (TFS_SUCCESS != ret)
  {	
    fprintf(stderr, "query superblock fail. ret: %d\n", ret);
    return ret;
  }

  // check if exsits?  if so, we can't override
  string block_prefix_file = super_block.mount_point_ + BLOCK_HEADER_PREFIX;
  ret = access(block_prefix_file.c_str(), F_OK);
  if (0 == ret)
  {
    fprintf(stderr, "%s already exists. \n", block_prefix_file.c_str());
    return ret;
  }

  // doesn't exist, create it
  ret = BlockFileManager::get_instance()->create_block_prefix();
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "create block_prefix file fail. ret: %d\n", ret);
    return ret;
  }

  // read bitmap
  BitMap normal_bit_map;
  char* bit_map_data = NULL;
  int32_t bit_map_len = 0;
  int32_t bit_map_set_count = 0;
  ret = BlockFileManager::get_instance()->query_bit_map(&bit_map_data, bit_map_len, bit_map_set_count);
  normal_bit_map.mount(bit_map_len * 8, bit_map_data);

  int block_count = super_block.main_block_count_ + super_block.extend_block_count_;
  int pf_file_size = block_count * sizeof(BlockPrefix);

  // just map whole file
  MMapOption map_option;
  map_option.max_mmap_size_ = pf_file_size;
  map_option.first_mmap_size_ = pf_file_size;
  map_option.per_mmap_size_ = pf_file_size;

  MMapFileOperation* file_op = new MMapFileOperation(block_prefix_file.c_str());
  ret = file_op->mmap_file(map_option);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "mmap block_prefix file fail. ret: %d\n", ret);
    tbsys::gDelete(file_op);
    return ret;
  }

  // read block header
  BlockPrefix block_prefix;
  string block_file;

  for (int i = 1; i <= block_count; i++)
  {
    if (normal_bit_map.test(i))
    {
      stringstream tmp_stream;
      if (i <= super_block.main_block_count_)
      {
        tmp_stream << super_block.mount_point_ <<  MAINBLOCK_DIR_PREFIX << i;
      }
      else
      {
        tmp_stream << super_block.mount_point_ <<  EXTENDBLOCK_DIR_PREFIX << i;
      }
      tmp_stream >> block_file;

      printf("processing block %s ...\n", block_file.c_str());

      FileOperation *block_op = new FileOperation(block_file);
      ret = block_op->pread_file((char*)(&block_prefix), sizeof(BlockPrefix), 0);
      if (TFS_SUCCESS != ret)
      {
        fprintf(stderr, "read main block %d header fail. ret: %d\n", i, ret);
        tbsys::gDelete(block_op);
        file_op->munmap_file();
        tbsys::gDelete(file_op);
        return ret;
      }

      ret = file_op->pwrite_file((char*)(&block_prefix), sizeof(BlockPrefix), (i - 1) * sizeof(BlockPrefix));
      if (TFS_SUCCESS != ret)
      {
        fprintf(stderr, "write block %d header fail. ret: %d\n", i, ret);
        tbsys::gDelete(block_op);
        file_op->munmap_file();
        tbsys::gDelete(file_op);
        return ret;
      }

      tbsys::gDelete(block_op);
    }
  }

  // flush file
  ret = file_op->flush_file();
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "sync block_prefix file fail. ret: %d\n", ret);
    file_op->munmap_file();
    tbsys::gDelete(file_op);
    return ret;
  }

  // munmmap file
  file_op->munmap_file();
  tbsys::gDelete(file_op);

  return 0;
}
