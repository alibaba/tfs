/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: repair_block_count.cpp 553 2012-07-16 15:30 linqing.zyd@taobao.com $
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
        fprintf(stderr, "repair block count tool, version: %s\n", Version::get_build_description());
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

  BitMap normal_bit_map;
  char* bit_map_data = NULL;
  int32_t bit_map_len = 0;
  int32_t bit_map_set_count = 0;
  ret = BlockFileManager::get_instance()->query_bit_map(&bit_map_data, bit_map_len, bit_map_set_count);
  normal_bit_map.mount(bit_map_len * 8, bit_map_data, false);

  int block_count = super_block.main_block_count_ + super_block.extend_block_count_;
  int used_main_block_count = 0;
  int used_ext_block_count = 0;

  for (int i = 1; i <= block_count; i++)
  {
    if (normal_bit_map.test(i))
    {
      if (i <= super_block.main_block_count_)
      {
        used_main_block_count++;
      }
      else
      {
        used_ext_block_count++;
      }
    }
  }

  // correct used main block count
  super_block.used_block_count_ = used_main_block_count;
  super_block.used_extend_block_count_ = used_ext_block_count;

  // flush fs super
  SuperBlockImpl* super_block_impl = new SuperBlockImpl(SYSPARAM_FILESYSPARAM.mount_name_
      , SYSPARAM_FILESYSPARAM.super_block_reserve_offset_);
  ret = super_block_impl->write_super_blk(super_block);
  if (0 == ret)
  {
    printf("correct used main block count to: %d\n", used_main_block_count);
    printf("correct used ext block count to: %d\n", used_ext_block_count);
    super_block_impl->flush_file();
  }
  tbsys::gDelete(super_block_impl);

  return 0;
}
