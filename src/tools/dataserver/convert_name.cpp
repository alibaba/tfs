/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: convert_name.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "tbsys.h"
#include "new_client/fsname.h"

using namespace tfs::client;

int main(int argc, char** argv)
{
  if (argc < 3)
  {
    printf("%s blockid fileid\n", argv[0]);
    return -1;
  }

  FSName fs_name;
  uint32_t block_id = atoi(argv[1]);
  uint64_t file_id = strtoull(argv[2], NULL, 10);
  fs_name.set_block_id(block_id);
  fs_name.set_file_id(file_id);
  if (argc > 3)
  {
    printf("prefix : %s\n", argv[3]);
    fs_name.set_suffix(argv[3]);
  }
  printf("blockid: %d, fileid: %" PRI64_PREFIX "u, name: %s\n", block_id, file_id, fs_name.get_name());
  return 0;
}
