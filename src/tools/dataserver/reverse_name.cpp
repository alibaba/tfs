/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: reverse_name.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
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
  if (argc < 2)
  {
    printf("%s tfsname\n", argv[0]);
    return -1;
  }
  FSName name(argv[1]);
  printf("blockid: %u, fileid: %" PRI64_PREFIX "u, seqid: %u\n", name.get_block_id(), name.get_file_id(), name.get_seq_id());
  return 0;
}
