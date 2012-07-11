/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: read_index_tool.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <iostream>
#include <string>
#include "dataserver/index_handle.h"
#include "common/file_op.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::dataserver;

int main(int argc, char* argv[])
{
  if (argc != 2)
  {
    cout << "Usage: " << argv[0] << " filename " << endl;
    return -1;
  }
  cout << "block prefix file: " << argv[1] << endl;

  string file = argv[1];
  FileOperation file_op(argv[1]);
  int32_t read_size = file_op.get_file_size();
  if (read_size <= 0)
  {
    cout << "open index file " << argv[1] << "failed!" << endl;
    return read_size;
  }
  const int SIZE = sizeof(BlockPrefix);
  BlockPrefix prefix;
  char* buf = (char*)&prefix;
  int count = read_size / SIZE;
  int offset = 0;
  for (int i = 0; i < count; i++)
  {
    int ret = file_op.pread_file(buf, SIZE, offset + i * SIZE);
    if (!ret)
    {
      cout <<"logic block id: " << prefix.logic_blockid_ << " prev: " << prefix.prev_physic_blockid_ << "next: " << prefix.next_physic_blockid_ << endl;
    }
  }
  return 0;
}
