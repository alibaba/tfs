/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blocktool.cpp 432 2011-06-08 07:06:11Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>
#include <set>
#include <iostream>

#include "tbsys.h"
#include "common/internal.h"


using namespace std;
using namespace tfs::common;

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d [-h]\n", name);
  fprintf(stderr, "       -s source blklist file\n");
  fprintf(stderr, "       -d dest blklist file\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int main(int argc, char* argv[])
{
  std::string src_blk_file = "";
  std::string dest_blk_file = "";
  int i = 0;
  while ((i = getopt(argc, argv, "s:d:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      src_blk_file = optarg;
      break;
    case 'd':
      dest_blk_file = optarg;
      break;
    case 'h':
    default:
      usage(argv[0]);
    }
  }

  if ((src_blk_file.empty())
      || (src_blk_file.compare(" ") == 0)
      || dest_blk_file.empty()
      || (dest_blk_file.compare(" ") == 0))
  {
    usage(argv[0]);
  }

  FILE* src_blk_ptr = NULL;
  FILE* dest_blk_ptr = NULL;

  set<uint32_t> dest_blk_list;
  set<uint32_t> diff_blk_list;
  int ret = TFS_SUCCESS;
  if (access(src_blk_file.c_str(), R_OK) < 0 || access(dest_blk_file.c_str(), R_OK) < 0)
  {
    TBSYS_LOG(ERROR, "access input src file: %s, dest file: %s fail, error: %s\n",
        src_blk_file.c_str(), dest_blk_file.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else if ((src_blk_ptr = fopen(src_blk_file.c_str(), "r")) == NULL ||
      (dest_blk_ptr = fopen(dest_blk_file.c_str(), "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "open input src file: %s, dest file: %s fail, error: %s\n",
        src_blk_file.c_str(), dest_blk_file.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else
  {
    // load dest ds
    uint32_t blockid = 0;
    while (fscanf(dest_blk_ptr, "%u", &blockid) != EOF)
    {
      dest_blk_list.insert(blockid);
      blockid = 0;
    }
    while (fscanf(src_blk_ptr, "%u", &blockid) != EOF)
    {
      if (dest_blk_list.find(blockid) == dest_blk_list.end())
      {
        diff_blk_list.insert(blockid);
      }
      blockid = 0;
    }
  }

  set<uint32_t>::iterator sit = diff_blk_list.begin();
  for (; sit != diff_blk_list.end(); ++sit)
  {
    cout << *sit << endl;
  }

  return ret;
}
