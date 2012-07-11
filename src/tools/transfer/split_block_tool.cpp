/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: split_block_tool.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */

#include <stdio.h>

#include <string>
#include <vector>
#include <set>
#include <string.h>

#include "tbsys.h"
#include "common/internal.h"

using namespace std;
using namespace tfs::common;

int main(int argc, char* argv[])
{
  if (argc != 5)
  {
    fprintf(stderr, "Usage: %s dslist blklist savepath file_prefix\n", argv[0]);
    return TFS_ERROR;
  }

  string dslist_file = argv[1];
  string blklist_file = argv[2];
  string save_path = argv[3];
  string file_prefix = argv[4];
  set<string> ds_input_files;
  vector<FILE*> ds_input_blocks;
  FILE* dslist_file_ptr = NULL;
  FILE* blklist_file_ptr = NULL;

  int ret = TFS_SUCCESS;
  if (access(dslist_file.c_str(), R_OK) < 0 || access(blklist_file.c_str(), R_OK) < 0 || access(save_path.c_str(), R_OK) < 0)
  {
    TBSYS_LOG(ERROR, "access input dslist file: %s, blklist file: %s, save_path: %s fail, herror: %s\n",
        dslist_file.c_str(), blklist_file.c_str(), save_path.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else if ((dslist_file_ptr = fopen(dslist_file.c_str(), "r")) == NULL ||
      (blklist_file_ptr = fopen(blklist_file.c_str(), "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "open input dslist file: %s, blklist file: %s fail, error: %s\n",
        dslist_file.c_str(), blklist_file.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else
  {
      // create input block
      char ds_addr[MAX_PATH_LENGTH];
      while (fscanf(dslist_file_ptr, "%s", ds_addr) != EOF)
      {
        string ds_input_block_file = save_path + "/" + file_prefix + ds_addr;
        ds_input_files.insert(ds_input_block_file);
      }

      set<string>::iterator sit = ds_input_files.begin();
      for ( ; sit != ds_input_files.end(); ++sit)
      {
        FILE* ds_input_ptr = fopen((*sit).c_str(), "w+");
        if (NULL == ds_input_ptr)
        {
          TBSYS_LOG(ERROR, "open ds input block file: %s, error: %s\n",
              (*sit).c_str(), strerror(errno));
          ret = TFS_ERROR;
          break;
        }
        else
        {
          ds_input_blocks.push_back(ds_input_ptr);
        }
      }
  }

  int size = ds_input_blocks.size();
  if (TFS_SUCCESS == ret)
  {
    int index = 0;
    uint32_t blockid = 0;
    while (fscanf(blklist_file_ptr, "%u", &blockid) != EOF)
    {
      fprintf(ds_input_blocks[index], "%u\n", blockid);
      ++index;
      if (index == size)
      {
        index = 0;
      }
    }
  }

  fclose(dslist_file_ptr);
  fclose(blklist_file_ptr);
  for (int i = 0; i < size; ++i)
  {
    fclose(ds_input_blocks[i]);
  }
  return ret;
}
