/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfstool.cpp 1000 2011-11-03 02:40:09Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>

#include <iostream>
#include <vector>
#include <string>

#include "new_client/fsname.h"
#include "new_client/tfs_client_impl.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;

static TfsClientImpl* g_tfs_client = NULL;
const char* g_ns_addr = NULL;
const char* g_remote_cache_master_addr = NULL;
const char* g_remote_cache_slave_addr = NULL;
const char* g_remote_cache_group_name = NULL;
int g_remote_cache_area = 0;

static void usage(const char* name)
{
  fprintf(stderr,
          "Usage: a) %s -f config_file [-n] [-h]. \n"
          "       -s nameserver ip port\n"
          "       -n set log level to error\n"
          "       -h help\n",
          name);
}

int clear_remote_block_cache(vector<string>& filename_list)
{
  int ret = TFS_ERROR;

  if (filename_list.size() <= 0)
  {
     fprintf(stderr, "nothing to do\n");
     return TFS_SUCCESS;
  }

  // init tfs client
  g_tfs_client = TfsClientImpl::Instance();
  ret = g_tfs_client->initialize(g_ns_addr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "init tfs client fail, ret: %d\n", ret);
    return ret;
  }

  // set remote cache info
  g_tfs_client->set_remote_cache_info(g_remote_cache_master_addr, g_remote_cache_slave_addr,
                                      g_remote_cache_group_name, g_remote_cache_area);
  g_tfs_client->set_use_local_cache();
  g_tfs_client->set_use_remote_cache();

  // do remove block cache stuff
  vector<string>::iterator iter = filename_list.begin();
  for (; filename_list.end() != iter; iter++)
  {
    const string& filename = *iter;
    string main_name = filename.substr(0, FILE_NAME_LEN);

    FSName fsname(main_name.c_str());
    uint32_t block_id = fsname.get_block_id();
    fprintf(stdout, "about to remove block cache: filename: %s, blockid: %u\n", filename.c_str(), block_id);
    g_tfs_client->remove_remote_block_cache(NULL, block_id);
  }

  // destroy tfs client
  if (NULL != g_tfs_client)
  {
    g_tfs_client->destroy();
  }

  return ret;
}

int main(int argc, char* argv[])
{
  int ret = TFS_ERROR;
  int32_t i;
  char *conf_file = NULL;
  const char *file_list = NULL;
  bool set_log_level = false;

  // analyze arguments
  while ((i = getopt(argc, argv, "f:nh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'h':
      default:
        usage(argv[0]);
        return ret;
    }
  }

  if (NULL == conf_file)
  {
    usage(argv[0]);
    return ret;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  TBSYS_CONFIG.load(conf_file);
  g_ns_addr = TBSYS_CONFIG.getString("public", "ns_addr", NULL);
  g_remote_cache_master_addr = TBSYS_CONFIG.getString("public", "remote_cache_master_addr", NULL);
  g_remote_cache_slave_addr = TBSYS_CONFIG.getString("public", "remote_cache_slave_addr", NULL);
  g_remote_cache_group_name = TBSYS_CONFIG.getString("public", "remote_cache_group_name", NULL);
  g_remote_cache_area = TBSYS_CONFIG.getInt("public", "remote_cache_area", 0);
  file_list = TBSYS_CONFIG.getString("public", "file_list", "./file_to_invalid_remote_cache.list");

  if (NULL == g_ns_addr ||
      NULL == g_remote_cache_master_addr || NULL == g_remote_cache_slave_addr ||
      NULL == g_remote_cache_group_name || g_remote_cache_area < 0)
  {
     fprintf(stderr, "error! must config ns addr and remote cache info!\n");
     return ret;
  }

  // open file list to get files
  FILE* fp = fopen(file_list, "r");
  if (NULL == fp)
  {
    fprintf(stderr, "error! open file list: %s failed!\n", file_list);
    return ret;
  }
  char filename[64];
  vector<string> filename_list;
  while (fgets(filename, 64, fp))
  {
    if ('\n' == filename[strlen(filename) - 1])
    {
      filename[strlen(filename) - 1] = '\0';
    }
    filename_list.push_back(filename);
  }
  fclose(fp);

  ret = clear_remote_block_cache(filename_list);

  return ret;
}



