/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: clear_file_system.cpp 380 2011-05-30 08:04:16Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <stdio.h>
#include "common/parameter.h"
#include "dataserver/blockfile_manager.h"
#include "dataserver/version.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  std::string server_index;

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
      fprintf(stderr, "create tfs file system tool, version: %s\n", Version::get_build_description());
      return 0;
    case 'h':
    default:
      help_info = 1;
      break;
    }
  }

  if ((conf_file == NULL) || (server_index.size() == 0) || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  int ret = 0;
  if (EXIT_SUCCESS != TBSYS_CONFIG.load(conf_file))
  {
    cerr << "load config error conf_file is " << conf_file;
    return TFS_ERROR;
  }
  if ((ret = SYSPARAM_DATASERVER.initialize(conf_file, server_index)) != TFS_SUCCESS)
  {
    cerr << "SysParam::load file system param failed:" << conf_file << endl;
    return ret;
  }

  ret = BlockFileManager::get_instance()->clear_block_file_system(SYSPARAM_FILESYSPARAM);
  if (ret)
  {
    fprintf(stderr, "clear tfs file system fail. ret: %d, error: %d, desc: %s\n", ret, errno, strerror(errno));
    return ret;
  }
  return 0;
}
