
/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mock_data_server.cpp 418 2011-06-03 07:20:32Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <Service.h>

#include "mock_data_server_instance.h"

int main(int argc, char* argv[])
{
  TBSYS_LOG(DEBUG, "EAGAIN : %d, EINVAL: %d, EDEADLK: %d", EAGAIN, EINVAL,EDEADLK);
  tfs::mock::MockDataService service;
  return service.main(argc, argv);
}
