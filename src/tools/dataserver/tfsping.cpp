/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfsping.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include "common/func.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
using namespace tfs::common;
//using namespace tfs::message;

int ping(const uint64_t ip)
{
  int32_t status_value = 0;
  StatusMessage ping_message(STATUS_MESSAGE_PING);
  int ret_status = send_msg_to_server(ip, &ping_message, status_value);
  if (TFS_SUCCESS == ret_status && STATUS_MESSAGE_PING != status_value)
  {
    ret_status = TFS_ERROR;
  }
  return ret_status;
}

int main(int argc, char* argv[])
{
  if (argc != 3)
  {
    printf("%s ip port\n\n", argv[0]);
    return TFS_ERROR;
  }

  int32_t port = strtoul(argv[2], NULL, 10);
  uint64_t ip;
  IpAddr* adr = reinterpret_cast<IpAddr*>(&ip);
  adr->ip_ = Func::get_addr(argv[1]);
  adr->port_ = port;

  int ret = ping(ip);
  if (TFS_SUCCESS == ret)
  {
    printf("%s SUCCESS.\n", tbsys::CNetUtil::addrToString(ip).c_str());
  }
  else
  {
    printf("%s FAILURE.\n", tbsys::CNetUtil::addrToString(ip).c_str());
  }
  return ret;
}
