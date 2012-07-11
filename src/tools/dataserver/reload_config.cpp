/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: reload_config.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>

#include "common/internal.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/message_factory.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/reload_message.h"

using namespace tfs::common;
using namespace tfs::message;

int reload_config(const uint64_t server_ip, const int32_t flag)
{

  printf("server_ip: %s,  flag: %u\n", tbsys::CNetUtil::addrToString(server_ip).c_str(), flag);

  int ret_status = TFS_ERROR;
  ReloadConfigMessage req_rc_msg;

  req_rc_msg.set_switch_cluster_flag(flag);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* ret_msg = NULL;
  if(TFS_SUCCESS == send_msg_to_server(server_ip, client, &req_rc_msg, ret_msg))
  {
    printf("getmessagetype: %d\n", ret_msg->getPCode());
    if (ret_msg->getPCode() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*>(ret_msg);
      printf("getMessageType: %d %d==%d\n", ret_msg->getPCode(), s_msg->get_status(), STATUS_MESSAGE_OK);
      if (STATUS_MESSAGE_OK == s_msg->get_status())
      {
        ret_status = TFS_SUCCESS;
      }
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret_status;
}

uint64_t get_ip_addr(const char* ip)
{
  char* port_str = strchr(const_cast<char*>(ip), ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "%s invalid format ip:port\n", ip);
    return 0;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  return Func::str_to_addr(ip, port);
}

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    printf("Usage: %s ds_ip:port switch_cluster_flag(default:1 switch. 0: no switch)\n\n", argv[0]);
    return TFS_ERROR;
  }

  uint64_t server_ip = get_ip_addr(argv[1]);
  if (0 == server_ip)
  {
    return TFS_ERROR;
  }
  int32_t flag = 1;
  if (argc >= 3)
  {
    flag = strtoul(argv[2], reinterpret_cast<char**>(NULL), 10);
  }

  MessageFactory packet_factory;
  BasePacketStreamer packet_streamer(&packet_factory);
  int ret = NewClientManager::get_instance().initialize(&packet_factory, &packet_streamer);
  if (TFS_SUCCESS != ret)
  {
    printf("initialize NewClientManager fail, must exit, ret: %d\n", ret);
    return ret;
  }

  ret = reload_config(server_ip, flag);
  if (TFS_SUCCESS == ret)
  {
    printf("reload Config Success.\n\n");
  }
  else
  {
    printf("reload Config Fail ret=%d.\n\n", ret);
  }
  NewClientManager::get_instance().destroy();
  return ret;
}
