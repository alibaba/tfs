/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: get_server_info.cpp 561 2011-07-04 06:49:46Z chuyu@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <string>

#include "common/internal.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/get_dataserver_information_message.h"
#include "message/message_factory.h"
#include "common/base_packet_streamer.h"

using namespace tfs::common;
using namespace tfs::message;

int print_info(const SuperBlock& block, const DataServerStatInfo& data_server_info, int32_t bad_count);
int get_ds_info(const uint64_t server_ip)
{

  int ret_status = TFS_ERROR;
  GetDataServerInformationMessage req_gdi_msg;

  req_gdi_msg.set_flag(ERROR_BIT_MAP);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* ret_msg = NULL;
  if(TFS_SUCCESS == send_msg_to_server(server_ip, client, &req_gdi_msg, ret_msg))
  {
    //printf("getmessagetype: %d\n", ret_msg->getPCode());
    if ((ret_msg != NULL) && (ret_msg->getPCode() == GET_DATASERVER_INFORMATION_RESPONSE_MESSAGE))
    {
      GetDataServerInformationResponseMessage* resp_gdi_msg = dynamic_cast<GetDataServerInformationResponseMessage*>(ret_msg);
      const SuperBlock& block = resp_gdi_msg->get_super_block();
      const DataServerStatInfo& data_server_info = resp_gdi_msg->get_dataserver_stat_info();
      int32_t& bad_count = resp_gdi_msg->get_bit_map_element_count();
      print_info(block, data_server_info, bad_count);
      ret_status = TFS_SUCCESS;
    }
    else
    {
      fprintf(stderr, "get msg response error\n");
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret_status;
}
int print_info(const SuperBlock& block, const DataServerStatInfo& data_server_info, int32_t bad_count)
{
  fprintf(stdout, "ds_info: %s %s %d%% %d %d %s\n",
      Func::format_size(data_server_info.use_capacity_).c_str(),
      Func::format_size(data_server_info.total_capacity_).c_str(),
      data_server_info.total_capacity_ > 0 ? static_cast<int32_t> (data_server_info.use_capacity_ * 100 / data_server_info.total_capacity_) : 0,
      data_server_info.block_count_,
      data_server_info.current_load_,
      Func::time_to_str(data_server_info.startup_time_).c_str()
      );
  fprintf(stdout, "super_block: %d %d %s %d %d %s %d %f %d%%\n",
      block.main_block_count_,
      block.used_block_count_,
      Func::format_size(block.main_block_size_).c_str(),
      block.extend_block_count_,
      block.used_extend_block_count_,
      Func::format_size(block.extend_block_size_).c_str(),
      bad_count,
      block.block_type_ratio_,
      block.used_extend_block_count_ > 0 ? static_cast<int32_t> (block.used_block_count_ * 100 / block.used_extend_block_count_) : 0
      );
  return 0;
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

static BasePacketStreamer gstreamer;
static MessageFactory gfactory;
int main(int argc, char* argv[])
{
  std::string file_name = "";
  if (argc < 2)
  {
    printf("Usage: %s srcip:port\n\n", argv[0]);
    return TFS_ERROR;
  }

  uint64_t server_ip = get_ip_addr(argv[1]);
  if (0 == server_ip)
  {
    return TFS_ERROR;
  }
  gstreamer.set_packet_factory(&gfactory);
  NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  int ret = get_ds_info(server_ip);
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "get dataserver infomation Fail ret=%d.\n\n", ret);
  }
  return ret;
}
