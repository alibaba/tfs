/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "common/base_service.h"
#include "common/lock.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_packet.h"
#include "test_base_service_helper.h"

using namespace tfs::common;
int main(int argc, char* argv[])
{
  std::string ip(argv[0]);
  uint64_t server = tbsys::CNetUtil::strToAddr("10.232.35.41", 8100);
  TestPacketFactory* factory = new TestPacketFactory();
  BasePacketStreamer* streamer = new BasePacketStreamer(factory);
  factory->initialize();
  streamer->set_packet_factory(factory);
  NewClientManager::get_instance().initialize(factory, streamer);
  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* output = NULL;
  TestPacket packet;
  int32_t iret = send_msg_to_server(server, client, &packet, output);
  if (TFS_SUCCESS == iret)
  {
    StatusPacket* st = dynamic_cast<StatusPacket*>(output);
    TBSYS_LOG(DEBUG, "receive: %d", st->get_status());
  }
  else
  {
    TBSYS_LOG(ERROR, "send msg to server error, iret : %d", iret);
  }
  NewClientManager::get_instance().destroy_client(client);

  while(1);
}
