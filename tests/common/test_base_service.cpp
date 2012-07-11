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
#include <Timer.h>
#include <Handle.h>
#include <time.h>
#include "common/base_service.h"
#include "common/lock.h"
#include "common/directory_op.h"
#include "common/status_message.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "test_base_service_helper.h"

using namespace tfs::common;

int test_async_call_back_func(NewClient* client)
{
  TBSYS_LOG(DEBUG, "async_call_back_func===========");
  int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == iret)
  {
    NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
    NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
    iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
    if (TFS_SUCCESS == iret)
    {
      iret = sresponse->empty() ? EXIT_TIMEOUT_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        NewClient::RESPONSE_MSG_MAP_ITER  iter = sresponse->begin();
        iret = NULL != iter->second.second? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(INFO, "async call back");
          TestPacketResponse* st = dynamic_cast<TestPacketResponse*>(iter->second.second);
          st->dump();
        }
      }
    }
  }
  return iret;
}

class SendTestPacketTimerTask : public tbutil::TimerTask
{
public:
  SendTestPacketTimerTask(uint64_t server):seq_(0), server_(server){}
  virtual ~SendTestPacketTimerTask(){}

  void runTimerTask()
  {
    TBSYS_LOG(DEBUG, "entry SendTestPacketTimerTask");
    //send msg to server
    TestPacket packet;
    packet.set_value(seq_++);
    NewClient* client = NewClientManager::get_instance().create_client();
    tbnet::Packet* output = NULL;
    packet.dump();
    int32_t iret = send_msg_to_server(server_, client, &packet, output);
    packet.dump();
    if (TFS_SUCCESS == iret)
    {
      dynamic_cast<TestPacketResponse*>(output)->dump();
    }
    NewClientManager::get_instance().destroy_client(client);

    //async send msg to server
    client =  NewClientManager::get_instance().create_client();
    if (NULL != client)
    {
      packet.set_value(seq_++);
      std::vector<uint64_t> servers;
      servers.push_back(server_);
      client->async_post_request(servers, &packet, test_async_call_back_func);
    }
  }
private:
  int64_t seq_;
  uint64_t server_;
};

typedef tbutil::Handle<SendTestPacketTimerTask> SendTestPacketTimerTaskPtr;


class TestService: public tfs::common::BaseService
{
public:
  TestService(){ factory_ =  new TestPacketFactory();streamer_ = new tfs::common::BasePacketStreamer(factory_);}
  ~TestService(){tbsys::gDelete(streamer_); tbsys::gDelete(factory_);}
  /** create the packet streamer, this is used to create packet according to packet code */
  virtual tbnet::IPacketStreamer* create_packet_streamer()
  {
    return streamer_;
  }
  virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
  {
  }

  /** create the packet streamer, this is used to create packet*/
  virtual tfs::common::BasePacketFactory* create_packet_factory()
  {
      return factory_;
  }

  /** get log file path*/
  virtual const char* get_log_file_path()
  {
    return NULL;
  }
  virtual void destroy_packet_factory(BasePacketFactory* factory)
  {
  }

  /** handle packet*/
  virtual bool handlePacketQueue(tbnet::Packet *packet, void *args)
  {
    TBSYS_LOG(DEBUG, "pcode: %d", packet->getPCode());
    BaseService::handlePacketQueue(packet, args);
    if (packet->getPCode() == TEST_PCODE)
    {
      TestPacket* tpacket = dynamic_cast<TestPacket*>(packet);
      TBSYS_LOG(DEBUG, "============================");
      //tpacket->dump();
      TestPacketResponse* st = new TestPacketResponse();
      st->set_value(tpacket->get_value() + 1);
      tpacket->reply(st);
      TBSYS_LOG(DEBUG, "======en=========================");
    }
    return true;
  }

  int parse_common_line_args(int argc, char* argv[])
  {
    std::string other_server_ip_port;
    int32_t i = 0;
    int32_t port = 0;
    while ((i = getopt(argc, argv, "o:l:")) != EOF)
    {
      switch (i)
      {
      case 'o':
        other_server_ip_port = optarg;
        break;
      case 'l':
        port = atoi(optarg);
        break;
      default:
        break;
      }
    }
    if (other_server_ip_port.empty() || port <= 1024 || port > 65535)
    {
      TBSYS_LOG(ERROR, "invalid ip: %s or port: %d", other_server_ip_port.c_str(), port);
      return TFS_ERROR;
    }
    server_ =  tbsys::CNetUtil::strToAddr(other_server_ip_port.c_str(), port); 
    return TFS_SUCCESS;
  }

  int initialize(int argc, char* argv[])
  {
    TBSYS_LOG(DEBUG, "initialize timer task");
    SendTestPacketTimerTaskPtr ptr = new SendTestPacketTimerTask(server_);
    get_timer()->scheduleRepeated(ptr, tbutil::Time::seconds(1));
    return TFS_SUCCESS;
  }
private:
  tfs::common::BasePacketStreamer* streamer_;
  tfs::common::BasePacketFactory* factory_;
  uint64_t server_;
};

int main(int argc, char* argv[])
{
  TestService service;
  return service.main(argc, argv);
}
