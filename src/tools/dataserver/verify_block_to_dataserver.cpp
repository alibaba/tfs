/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ds_client.cpp 465 2011-06-09 11:26:40Z daoan@taobao.com $
 *
 * Authors:
 *   jihe
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "common/internal.h"
#include "common/func.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "message/block_info_message.h"
#include "tools/util/ds_lib.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

static const int32_t MAX_SCAN_NUMS = 40960;

int main(int argc, char* argv[])
{
  int i = 0;
  std::string file_path;
  while ((i = getopt(argc, argv, "f:h")) != -1)
  {
    switch (i)
    {
    case 'f':
      file_path = optarg;
      break;
    case 'h':
    default:
      TBSYS_LOG(INFO, "Usage: %s -f filepath", argv[0]);
      return -1;
    }
  }

  int ret = access(file_path.c_str(), R_OK) == 0 ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    FILE * fp = fopen(file_path.c_str(), "r+");
    ret = NULL == fp ? TFS_ERROR : TFS_SUCCESS;
    if (TFS_SUCCESS == ret)
    {
      TBSYS_LOGGER.setLogLevel("WARN");
      tfs::message::MessageFactory factory;
      tfs::common::BasePacketStreamer streamer;
      streamer.set_packet_factory(&factory);
      NewClientManager::get_instance().initialize(&factory, &streamer);
      uint32_t block = 0;
      uint64_t server = 0;
      uint32_t port = 0;
      char addr[32];
      std::map<uint64_t, std::set<uint32_t> > result;
      do
      {
        int32_t count = 0;
        while (count < MAX_SCAN_NUMS && EOF != ret)
        {
          ++count;
          ret = fscanf(fp, "%u;%u;%s", &block, &port, addr);
          if (3 == ret)
          {
            server = Func::str_to_addr(addr, port);
            std::pair<std::map<uint64_t, std::set<uint32_t> >::iterator, bool> res;
            res.first = result.find(server);
            if (res.first == result.end())
               res = result.insert(std::map<uint64_t, std::set<uint32_t> >::value_type(server, std::set<uint32_t>()));
            res.first->second.insert(block);
          }
        }
        std::map<uint64_t, std::set<uint32_t> >::iterator iter = result.begin();
        while (iter != result.end())
        {
          server = iter->first;
          ListBlockMessage req_msg;
          req_msg.set_block_type(LB_BLOCK);
          NewClient* client = NewClientManager::get_instance().create_client();
          if (NULL != client)
          {
            tbnet::Packet* ret_msg = NULL;
            if (TFS_SUCCESS == send_msg_to_server(server, client, &req_msg, ret_msg))
            {
              if (RESP_LIST_BLOCK_MESSAGE == ret_msg->getPCode())
              {
                RespListBlockMessage* msg = dynamic_cast<RespListBlockMessage*>(ret_msg);
                std::vector<uint32_t>::const_iterator it = msg->get_blocks()->begin();
                TBSYS_LOG(WARN, "%s has %zd block", tbsys::CNetUtil::addrToString(server).c_str(), msg->get_blocks()->size());
                for (;it != msg->get_blocks()->end(); ++it)
                {
                  if (iter->second.end() != iter->second.find((*it)))
                    iter->second.erase((*it));
                }
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);

          if (iter->second.empty())
              result.erase(iter++);
          else
              ++iter;
        }
      }
      while (ret != EOF);

      std::map<uint64_t, std::set<uint32_t> >::iterator iter = result.begin();
      std::set<uint32_t>::iterator it;
      for (; iter != result.end(); ++iter)
      {
        if (!iter->second.empty())
        {
          std::stringstream str;
          it = iter->second.begin();
          for (; it != iter->second.end(); ++it)
          {
            str << " " << (*it);
          }

          TBSYS_LOG(WARN, "server: %s, blocks: %s not match", tbsys::CNetUtil::addrToString(iter->first).c_str(), str.str().c_str());
        }
      }
      NewClientManager::get_instance().destroy();
    }
    fclose(fp);
    fp = NULL;
  }
  return ret;
}
