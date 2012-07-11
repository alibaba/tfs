/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: repair_block_info.cpp 1000 2012-06-24 02:40:09Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "tbsys.h"

#include "common/internal.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/meta_server_define.h"
#include "message/server_status_message.h"
#include "message/client_cmd_message.h"
#include "message/message_factory.h"
#include "common/base_packet_streamer.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

int get_block_ds_list(const uint64_t server_id, const uint32_t block_id, VUINT64& ds_list)
{
  int ret = TFS_SUCCESS;
  if (false == NewClientManager::get_instance().is_init())
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "client manager not init.");
  }
  else if (0 == server_id)
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "server is is invalid: %"PRI64_PREFIX"u", server_id);
  }
  else
  {
    GetBlockInfoMessage gbi_message(T_READ);
    gbi_message.set_block_id(block_id);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &gbi_message, rsp);

    if (rsp != NULL)
    {
      if (rsp->getPCode() == SET_BLOCK_INFO_MESSAGE)
      {
        ds_list = dynamic_cast<SetBlockInfoMessage*>(rsp)->get_block_ds();
      }
      else if (rsp->getPCode() == STATUS_MESSAGE)
      {
        ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
        TBSYS_LOG(ERROR, "get block info fail, error: %s\n,", dynamic_cast<StatusMessage*>(rsp)->get_error());
        ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "get NULL response message, ret: %d\n", ret);
    }
    NewClientManager::get_instance().destroy_client(client);
  }

  return ret;
}

int repair_block_info(const uint64_t server_id, const uint32_t block_id)
{
  int ret = TFS_SUCCESS;
  if (false == NewClientManager::get_instance().is_init())
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "client manager not init.");
  }
  else if (0 == server_id)
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "server is is invalid: %"PRI64_PREFIX"u", server_id);
  }
  else
  {
    VUINT64 ds_list;
    ret = get_block_ds_list(server_id, block_id, ds_list);
    if (TFS_SUCCESS == ret)
    {
      for (uint32_t i = 0; i < ds_list.size(); i++)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        if(NULL == client)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "create client fail, ret: %d", ret);
        }
        else
        {
          CheckBlockRequestMessage req_cb_msg;
          req_cb_msg.set_block_id(block_id);
          req_cb_msg.set_check_flag(1);  // none zero, repair block
          tbnet::Packet* ret_msg = NULL;
          ret = send_msg_to_server(ds_list[i], client, &req_cb_msg, ret_msg);
          if (NULL != ret_msg)
          {
            if (ret_msg->getPCode() == RSP_CHECK_BLOCK_MESSAGE)
            {
              TBSYS_LOG(DEBUG, "repair block %u request succeed.", block_id);
            }
            else
            {
              StatusMessage* status_msg = dynamic_cast<StatusMessage*>(ret_msg);
              TBSYS_LOG(ERROR, "%u %s %d", block_id, status_msg->get_error(), status_msg->get_status());
            }
          }
          else
          {
            TBSYS_LOG(WARN, "check block %u, dataserver %s request fail, ret: %d",
                block_id, Func::addr_to_str(ds_list[i], true).c_str(), ret);
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
    }
  }
  return ret;
}

static void usage(const char* name)
{
  fprintf(stderr,
      "Usage: %s -s nsip -l loglevel -f filelist -h\n"
      "       -s nameserver ip port\n"
      "       -l set log level\n"
      "       -f block list\n"
      "       -h help\n",
      name);
}

int main(int argc, char* argv[])
{
  int32_t i;
  int ret = TFS_SUCCESS;
  string log_level = "debug";
  string nsip = " ";
  string blk_list = " ";

  // analyze arguments
  while ((i = getopt(argc, argv, "s:f:l:h")) != EOF)
  {
    switch (i)
    {
      case 'l':
        log_level = optarg;
        break;
      case 's':
        nsip = optarg;
        break;
      case 'f':
        blk_list = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }

  if (nsip == " " || blk_list == " ")
  {
    usage(argv[0]);
    ret = TFS_ERROR;
  }

  TBSYS_LOGGER.setLogLevel(log_level.c_str());

  MessageFactory factory;
  BasePacketStreamer streamer(&factory);
  ret = NewClientManager::get_instance().initialize(&factory, &streamer);
  if (TFS_SUCCESS == ret)
  {
    uint64_t server_id = 0;
    vector<string> fields;
    Func::split_string(nsip.c_str(), ':', fields);
    if (2 == fields.size())
    {
      server_id = Func::str_to_addr(fields[0].c_str(), atoi(fields[1].c_str()));
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid ns ip");
      ret = TFS_ERROR;
    }

    if (TFS_SUCCESS == ret && 0 != server_id)
    {
      FILE* fp = fopen(blk_list.c_str(), "r");
      if (NULL == fp)
      {
        TBSYS_LOG(ERROR, "open file %s error.", blk_list.c_str());
        ret = TFS_ERROR;
      }
      else
      {
        char line[128];
        while (fgets(line, 128, fp) != NULL)
        {
          if (*(line + strlen(line) - 1) == '\n')
          {
            *(line + strlen(line) - 1) = '\0';
          }
          uint32_t block_id = atoi(line);
          ret = repair_block_info(server_id, block_id);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(INFO, "repair block %u succeed.", block_id);
          }
          else
          {
            TBSYS_LOG(WARN, "repair block %u fail, ret: %d", block_id, ret);
          }
        }
      }
    }
    NewClientManager::get_instance().destroy();
  }

  return 0;
}

