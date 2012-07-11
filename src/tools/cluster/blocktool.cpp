/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blocktool.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <bitset>

#include "common/config_item.h"
#include "common/func.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/block_info_message.h"
#include "new_client/fsname.h"
#include "new_client/tfs_client_api.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

int list_block(TfsClient* tfs_client, const uint64_t ds_id);
int get_block_copys(TfsClient* tfs_client, const uint64_t ds_id, VUINT32* vec);
void print_block(VUINT32* vec);

static const int32_t MAX_BITS_SIZE = 81920;
static bitset<MAX_BITS_SIZE> g_flag_;

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d [-h]\n", name);
  fprintf(stderr, "       -s nameserver ip port\n");
  fprintf(stderr, "       -d dataserver ip port\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int main(int argc, char* argv[])
{
  int32_t i;
  std::string ns_ip = "", ds_ip = "";
  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      ns_ip = optarg;
      break;
    case 'd':
      ds_ip = optarg;
      break;
    case 'h':
    default:
      usage(argv[0]);
    }
  }

  if ((ns_ip.empty())
      || (ns_ip.compare(" ") == 0)
      || (ds_ip.empty())
      || (ds_ip.compare(" ") == 0))
  {
    usage(argv[0]);
  }

  TfsClient* tfs_client = TfsClient::Instance();
  int iret = tfs_client->initialize(ns_ip.c_str());
  BasePacketFactory factory;
  BasePacketStreamer streamer;
  NewClientManager::get_instance().initialize(&factory, &streamer);
  if (iret != TFS_SUCCESS)
  {
    return TFS_ERROR;
  }
  list_block(tfs_client, Func::get_host_ip(ds_ip.c_str()));

  return TFS_SUCCESS;
}

int list_block(TfsClient* tfs_client, const uint64_t ds_id)
{
  int ret_status = TFS_ERROR;
  ListBlockMessage req_lb_msg;
  req_lb_msg.set_block_type(1);

  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* ret_msg= NULL;
    if (TFS_SUCCESS == send_msg_to_server(ds_id, client, &req_lb_msg, ret_msg))
    {
      vector<uint32_t>* block_vec = NULL;
      TBSYS_LOG(DEBUG, "ds_id: %lu", ds_id);
      if ((ret_status == TFS_SUCCESS) && (ret_msg->getPCode() == RESP_LIST_BLOCK_MESSAGE))
      {
        RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*> (ret_msg);

        block_vec = const_cast<VUINT32*> (resp_lb_msg->get_blocks());

        get_block_copys(tfs_client, ds_id, block_vec);
        print_block(block_vec);
      }
    }

    NewClientManager::get_instance().destroy_client(client);
  }
  return ret_status;
}
int get_block_copys(TfsClient* tfs_client, uint64_t ds_id, VUINT32* vec)
{
  vector<uint32_t>::iterator iter = vec->begin();
  for (; iter != vec->end(); iter++)
  {
    VUINT64 ds_list;
    int ret = ToolUtil::get_block_ds_list(tfs_client->get_server_id(), (*iter), ds_list);
    if (ret != TFS_SUCCESS)
    {
      fprintf(stderr, "block no exist in nameserver, blockid:%u.\n", (*iter));
      return ret;
    }
    int32_t ds_size = static_cast<int32_t> (ds_list.size());
    if ((ds_size == 1) && (ds_list[0] == ds_id))
    {
      g_flag_.set( distance(vec->begin(), iter) );
    }
    else
    {
      g_flag_.reset( distance(vec->begin(), iter) );
    }
  }

  return TFS_SUCCESS;
}
void print_block(VUINT32* vec)
{
  vector<uint32_t>::iterator vit = vec->begin();
  for (; vit != vec->end(); vit++)
  {
    int32_t i = distance(vec->begin(), vit);
    if (g_flag_[i])
    {
      fprintf(stdout, "%d\n", *vit);
    }
  }
}
