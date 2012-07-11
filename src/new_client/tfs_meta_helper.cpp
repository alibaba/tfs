/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_helper.cpp 918 2011-10-14 03:16:21Z daoan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include <zlib.h>
#include "tfs_meta_helper.h"
#include "common/meta_server_define.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/meta_nameserver_client_message.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;


//static tfs::common::BasePacketStreamer gstreamer;
//static tfs::message::MessageFactory gfactory;
//
int NameMetaHelper::do_file_action(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
    const MetaActionOp action, const char* path, const char* new_path, const int64_t version_id)
{
  int ret = 0;
  if (NULL == path)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    //gstreamer.set_packet_factory(&gfactory);
    //NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

    FilepathActionMessage req_fa_msg;
    req_fa_msg.set_app_id(app_id);
    req_fa_msg.set_user_id(user_id);
    req_fa_msg.set_file_path(path);
    req_fa_msg.set_version(version_id);
    if (new_path != NULL)
    {
      req_fa_msg.set_new_file_path(new_path);
    }
    req_fa_msg.set_action(action);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_fa_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call req file path fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s, new_path: %s, action: %d ret: %d",
          server_id, app_id, user_id, path, new_path == NULL?"null":new_path, action, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "req file action return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "call req file path fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s, new_path: %s, action: %d, ret: %d: msg type: %d",
          server_id, app_id, user_id, path, new_path == NULL?"null":new_path, action, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}
int NameMetaHelper::do_write_file(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
    const char* path, const int64_t version_id, const FragInfo& frag_info)
{
  int ret = 0;
  if (NULL == path)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    //gstreamer.set_packet_factory(&gfactory);
    //NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

    WriteFilepathMessage req_fa_msg;
    req_fa_msg.set_app_id(app_id);
    req_fa_msg.set_user_id(user_id);
    req_fa_msg.set_file_path(path);
    req_fa_msg.set_frag_info(frag_info);
    req_fa_msg.set_version(version_id);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_fa_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call write file fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s, ret: %d",
          server_id, app_id, user_id, path, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "write file return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "write file fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s ret: %d: msg type: %d",
          server_id, app_id, user_id, path, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}
int NameMetaHelper::do_read_file(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
    const char* path, const int64_t offset, const int64_t size, const int64_t version_id,
    common::FragInfo& frag_info, bool& still_have)
{
  int ret = 0;
  if (NULL == path)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    //gstreamer.set_packet_factory(&gfactory);
    //NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

    ReadFilepathMessage req_fa_msg;
    req_fa_msg.set_app_id(app_id);
    req_fa_msg.set_user_id(user_id);
    req_fa_msg.set_file_path(path);
    req_fa_msg.set_offset(offset);
    req_fa_msg.set_size(size);
    req_fa_msg.set_version(version_id);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_fa_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call read file fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s, offset %"PRI64_PREFIX"d, size %"PRI64_PREFIX"d ret: %d",
          server_id, app_id, user_id, path, offset, size, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RESP_READ_FILEPATH_MESSAGE == rsp->getPCode())
    {
      RespReadFilepathMessage* resp_msg = dynamic_cast<RespReadFilepathMessage*>(rsp);
      frag_info = resp_msg->get_frag_info();
      still_have = resp_msg->get_still_have();
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "read file return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "call read file fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s ret: %d: msg type: %d",
          server_id, app_id, user_id, path, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int NameMetaHelper::do_ls(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
    const char* path, const common::FileType file_type, const int64_t pid, const int64_t version_id,
    std::vector<common::MetaInfo>& meta_infos, bool& still_have)
{
  int ret = 0;
  if (NULL == path)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    //gstreamer.set_packet_factory(&gfactory);
    //NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

    LsFilepathMessage req_fa_msg;
    req_fa_msg.set_app_id(app_id);
    req_fa_msg.set_user_id(user_id);
    req_fa_msg.set_pid(pid);
    req_fa_msg.set_file_path(path);
    req_fa_msg.set_file_type(file_type);
    req_fa_msg.set_version(version_id);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_fa_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call ls fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s, file_type: %d ret: %d",
          server_id, app_id, user_id, path, file_type, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RESP_LS_FILEPATH_MESSAGE == rsp->getPCode())
    {
      RespLsFilepathMessage* resp_msg = dynamic_cast<RespLsFilepathMessage*>(rsp);
      meta_infos = resp_msg->get_meta_infos();
      still_have = resp_msg->get_still_have();
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "ls return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "call ls fail,"
          "server_id: %"PRI64_PREFIX"u, app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d "
          "path: %s ret: %d: msg type: %d",
          server_id, app_id, user_id, path, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int NameMetaHelper::get_table(const uint64_t server_id,
    char* table_info, uint64_t& table_length, int64_t& version_id)
{
  //gstreamer.set_packet_factory(&gfactory);
  //NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  GetTableFromRtsMessage req_gtfr_msg;

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(server_id, client, &req_gtfr_msg, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call get table fail,"
        "server_id: %"PRI64_PREFIX"u, ret: %d", server_id, ret);
    ret = EXIT_NETWORK_ERROR;
  }
  else if (RSP_RT_GET_TABLE_MESSAGE == rsp->getPCode())
  {
    GetTableFromRtsResponseMessage* resp_msg = dynamic_cast<GetTableFromRtsResponseMessage*>(rsp);
    ret = uncompress(reinterpret_cast<unsigned char*> (table_info), &table_length, (unsigned char*)resp_msg->get_table(), resp_msg->get_table_length());
    if (Z_OK != ret)
    {
      TBSYS_LOG(ERROR, "uncompress error: ret : %d, version: %"PRI64_PREFIX"d, dest length: %"PRI64_PREFIX"d, lenght: %"PRI64_PREFIX"d",
          ret, resp_msg->get_version(), table_length, resp_msg->get_table_length());
      ret = TFS_ERROR;
    }
    else
    {
      version_id = resp_msg->get_version();
      ret = TFS_SUCCESS;
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    TBSYS_LOG(ERROR, "call get table fail,"
        "server_id: %"PRI64_PREFIX"u, ret: %d: msg type: %d",
        server_id, ret, rsp->getPCode());
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}
