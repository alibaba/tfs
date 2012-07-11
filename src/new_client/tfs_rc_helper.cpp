/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_rc_helper.cpp 529 2011-06-20 09:29:28Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "tfs_rc_helper.h"
#include "common/rc_define.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/rc_session_message.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;


int RcHelper::login(const uint64_t rc_ip, const string& app_key, const uint64_t app_ip,
    string& session_id, BaseInfo& base_info)
{
  ReqRcLoginMessage req_login_msg;
  req_login_msg.set_app_key(app_key.c_str());
  req_login_msg.set_app_ip(app_ip);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(rc_ip, client, &req_login_msg, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call req login rc fail, rc_ip: %"PRI64_PREFIX"u, app_key: %s, app_ip: %"PRI64_PREFIX"u, ret: %d",
        rc_ip, app_key.c_str(), app_ip, ret);
  }
  else if (RSP_RC_LOGIN_MESSAGE == rsp->getPCode()) //rsp will not be null
  {
    RspRcLoginMessage* rsp_login_msg = dynamic_cast<RspRcLoginMessage*>(rsp);

    session_id = rsp_login_msg->get_session_id();
    base_info = rsp_login_msg->get_base_info();
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      TBSYS_LOG(ERROR, "call req login rc fail, rc_ip: %"PRI64_PREFIX"u, app_key: %s, app_ip: %"PRI64_PREFIX"u,"
          "ret: %d, error: %s, status: %d",
          rc_ip, app_key.c_str(), app_ip, ret,
          dynamic_cast<StatusMessage*>(rsp)->get_error(), dynamic_cast<StatusMessage*>(rsp)->get_status());
    }
    else
    {
      TBSYS_LOG(ERROR, "call req login rc fail, rc_ip: %"PRI64_PREFIX"u, app_key: %s, app_ip: %"PRI64_PREFIX"u, ret: %d, msg type: %d",
          rc_ip, app_key.c_str(), app_ip, ret, rsp->getPCode());
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int RcHelper::keep_alive(const uint64_t rc_ip, const common::KeepAliveInfo& ka_info,
    bool& update_flag, common::BaseInfo& base_info)
{
  ReqRcKeepAliveMessage req_ka_msg;
  req_ka_msg.set_ka_info(ka_info);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(rc_ip, client, &req_ka_msg, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call req keep alive rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d",
        rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret);
  }
  else if (RSP_RC_KEEPALIVE_MESSAGE == rsp->getPCode()) //rsp will not be null
  {
    RspRcKeepAliveMessage* rsp_ka_msg = dynamic_cast<RspRcKeepAliveMessage*>(rsp);

    update_flag = rsp_ka_msg->get_update_flag();
    if (update_flag)
    {
      base_info = rsp_ka_msg->get_base_info();
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      TBSYS_LOG(ERROR, "call req keep alive rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d, error: %s, status: %d",
          rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret,
          dynamic_cast<StatusMessage*>(rsp)->get_error(), dynamic_cast<StatusMessage*>(rsp)->get_status());
    }
    else
    {
      TBSYS_LOG(ERROR, "call req keep alive rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d, msg type: %d",
          rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret, rsp->getPCode());
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int RcHelper::logout(const uint64_t rc_ip, const common::KeepAliveInfo& ka_info)
{
  ReqRcLogoutMessage req_logout_msg;
  req_logout_msg.set_ka_info(ka_info);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(rc_ip, client, &req_logout_msg, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call req logout rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d",
        rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret);
  }
  else if (STATUS_MESSAGE == rsp->getPCode()) //rsp will not be null
  {
    if (STATUS_MESSAGE_OK != dynamic_cast<StatusMessage*>(rsp)->get_status())
    {
      ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
      TBSYS_LOG(ERROR, "call req logout rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d",
          rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret);
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    TBSYS_LOG(ERROR, "call req logout rc fail, rc_ip: %"PRI64_PREFIX"u, session_id: %s, ret: %d, msg type: %d",
        rc_ip, ka_info.s_base_info_.session_id_.c_str(), ret, rsp->getPCode());
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int RcHelper::reload(const uint64_t rc_ip, const common::ReloadType reload_type)
{
  ReqRcReloadMessage req_reload_msg;
  req_reload_msg.set_reload_type(reload_type);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(rc_ip, client, &req_reload_msg, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call req reload rc fail, rc_ip: %"PRI64_PREFIX"u, reload_type: %d, ret: %d",
        rc_ip, reload_type, ret);
  }
  else if (STATUS_MESSAGE == rsp->getPCode()) //rsp will not be null
  {
    if (STATUS_MESSAGE_OK != dynamic_cast<StatusMessage*>(rsp)->get_status())
    {
      ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
      TBSYS_LOG(ERROR, "call req reload rc fail, rc_ip: %"PRI64_PREFIX"u, reload_type: %d, ret: %d",
          rc_ip, reload_type, ret);
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    TBSYS_LOG(ERROR, "call req reload rc fail, rc_ip: %"PRI64_PREFIX"u, reload_type: %d, ret: %d, msg type: %d",
        rc_ip, reload_type, ret, rsp->getPCode());
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}
