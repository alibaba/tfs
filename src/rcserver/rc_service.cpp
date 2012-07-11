/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rc_service.cpp 520 2011-06-20 06:05:52Z daoan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "rc_service.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "common/parameter.h"
#include "message/message_factory.h"
#include "resource_manager.h"
#include "session_manager.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    using namespace message;
    using namespace std;

    RcService::RcService() :
      resource_manager_(NULL), session_manager_(NULL)
    {
    }

    RcService::~RcService()
    {
      tbsys::gDelete(resource_manager_);
      tbsys::gDelete(session_manager_);
    }

    const char* RcService::get_log_file_path()
    {
      return NULL;
    }

    const char* RcService::get_pid_file_path()
    {
      return NULL;
    }

    bool RcService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      int ret = true;
      BasePacket* base_packet = NULL;
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        base_packet = dynamic_cast<BasePacket*>(packet);
        switch (base_packet->getPCode())
        {
          case REQ_RC_LOGIN_MESSAGE:
            ret = req_login(base_packet);
            break;
          case REQ_RC_KEEPALIVE_MESSAGE:
            ret = req_keep_alive(base_packet);
            break;
          case REQ_RC_LOGOUT_MESSAGE:
            ret = req_logout(base_packet);
            break;
          default:
            ret = EXIT_UNKNOWN_MSGTYPE;
            TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
            break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
      }
      // always return true. packet will be freed by caller
      return true;
    }

    int RcService::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);
      int ret = TFS_SUCCESS;
      uint32_t dev_ip = Func::get_local_addr(get_dev());
      uint32_t cfg_ip = Func::get_addr(get_ip_addr());
      if (dev_ip != cfg_ip)
      {
        ret = EXIT_CONFIG_ERROR;
        TBSYS_LOG(ERROR, "call RcServerParameter::initialize fail. dev name: %s, cfg ip: %s, dev id: %u, cfg id: %u, ret: %d",
            get_dev(), get_ip_addr(), dev_ip, cfg_ip, ret);
      }
      else
      {
        if ((ret = RcServerParameter::instance().initialize()) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call RcServerParameter::initialize fail. ret: %d", ret);
        }
        else
        {
          resource_manager_ = new ResourceManager(get_timer());
          if ((ret = resource_manager_->initialize()) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call ResourceManager::initialize fail. ret: %d", ret);
          }
          else
          {
            session_manager_ = new SessionManager(resource_manager_, get_timer());
            if ((ret = session_manager_->initialize()) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "call SessionManager::initialize fail. ret: %d", ret);
            }
            //TBSYS_LOG(INFO, "init ok ========");
          }
        }
      }

      return ret;
    }

    int RcService::destroy_service()
    {
      int ret = TFS_SUCCESS;
      if (NULL != session_manager_)
        session_manager_->stop();
      if (NULL != resource_manager_)
        resource_manager_->stop();
      return ret;
    }

    int RcService::req_login(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_login fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcLoginMessage* req_login_msg = dynamic_cast<ReqRcLoginMessage*>(packet);
        string session_id;
        BaseInfo base_info;
        TBSYS_LOG(DEBUG, "call RspRcLoginMessage::login start. app_key: %s, app_ip: %"PRI64_PREFIX"d, ret: %d",
            req_login_msg->get_app_key(), req_login_msg->get_app_ip(), ret);
        if ((ret = session_manager_->login(req_login_msg->get_app_key(),
                req_login_msg->get_app_ip(), session_id, base_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::login fail. app_key: %s, app_ip: %"PRI64_PREFIX"d, ret: %d",
            req_login_msg->get_app_key(), req_login_msg->get_app_ip(), ret);
        }
        else
        {
          RspRcLoginMessage* rsp_login_msg = new RspRcLoginMessage();
          if ((ret = rsp_login_msg->set_session_id(session_id.c_str())) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call RspRcLoginMessage::set_session_id fail. ret: %d", ret);
          }
          else
          {
            base_info.dump();
            rsp_login_msg->set_base_info(base_info);
            int inner_ret = req_login_msg->reply(rsp_login_msg);
            if (TFS_SUCCESS != inner_ret)
            {
              TBSYS_LOG(DEBUG, "call RspRcLoginMessage::login fail. app_key: %s, app_ip: %"PRI64_PREFIX"d, inner_ret: %d",
                  req_login_msg->get_app_key(), req_login_msg->get_app_ip(), inner_ret);
            }
            TBSYS_LOG(DEBUG, "call RspRcLoginMessage::login end. app_key: %s, app_ip: %"PRI64_PREFIX"d, session_id: %s, ret: %d",
                req_login_msg->get_app_key(), req_login_msg->get_app_ip(), session_id.c_str(), ret);
          }
        }
      }
      return ret;
    }

    int RcService::req_keep_alive(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_keep_alive fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcKeepAliveMessage* req_ka_msg = dynamic_cast<ReqRcKeepAliveMessage*>(packet);
        const KeepAliveInfo& ka_info = req_ka_msg->get_ka_info();
        bool update_flag = false;

        //map<OperType, AppOperInfo>::const_iterator it = ka_info.s_stat_.app_oper_info_.find(OPER_READ);
        //if (it != ka_info.s_stat_.app_oper_info_.end())
        //{
        //TBSYS_LOG(INFO, "===================== times %d", it->second.oper_times_);
        //}
        ///////////////////////
        BaseInfo base_info;
        if ((ret = session_manager_->keep_alive(ka_info.s_base_info_.session_id_,
                ka_info, update_flag, base_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::keep_alive fail. session_id: %s, ret: %d",
              ka_info.s_base_info_.session_id_.c_str(), ret);
        }
        else
        {
          RspRcKeepAliveMessage* rsp_ka_msg = new RspRcKeepAliveMessage();
          rsp_ka_msg->set_update_flag(update_flag);
          if (update_flag)
          {
            rsp_ka_msg->set_base_info(base_info);
          }
          req_ka_msg->reply(rsp_ka_msg);
        }
      }
      return ret;
    }

    int RcService::req_logout(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_logout fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcLogoutMessage* req_logout_msg = dynamic_cast<ReqRcLogoutMessage*>(packet);
        const KeepAliveInfo& ka_info = req_logout_msg->get_ka_info();
        if ((ret = session_manager_->logout(ka_info.s_base_info_.session_id_,
                ka_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::logout fail. session_id: %s, ret: %d",
              ka_info.s_base_info_.session_id_.c_str(), ret);
        }
        else
        {
          ret = req_logout_msg->reply(new StatusMessage(ret));
        }
      }
      return ret;
    }

    int RcService::req_reload(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "RcService::req_reload fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReqRcReloadMessage* req_reload_msg = dynamic_cast<ReqRcReloadMessage*>(packet);
        ReloadType reload_type = req_reload_msg->get_reload_type();
        if (RELOAD_CONFIG == reload_type)
        {
          ret = req_reload_config();
        }
        else if (RELOAD_RESOURCE == reload_type)
        {
          ret = req_reload_resource();
        }
        else
        {
          ret = EXIT_INVALID_ARGU;
          TBSYS_LOG(ERROR, "call RcService::req_reload fail. reload type: %d, ret: %d", reload_type, ret);
        }

         req_reload_msg->reply(new StatusMessage(ret));
      }
      return TFS_SUCCESS;
    }

    int RcService::req_reload_config()
    {
      int ret = TFS_SUCCESS;
      if (NULL == resource_manager_ || NULL == session_manager_)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "call RcService::req_reload_config fail. resource_manager_ or session_manager_ is null, ret: %d", ret);
      }
      else
      {
        tbutil::Mutex::Lock lock(mutex_);
        // reload config
        TBSYS_CONFIG.load(config_file_.c_str());
        //BaseService::reload();
        if ((ret = RcServerParameter::instance().initialize()) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call RcServerParameter::initialize fail. ret: %d", ret);
        }
        else
        {
          //stop task
          session_manager_->stop();
          resource_manager_->stop();

          int retry_times = 3;
          while (retry_times > 0)
          {
            if ((ret = resource_manager_->initialize()) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "call resource_manager::initialize fail. retry time: %d, ret: %d", retry_times, ret);
            }
            else
            {
              break;
            }
            --retry_times;
          }

          if (TFS_SUCCESS == ret)
          {
            retry_times = 3;
            while (retry_times > 0)
            {
              if ((ret = session_manager_->start()) != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "call SessionManager::start fail. retry time: %d, ret: %d", retry_times, ret);
              }
              else
              {
                break;
              }
              --retry_times;
            }
          }
        }
      }

      return ret;
    }

    int RcService::req_reload_resource()
    {
      return resource_manager_->load();
    }
  }
}
