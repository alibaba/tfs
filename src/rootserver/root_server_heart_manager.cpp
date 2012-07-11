/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: root_server_heart_manager.cpp 590 2011-09-28 10:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <zlib.h>
#include <Time.h>
#include "common/define.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/rts_define.h"
#include "common/atomic.h"
#include "common/parameter.h"
#include "common/client_manager.h"
#include "message/rts_rts_heart_message.h"
#include "message/get_tables_from_rts_message.h"
#include "root_server_heart_manager.h"
#include "meta_server_manager.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace rootserver
  {

    const int8_t RootServerHeartManager::MAX_RETRY_COUNT = 2;
    const int16_t RootServerHeartManager::MAX_TIMEOUT_MS = 200;//500ms

    RootServerHeartManager::RootServerHeartManager(MetaServerManager& manager):
      manager_(manager),
      check_thread_(0),
      lease_id_factory_(INVALID_LEASE_ID),
      keepalive_type_(RTS_RS_KEEPALIVE_TYPE_LOGIN),
      initialize_(false),
      destroy_(false)
    {

    }

    RootServerHeartManager::~RootServerHeartManager()
    {

    }

    int RootServerHeartManager::initialize(void)
    {
      int32_t iret = !initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        initialize_ = true;
        check_thread_ = new CheckThreadHelper(*this);
      }
      return iret;
    }

    int RootServerHeartManager::destroy()
    {
      initialize_ = false;
      destroy_ = true;
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.notifyAll();
      }

      if (0 != check_thread_)
      {
        check_thread_->join();
        check_thread_ = 0;
      }
      return TFS_SUCCESS;
    }

    bool RootServerHeartManager::exist(const uint64_t id)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      ROOT_SERVER_MAPS_ITER iter = servers_.find(id);
      return (initialize_ && (servers_.end() != iter));
    }

    bool RootServerHeartManager::lease_exist(const uint64_t id)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      ROOT_SERVER_MAPS_ITER iter = servers_.find(id);
      return initialize_
              && servers_.end() != iter
              && iter->second.lease_.has_valid_lease(now.toSeconds());
    }

    int RootServerHeartManager::keepalive(const int8_t type, common::RootServerInformation& server)
    {
      int32_t iret = TFS_ERROR;
      switch (type)
      {
      case RTS_MS_KEEPALIVE_TYPE_LOGIN:
        iret = register_(server);
        break;
      case RTS_MS_KEEPALIVE_TYPE_RENEW:
        iret = renew(server);
        break;
      case RTS_MS_KEEPALIVE_TYPE_LOGOUT:
        iret = unregister(server.base_info_.id_);
        break;
      default:
        TBSYS_LOG(ERROR, "%s keepalive, type: %d not found",
            tbsys::CNetUtil::addrToString(server.base_info_.id_).c_str(), type);
        break;
      }
      return iret;
    }

    int RootServerHeartManager::register_(common::RootServerInformation& server)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        RootServerInformation* pserver = NULL;
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        ROOT_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
        if (servers_.end() == iter)
        {
          std::pair<ROOT_SERVER_MAPS_ITER, bool> res =
            servers_.insert(ROOT_SERVER_MAPS::value_type(server.base_info_.id_, server));
          pserver =  &res.first->second;
        }
        else
        {
          pserver =  &iter->second;
        }
        memcpy(pserver, &server, sizeof(RootServerInformation));
        pserver->lease_.lease_id_ = new_lease_id();
        pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_RTSERVER.rts_rts_lease_expired_time_ ;
        pserver->base_info_.last_update_time_ = now.toSeconds();
        server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.rts_rts_lease_expired_time_;
      }
      return iret;
    }

    int RootServerHeartManager::unregister(const uint64_t id)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      ROOT_SERVER_MAPS_ITER iter = servers_.find(id);
      if (servers_.end() != iter)
      {
        servers_.erase(iter);
      }
      return TFS_SUCCESS;
    }

    int RootServerHeartManager::renew(common::RootServerInformation& server)
    {
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR ;
      if (TFS_SUCCESS == iret)
      {
        RootServerInformation* pserver = NULL;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        ROOT_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
        if (servers_.end() == iter)
        {
          iret = EXIT_REGISTER_NOT_EXIST_ERROR;
        }
        else
        {
          iret = iter->second.lease_.has_valid_lease(now.toSeconds()) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
          if (TFS_SUCCESS == iret)
          {
            pserver = &iter->second;
            iret = pserver->lease_.renew(SYSPARAM_RTSERVER.rts_rts_lease_expired_time_, now.toSeconds()) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
            if (TFS_SUCCESS == iret)
            {
              pserver->base_info_.last_update_time_ = now.toSeconds();
              pserver->base_info_.status_ = server.base_info_.status_;
              server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.rts_rts_lease_expired_time_;
            }
          }
        }
      }
      return iret;
    }

    int RootServerHeartManager::dump_rs_server(void)
    {
      return TFS_SUCCESS;
    }

    int RootServerHeartManager::dump_rs_server(common::Buffer& buffer)
    {
      UNUSED(buffer);
      return TFS_SUCCESS;
    }

    uint64_t RootServerHeartManager::new_lease_id(void)
    {
      uint64_t lease_id = atomic_inc(&lease_id_factory_);
      assert(lease_id <= UINT64_MAX - 1);
      return lease_id;
    }

    void RootServerHeartManager::check_rs_lease_expired_helper(const tbutil::Time& now)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      ROOT_SERVER_MAPS_ITER iter = servers_.begin();
      for (; iter != servers_.end(); )
      {
        if (!iter->second.lease_.has_valid_lease(now.toSeconds()))
        {
          TBSYS_LOG(DEBUG, "%s lease expired: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d",
            tbsys::CNetUtil::addrToString(iter->second.base_info_.id_).c_str(),
            iter->second.lease_.lease_expired_time_, (int64_t)now.toSeconds());
          servers_.erase(iter++);
        }
        else
          ++iter;
      }
      return;
    }

    void RootServerHeartManager::check(void)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t wait_time = RTS_RS_RENEW_LEASE_INTERVAL_TIME_DEFAULT;
      tbutil::Time lease_expired;
      int32_t iret = TFS_SUCCESS;
      while (!destroy_)
      {
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        RsRuntimeGlobalInformation& rgi = RsRuntimeGlobalInformation::instance();
        {
          RWLock::Lock lock(rgi, WRITE_LOCKER);
          rs_role_establish_helper(rgi, now.toSeconds());
        }

        if (RS_ROLE_MASTER == rgi.info_.base_info_.role_)
        {
          check_rs_lease_expired_helper(now);
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.timedWait(tbutil::Time::milliSeconds(100));
        }
        else
        {
          iret = keepalive(keepalive_type_, wait_time, lease_expired, rgi, now);
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.timedWait(tbutil::Time::seconds(wait_time));
        }
      }

      keepalive_type_ = RTS_RS_KEEPALIVE_TYPE_LOGOUT;
      RsRuntimeGlobalInformation& rgi = RsRuntimeGlobalInformation::instance();
      iret = keepalive(keepalive_type_, wait_time, lease_expired, rgi, now);
    }

    int RootServerHeartManager::rs_role_establish_helper(common::RsRuntimeGlobalInformation& rgi, const int64_t now)
    {
      if (check_vip_helper(rgi))//vip is local ip
      {
        if (RS_ROLE_SLAVE == rgi.info_.base_info_.role_)//switch
        {
          rgi.info_.base_info_.role_ = RS_ROLE_MASTER;
          rgi.switch_time_ = now;
          manager_.notifyAll();
          TBSYS_LOG(INFO, "rootserver switch, old role: slave, current role: master");
        }
      }
      else
      {
        if (RS_ROLE_MASTER == rgi.info_.base_info_.role_)
        {
          rgi.info_.base_info_.role_ = RS_ROLE_SLAVE;
          {
            tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
            servers_.clear();
          }
          TBSYS_LOG(INFO, "rootserver switch, old role: master, current role: slave");
        }
      }
      return TFS_SUCCESS;
    }

    bool RootServerHeartManager::check_vip_helper(RsRuntimeGlobalInformation& rgi)
    {
      assert(rgi.vip_ != 0);
      return Func::is_local_addr(rgi.vip_);
    }

    void RootServerHeartManager::CheckThreadHelper::run()
    {
      manager_.check();
    }

    int RootServerHeartManager::keepalive(common::RtsRsKeepAliveType& type, int32_t& wait_time, tbutil::Time& lease_expired,
          common::RsRuntimeGlobalInformation& rgi, const tbutil::Time& now)
    {
      int32_t iret = TFS_SUCCESS;
      bool has_valid_lease = ((now < lease_expired));
      if (!has_valid_lease)//relogin
        type = RTS_RS_KEEPALIVE_TYPE_LOGIN;

      uint64_t id = 0;
      uint64_t server = 0;
      RtsRsHeartMessage msg;
      msg.set_type(type);
      {
        RWLock::Lock lock(rgi, READ_LOCKER);
        server = rgi.other_id_;
        id = rgi.info_.base_info_.id_;
        msg.set_rs(rgi.info_);
      }
      NewClient* client = NULL;
      int32_t retry_count = 0;
      tbnet::Packet* response = NULL;
      do
      {
        ++retry_count;
        client = NewClientManager::get_instance().create_client();
        iret = send_msg_to_server(server, client, &msg, response, MAX_TIMEOUT_MS);
        if (TFS_SUCCESS == iret)
        {
          assert(NULL != response);
          RtsRsHeartResponseMessage* rmsg = dynamic_cast<RtsRsHeartResponseMessage*>(response);
          iret = rmsg->get_ret_value();
          if (TFS_SUCCESS == iret)
          {
            lease_expired = tbutil::Time::now(tbutil::Time::Monotonic) + tbutil::Time::seconds(rmsg->get_lease_expired_time());
            wait_time = rmsg->get_renew_lease_interval_time();
            if (RTS_RS_KEEPALIVE_TYPE_LOGIN == type)
              type = RTS_RS_KEEPALIVE_TYPE_RENEW;
            if (manager_.get_active_table_version() != rmsg->get_active_table_version())
            {
              iret = get_tables_from_rs(server);
              if (TFS_SUCCESS != iret)
              {
                TBSYS_LOG(ERROR, "failed to get buckets from %s",tbsys::CNetUtil::addrToString(server).c_str());
              }
            }
          }
          else if (EXIT_LEASE_EXPIRED == iret)
          {
            lease_expired = 0;
            TBSYS_LOG(WARN, "%s lease expired", tbsys::CNetUtil::addrToString(id).c_str());
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      while ((retry_count < MAX_RETRY_COUNT)
            && (TFS_SUCCESS != iret)
            && (!destroy_));
      return iret;
    }

    int RootServerHeartManager::get_tables_from_rs(const uint64_t server)
    {
      NewClient* client = NewClientManager::get_instance().create_client();
      GetTableFromRtsMessage msg;
      tbnet::Packet* response = NULL;
      int32_t iret = send_msg_to_server(server, client, &msg, response, MAX_TIMEOUT_MS);
      if (TFS_SUCCESS == iret)
      {
        assert(NULL != response);
        iret = response->getPCode() == RSP_RT_GET_TABLE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          GetTableFromRtsResponseMessage* reply = dynamic_cast<GetTableFromRtsResponseMessage*>(response);
          if (TABLE_VERSION_MAGIC != reply->get_version())
          {
            uint64_t dest_length = common::MAX_BUCKET_DATA_LENGTH;
            unsigned char* dest = new unsigned char[common::MAX_BUCKET_DATA_LENGTH];
            iret = uncompress(dest, &dest_length, (unsigned char*)reply->get_table(), reply->get_table_length());
            if (Z_OK != iret)
            {
              TBSYS_LOG(ERROR, "uncompress error: ret : %d, version: %"PRI64_PREFIX"d", iret, reply->get_version());
              iret = TFS_ERROR;
            }
            else
            {
              iret = manager_.update_active_tables(dest, dest_length, reply->get_version());
              if (TFS_SUCCESS != iret)
              {
                TBSYS_LOG(ERROR, "update_active_tables error: %d", iret);
              }
              else
              {
                manager_.dump_tables(TBSYS_LOG_LEVEL_DEBUG, DUMP_TABLE_TYPE_ACTIVE_TABLE);
              }
            }
            tbsys::gDeleteA(dest);
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"get table failed, response message is status message");
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return iret;
    }
  } /** root server **/
}/** tfs **/
