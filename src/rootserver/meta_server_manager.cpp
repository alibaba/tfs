/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_manager.cpp 590 2011-08-17 16:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <Time.h>
#include "common/define.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/rts_define.h"
#include "common/atomic.h"
#include "common/parameter.h"
#include "meta_server_manager.h"
#include "global_factory.h"

using namespace tfs::common;

namespace tfs
{
  namespace rootserver
  {
    MetaServerManager::MetaServerManager():
      lease_id_factory_(INVALID_LEASE_ID),
      build_table_thread_(0),
      check_ms_lease_thread_(0),
      interrupt_(INTERRUPT_NONE),
      initialize_(false),
      destroy_(false)
    {

    }

    MetaServerManager::~MetaServerManager()
    {

    }

    int MetaServerManager::initialize(const std::string& table_file_path)
    {
      int32_t iret = !initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = table_file_path.empty() ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          iret = build_tables_.intialize(table_file_path);
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "initialize build table manger fail: %d", iret);
          }
        }
        if (TFS_SUCCESS == iret)
        {
          build_table_thread_ = new BuildTableThreadHelper(*this);
          check_ms_lease_thread_ = new CheckMetaServerLeaseThreadHelper(*this);
          initialize_ = true;
        }
      }
      return iret;
    }

    int MetaServerManager::destroy()
    {
      initialize_ = false;
      destroy_ = true;
      servers_.clear();

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        build_table_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
        check_ms_lease_monitor_.notifyAll();
      }
      if (0 != build_table_thread_)
      {
        build_table_thread_->join();
      }
      if (0 != check_ms_lease_thread_ )
      {
        check_ms_lease_thread_ ->join();
      }
      return build_tables_.destroy();
    }

    bool MetaServerManager::exist(const uint64_t id)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return (initialize_ && (servers_.end() != iter));
    }

    bool MetaServerManager::lease_exist(const uint64_t id)
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return initialize_
              && servers_.end() != iter
              && iter->second.lease_.has_valid_lease(now.toSeconds());
    }

    int MetaServerManager::keepalive(const int8_t type, common::MetaServer& server)
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

    int MetaServerManager::register_(common::MetaServer& server)
    {
      bool exist = false;
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        MetaServer* pserver = NULL;
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        META_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
        if (servers_.end() == iter)
        {
          std::pair<META_SERVER_MAPS_ITER, bool> res =
            servers_.insert(META_SERVER_MAPS::value_type(server.base_info_.id_, server));
          pserver =  &res.first->second;
        }
        else
        {
          pserver = &iter->second;
          exist = pserver->lease_.has_valid_lease(now.toSeconds());
        }
        memcpy(pserver, &server, sizeof(MetaServer));
        pserver->lease_.lease_id_ = new_lease_id();
        pserver->lease_.lease_expired_time_ = now.toSeconds() + SYSPARAM_RTSERVER.mts_rts_lease_expired_time_ ;
        pserver->base_info_.last_update_time_ = now.toSeconds();
        server.tables_.version_ = build_tables_.get_active_table_version();
        server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.mts_rts_lease_expired_time_ ;
      }
      if (TFS_SUCCESS == iret
          && !exist)
      {
        TBSYS_LOG(DEBUG, "%s register successful", tbsys::CNetUtil::addrToString(server.base_info_.id_).c_str());
        interrupt();
      }
      return iret;
    }

    int MetaServerManager::unregister(const uint64_t id)
    {
      int32_t iret = TFS_SUCCESS;
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        META_SERVER_MAPS_ITER iter = servers_.find(id);
        iret = (servers_.end() != iter) ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          servers_.erase(iter);
        }
      }
      if (TFS_SUCCESS == iret)
      {
        TBSYS_LOG(DEBUG, "%s unregister successful", tbsys::CNetUtil::addrToString(id).c_str());
        interrupt();
      }
      return TFS_SUCCESS;
    }

    int MetaServerManager::renew(common::MetaServer& server)
    {
      int32_t iret = initialize_ ? TFS_SUCCESS : TFS_ERROR ;
      if (TFS_SUCCESS == iret)
      {
        MetaServer* pserver = NULL;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        //RWLock::Lock lock(mutex_, WRITE_LOCKER);
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        META_SERVER_MAPS_ITER iter = servers_.find(server.base_info_.id_);
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
            iret = pserver->lease_.renew(SYSPARAM_RTSERVER.mts_rts_lease_expired_time_, now.toSeconds()) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
            if (TFS_SUCCESS == iret)
            {
              pserver->base_info_.update(server.base_info_, now.toSeconds());
              memcpy(&pserver->throughput_, &server.throughput_, sizeof(MetaServerThroughput));
              memcpy(&pserver->net_work_stat_, &server.net_work_stat_, sizeof(NetWorkStatInformation));
              memcpy(&pserver->capacity_, &server.capacity_, sizeof(MetaServerCapacity));
              server.tables_.version_ = build_tables_.get_active_table_version();
              server.lease_.lease_expired_time_ = SYSPARAM_RTSERVER.mts_rts_lease_expired_time_;
            }
          }
        }
      }
      return iret;
    }

    int MetaServerManager::dump_meta_server(void)
    {
      return TFS_SUCCESS;
    }

    int MetaServerManager::dump_meta_server(common::Buffer& buffer)
    {
      UNUSED(buffer);
      return TFS_SUCCESS;
    }

    int MetaServerManager::check_ms_lease_expired(void)
    {
      tbutil::Time now;
      bool interrupt_flag = false;
      while (!destroy_)
      {
        RsRuntimeGlobalInformation& rgi = GFactory::get_runtime_info();
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        if (rgi.in_safe_mode_time(now.toSeconds()))
        {
          Func::sleep(SYSPARAM_RTSERVER.safe_mode_time_, destroy_);
        }
        else
        {
          if (!rgi.is_master())
          {
            tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
            check_ms_lease_monitor_.wait();
          }
          else
          {
            now = tbutil::Time::now(tbutil::Time::Monotonic);
            check_ms_lease_expired_helper(now, interrupt_flag);
            if (interrupt_flag)
            {
              interrupt_flag = false;
              interrupt();
            }
            tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
            check_ms_lease_monitor_.timedWait(tbutil::Time::milliSeconds(500));
          }
        }
     }
      return TFS_SUCCESS;
    }

    int MetaServerManager::switch_table(NEW_TABLE& tables)
    {
      int32_t iret = initialize_ && !tables.empty() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.switch_table();
      }
      return iret;
    }

    int MetaServerManager::update_tables_item_status(const uint64_t server, const int64_t version,
                                    const int8_t status, const int8_t phase)
    {
      int32_t iret  = initialize_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        TBSYS_LOG(DEBUG, "update tables item status: version: %"PRI64_PREFIX"d, status: %d, phase: %d", version, status, phase);
        iret = build_tables_.update_tables_item_status(server, version, status, phase, new_tables_);
      }
      return iret;
    }

    int MetaServerManager::get_tables(char* tables, int64_t& length, int64_t& version)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      int32_t iret = NULL == tables || length < build_tables_.get_active_table_length()
        ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = NULL == build_tables_.get_active_table() ? EXIT_NEW_TABLE_NOT_EXIST : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.get_active_table_version() <= INVALID_TABLE_VERSION ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.get_active_table_length() <= 0 ? EXIT_NEW_TABLE_NOT_EXIST : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == iret)
      {
        version = build_tables_.get_active_table_version();
        length  = build_tables_.get_active_table_length();
        memcpy(tables, build_tables_.get_active_table(), build_tables_.get_active_table_length());
      }
      return iret;
    }

    int MetaServerManager::update_active_tables(const unsigned char* tables, const int64_t length, const int64_t version)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      return build_tables_.update_active_tables(tables, length, version);
    }

    void MetaServerManager::dump_tables(int32_t level,const int8_t type,
        const char* file, const int32_t line,
        const char* function) const
    {
      build_tables_.dump_tables(level, type, file, line, function);
    }

    int64_t MetaServerManager::get_active_table_version() const
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      return build_tables_.get_active_table_version();
    }

    void MetaServerManager::build_table(void)
    {
      tbutil::Time now;
      bool update_complete = false;
      int8_t phase = UPDATE_TABLE_PHASE_1;
      while (!destroy_)
      {
        RsRuntimeGlobalInformation& rgi = GFactory::get_runtime_info();
        now = tbutil::Time::now(tbutil::Time::Monotonic);
        if (rgi.in_safe_mode_time(now.toSeconds()))
        {
          Func::sleep(SYSPARAM_RTSERVER.safe_mode_time_, destroy_);
          interrupt();
        }
        else
        {
          if (!rgi.is_master())
          {
            tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
            build_table_monitor_.wait();
          }
          else
          {
            tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
            if(!Func::test_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL)//no rebuild && no update tables, wait
                && new_tables_.empty())
            {
              build_table_monitor_.wait();
            }

            if (Func::test_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL))//rebuild
            {
              build_table_helper(phase, new_tables_, update_complete);
            }
            else if (!new_tables_.empty())//check update tables status
            {
              build_table_monitor_.timedWait(tbutil::Time::milliSeconds(100));//100ms
              update_table_helper(phase, new_tables_, update_complete);
            }
          }
        }
      }
      return;
    }

    uint64_t MetaServerManager::new_lease_id(void)
    {
      uint64_t lease_id = atomic_inc(&lease_id_factory_);
      assert(lease_id <= UINT64_MAX - 1);
      return lease_id;
    }

    void MetaServerManager::interrupt(void)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      Func::set_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL);
      build_table_monitor_.notifyAll();
    }

    void MetaServerManager::check_ms_lease_expired_helper(const tbutil::Time& now, bool& interrupt)
    {
      TBSYS_LOG(INFO, "check_ms_lease_expired_helper start");
      //RWLock::Lock lock(mutex_, WRITE_LOCKER);
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      META_SERVER_MAPS_ITER iter = servers_.begin();
      for (; iter != servers_.end(); )
      {
        TBSYS_LOG(INFO, "%s now time: %ld, lease_id: %"PRI64_PREFIX"u, lease_expired_time: %"PRI64_PREFIX"d",
          tbsys::CNetUtil::addrToString(iter->first).c_str(), (int64_t)now.toSeconds(),
          iter->second.lease_.lease_id_, iter->second.lease_.lease_expired_time_);
        if (!iter->second.lease_.has_valid_lease(now.toSeconds()))
        {
          interrupt = true;
          TBSYS_LOG(INFO, "%s lease expired, must be delete", tbsys::CNetUtil::addrToString(iter->first).c_str());
          servers_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      return;
    }

    int MetaServerManager::build_table_helper(int8_t& phase, NEW_TABLE& tables, bool& update_complete)
    {
      bool change = false;
      tables.clear();
      phase = UPDATE_TABLE_PHASE_1;
      std::set<uint64_t> servers;
      Func::clr_bit(interrupt_, BUILD_TABLE_INTERRUPT_ALL);
      //RWLock::Lock lock(mutex_, READ_LOCKER);
      META_SERVER_MAPS_CONST_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        servers.insert(iter->first);
      }
      int32_t iret = build_tables_.build_table(change, tables, servers);
      if (TFS_SUCCESS == iret)
      {
        if (change)
        {
          build_tables_.update_table(phase, tables, update_complete);
        }
        TBSYS_LOG(INFO, "build table complete, change: %s", change ? "true" : "false");
      }
      return iret;
    }

    int MetaServerManager::update_table_helper(int8_t& phase, NEW_TABLE& tables, bool& update_complete)
    {
      int32_t iret = tables.empty() ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = build_tables_.check_update_table_complete(phase, tables, update_complete);//check update status
        if (TFS_SUCCESS == iret)
        {
          if (update_complete)
          {
            if (UPDATE_TABLE_PHASE_1 == phase)
            {
              iret = switch_table(tables);
              if (TFS_SUCCESS == iret)
              {
                TBSYS_LOG(INFO, "update table complete, version: %"PRI64_PREFIX"d, phase: %d",
                  build_tables_.get_active_table_version(), phase);
                update_complete = false;
                phase = UPDATE_TABLE_PHASE_2;
                build_tables_.update_table(phase, tables, update_complete);
              }
            }
            else
            {
              tables.clear();
              TBSYS_LOG(INFO, "update table complete, version: %"PRI64_PREFIX"d, phase: %d",
                  build_tables_.get_active_table_version(), phase);
            }
          }
        }
      }
      return iret;
    }

    common::MetaServer* MetaServerManager::get(const uint64_t id)
    {
      //RWLock::Lock lock(mutex_, READ_LOCKER);
      tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
      META_SERVER_MAPS_ITER iter = servers_.find(id);
      return servers_.end() != iter && initialize_ ? &iter->second : NULL;
    }

    void MetaServerManager::notifyAll(void)
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_ms_lease_monitor_);
        check_ms_lease_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_table_monitor_);
        build_table_monitor_.notifyAll();
      }
    }

    void MetaServerManager::BuildTableThreadHelper::run()
    {
      manager_.build_table();
    }

    void MetaServerManager::CheckMetaServerLeaseThreadHelper::run()
    {
      manager_.check_ms_lease_expired();
    }
  } /** root server **/
}/** tfs **/
