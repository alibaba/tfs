/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: session_manager.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "session_manager.h"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "common/session_util.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    using namespace std;

    map<OperType, string> ISessionTask::key_map_;

    SessionManager::SessionManager(IResourceManager* resource_manager, tbutil::TimerPtr timer)
      : resource_manager_(resource_manager), timer_(timer),
      monitor_task_(0), stat_task_(0), is_init_(false)
    {
    }
    SessionManager::~SessionManager()
    {
      clean_task();
      is_init_ = false;
    }

    int SessionManager::initialize(bool reload_flag)
    {
      int ret = TFS_SUCCESS;
      if (NULL == resource_manager_ || 0 == timer_ || (!is_init_ && reload_flag) || (is_init_ && !reload_flag))
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "call ResourceManager::initialize failed, is_init_: %d, reload_flag: %d, ret: %d",
            is_init_, reload_flag, ret);
      }
      else
      {
        if ((!is_init_ && !reload_flag) || (is_init_ && reload_flag))
        {
          tbutil::Mutex::Lock lock(mutex_);
          if ((!is_init_ && !reload_flag) || (is_init_ && reload_flag))
          {
            if (is_init_ && reload_flag)
            {
              stop();
              clean_task();
            }

            monitor_task_ = new SessionMonitorTask(*this);
            stat_task_ = new SessionStatTask(*this, resource_manager_);

            if ((ret = start()) != TFS_SUCCESS)
            {
              clean_task();
            }
            else
            {
              is_init_ = true;
              ret = TFS_SUCCESS;
            }
          }
        }
      }

      return ret;
    }

    void SessionManager::clean_task()
    {
      monitor_task_ = 0;
      stat_task_ = 0;
    }

    void SessionManager::stop()
    {
      if (0 != monitor_task_)
      {
        monitor_task_->stop();
        if (0 != timer_)
        {
          timer_->cancel(monitor_task_);
        }
      }
      if (0 != stat_task_)
      {
        stat_task_->stop();
        if (0 != timer_)
        {
          timer_->cancel(stat_task_);
        }
      }
    }

    int SessionManager::start()
    {
      int ret = TFS_SUCCESS;
      if (0 == monitor_task_ || 0 == stat_task_ || 0 == timer_)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "call SessionManager::start failed, monitor task or stat task or timer is null, ret: %d", ret);
      }
      else if (((ret = timer_->scheduleRepeated(monitor_task_, tbutil::Time::seconds(SYSPARAM_RCSERVER.monitor_interval_))) != 0)
          || ((ret = timer_->scheduleRepeated(stat_task_, tbutil::Time::seconds(SYSPARAM_RCSERVER.stat_interval_))) != 0)
         )
      { //schedule fail. will not happen
        TBSYS_LOG(ERROR, "call scheduleRepeated failed, stat_interval_: %"PRI64_PREFIX"d, monitor_interval_: %"PRI64_PREFIX"d,"
            " ret: %d",
            SYSPARAM_RCSERVER.stat_interval_, SYSPARAM_RCSERVER.monitor_interval_, ret);

        // do not destroy task, just stop
        stop();
        ret = TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        monitor_task_->start();
        stat_task_->start();
      }
      return ret;
    }

    int SessionManager::login(const std::string& app_key, const int64_t session_ip,
        std::string& session_id, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      int32_t app_id = 0;
      BaseInfo base_info_befor_sort;
      if ((ret = resource_manager_->login(app_key, app_id, base_info_befor_sort)) == TFS_SUCCESS)
      {
        // gene session id
        SessionUtil::gene_session_id(app_id, session_ip, session_id);

        KeepAliveInfo keep_alive_info(session_id);
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, LOGIN_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed,"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, LOGIN_FLAG, ret);
        }
        resource_manager_->sort_ns_by_distance(app_id, common::Func::addr_to_str(session_ip, false),
            base_info_befor_sort, base_info);

      }
      else
      {
        TBSYS_LOG(ERROR, "call ResourceManager::login failed, app_key: %s, session_ip: %"PRI64_PREFIX"u, ret: %d",
            app_key.c_str(), session_ip, ret);
      }
      return ret;
    }

    int SessionManager::keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info,
        bool& update_flag, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      int32_t app_id = 0;
      int64_t session_ip = 0;
      BaseInfo base_info_befor_sort;
      if ((ret = SessionUtil::parse_session_id(session_id, app_id, session_ip)) == TFS_SUCCESS)
      {
        // first check update info, then update session info
        if ((ret = resource_manager_->check_update_info(app_id, keep_alive_info.s_base_info_.modify_time_,
                update_flag, base_info_befor_sort)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call IResourceManager::check_update_info failed. app_id: %d, modify_time: %"PRI64_PREFIX"d, ret: %d",
              app_id, keep_alive_info.s_base_info_.modify_time_, ret);
        }
        else
        {
          if (update_flag)
          {
            resource_manager_->sort_ns_by_distance(app_id,
                Func::addr_to_str(session_ip, false), base_info_befor_sort, base_info);
          }
          if ((ret = update_session_info(app_id, session_id, keep_alive_info, KA_FLAG)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed, this will not be happen!"
                " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
                session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, KA_FLAG, ret);
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionUtil::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
      }
      return ret;
    }

    int SessionManager::logout(const std::string& session_id, const KeepAliveInfo& keep_alive_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      int32_t app_id = 0;
      int64_t session_ip = 0;
      if ((ret = SessionUtil::parse_session_id(session_id, app_id, session_ip) == TFS_SUCCESS))
      {
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, LOGOUT_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::update_session_info failed, this will not be happen!"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, LOGOUT_FLAG, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionUtil::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
      }

      return ret;
    }

    int SessionManager::update_session_info(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      int ret = TFS_SUCCESS;
      if ((ret = monitor_task_->update_session_info(app_id, session_id, keep_alive_info, update_flag)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed."
            " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, update_flag, ret);
      }
      else if ((ret = stat_task_->update_session_info(app_id, session_id, keep_alive_info, update_flag)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SessionStatTask::update_session_info failed."
            " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, update_flag, ret);
      }

      return ret;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    int ISessionTask::update_session_info(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      return update_session_info_ex(app_id, session_id, keep_alive_info, update_flag);
    }

    int ISessionTask::update_session_info_ex(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      AppSessionMapIter mit = app_sessions_.find(app_id);
      if (mit == app_sessions_.end()) // keep_alive accept this scene: client maybe change rc when run
      {
        SessionCollectMap sc;
        sc.insert(pair<string, KeepAliveInfo>(session_id, keep_alive_info));
        app_sessions_.insert(pair<int32_t, SessionCollectMap>(app_id, sc));
      }
      else //find
      {
        SessionCollectMapIter scit = mit->second.find(session_id);
        if (scit == mit->second.end()) //new session
        {
          mit->second.insert(pair<string, KeepAliveInfo>(session_id, keep_alive_info));
        }
        else //session exist. Login will failed, others will accumulate keep alive stat info
        {
          if (LOGIN_FLAG == update_flag)
          {
            TBSYS_LOG(INFO, "session id exist when login, session id: %s, flag: %d", session_id.c_str(), update_flag);
            ret = EXIT_SESSION_EXIST_ERROR;
          }
          else
          {
            scit->second += keep_alive_info;

            if (LOGOUT_FLAG == update_flag)
            {
              scit->second.s_base_info_.is_logout_ = true;
            }
            else
            {
              scit->second.s_base_info_.is_logout_ = false;
            }
          }
        }
      }
      return ret;
    }

    void ISessionTask::display(const int32_t app_id, const SessionStat& s_stat, const int64_t cache_hit_ratio)
    {
      //Todo: replace app id with app name
      TBSYS_LOG(INFO, "monitor app: %d, avg cache_hit_ratio: %"PRI64_PREFIX"d", app_id, cache_hit_ratio);
      std::map<OperType, AppOperInfo>::const_iterator mit = s_stat.app_oper_info_.begin();
      for ( ; mit != s_stat.app_oper_info_.end(); ++mit)
      {
        TBSYS_LOG(INFO, "monitor app: %d, oper_type: %s, oper_times: %"PRI64_PREFIX"d, oper_size: %"PRI64_PREFIX"d,"
            " oper_rt: %"PRI64_PREFIX"d, oper_succ: %"PRI64_PREFIX"d",
            app_id, key_map_[mit->first].c_str(), mit->second.oper_times_,
            mit->second.oper_size_, mit->second.oper_rt_, mit->second.oper_succ_);
      }
      return;
    }

    void SessionMonitorTask::runTimerTask()
    {
      if (!stop_)
      {
        AppSessionMap tmp_sessions;
        {
          ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
          tmp_sessions.swap(app_sessions_);
          assert(app_sessions_.size() == 0);
        }

        // dump session stat
        // use app id now. should replace it with app name
        AppSessionMapConstIter mit = tmp_sessions.begin();
        for ( ; mit != tmp_sessions.end(); ++mit)
        {
          SessionStat merge_stat;
          int64_t total_cache_hit_ratio = 0, avg_cache_hit_ratio = 0;
          SessionCollectMapConstIter sit = mit->second.begin();
          for ( ; sit != mit->second.end(); ++sit)
          {
            merge_stat += sit->second.s_stat_;
            total_cache_hit_ratio += sit->second.s_stat_.cache_hit_ratio_;
          }
          avg_cache_hit_ratio = total_cache_hit_ratio / mit->second.size();
          display(mit->first, merge_stat, avg_cache_hit_ratio);
        }
      }
      return;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    void SessionStatTask::runTimerTask()
    {
      if (!stop_)
      {
        AppSessionMap tmp_sessions;
        {
          ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
          tmp_sessions.swap(app_sessions_);
        }

        vector<SessionBaseInfo> v_session_infos;
        map<string, SessionStat> m_session_stats;
        MIdAppStat m_app_stat;

        //extract info
        extract(tmp_sessions, v_session_infos, m_session_stats, m_app_stat);

        int ret = TFS_SUCCESS;
        //update session info
        ret = resource_manager_->update_session_info(v_session_infos);
        if (TFS_SUCCESS == ret)
        {
          //update session stat
          ret = resource_manager_->update_session_stat(m_session_stats);
        }
        if (TFS_SUCCESS == ret)
        {
          //update app stat
          ret = resource_manager_->update_app_stat(m_app_stat);
        }
        if (TFS_SUCCESS != ret)
        {
          //add log
          TBSYS_LOG(ERROR, "update info to db fail. session info size: %zd, session stat size: %zd, app stat size: %zd, ret: %d",
              v_session_infos.size(), m_session_stats.size(), m_app_stat.size(), ret);
          //roll back
          rollback(tmp_sessions);
        }
      }
      return;
    }

    void SessionStatTask::extract(const AppSessionMap& app_sessions, std::vector<SessionBaseInfo>& v_session_infos,
        std::map<std::string, SessionStat>& m_session_stats, MIdAppStat& m_app_stat)
    {
      AppSessionMapConstIter mit = app_sessions.begin();
      for ( ; mit != app_sessions.end(); ++mit)
      {
        AppStat app_stat(mit->first);
        SessionCollectMapConstIter sit = mit->second.begin();
        for ( ; sit != mit->second.end(); ++sit)
        {
          TBSYS_LOG(DEBUG, "extract stat info. session_id: %s, client_version: %s, cache_size: %"PRI64_PREFIX"d,"
              " cache_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, is_logout: %d",
              sit->second.s_base_info_.session_id_.c_str(), sit->second.s_base_info_.client_version_.c_str(),
              sit->second.s_base_info_.cache_size_,
              sit->second.s_base_info_.cache_time_, sit->second.s_base_info_.modify_time_, sit->second.s_base_info_.is_logout_);
          v_session_infos.push_back(sit->second.s_base_info_);
          // assign
          m_session_stats[sit->first] = sit->second.s_stat_;
          app_stat.add(sit->second.s_stat_);
        }
        m_app_stat[mit->first] = app_stat;
      }
      return;
    }

    void SessionStatTask::rollback(const AppSessionMap& app_sessions)
    {
      int ret = TFS_SUCCESS;
      int32_t app_id = 0;
      int64_t session_ip = 0;
      AppSessionMapConstIter mit = app_sessions.begin();
      for ( ; mit != app_sessions.end(); ++mit)
      {
        SessionCollectMapConstIter sit = mit->second.begin();
        for ( ; sit != mit->second.end(); ++sit)
        {
          TBSYS_LOG(DEBUG, "roll back info. session_id: %s, client_version: %s, cache_size: %"PRI64_PREFIX"d,"
              " cache_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, is_logout: %d",
              sit->second.s_base_info_.session_id_.c_str(), sit->second.s_base_info_.client_version_.c_str(),
              sit->second.s_base_info_.cache_size_,
              sit->second.s_base_info_.cache_time_, sit->second.s_base_info_.modify_time_, sit->second.s_base_info_.is_logout_);

          if ((ret = SessionUtil::parse_session_id(sit->first, app_id, session_ip)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call SessionUtil::parse_session_id failed, session_id: %s, ret: %d",
                sit->first.c_str(), ret);
          }
          else
          {
            UpdateFlag update_flag = sit->second.s_base_info_.is_logout_ ? LOGOUT_FLAG : KA_FLAG;
            if ((ret = update_session_info_ex(app_id, sit->first, sit->second, update_flag)) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "call ISessionTask::update_session_info_ex failed."
                  "app_id: %d, session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
                  app_id, sit->first.c_str(), sit->second.s_base_info_.modify_time_, update_flag, ret);
            }
          }
        }
      }
      return;
    }
  }
}
