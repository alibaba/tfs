/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: session_manager.h 378 2011-05-30 07:16:34Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_SESSIONMANAGER_H_
#define TFS_RCSERVER_SESSIONMANAGER_H_

#include <time.h>
#include <map>
#include <string>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/internal.h"
#include "common/lock.h"
#include "i_resource_manager.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    class SessionManager;
    class ISessionTask : public tbutil::TimerTask
    {
      public:
        explicit ISessionTask(SessionManager& manager)
          : manager_(manager), stop_(false)
        {
          key_map_[common::OPER_INVALID] = "invalid";
          key_map_[common::OPER_READ] = "read";
          key_map_[common::OPER_WRITE] = "write";
          key_map_[common::OPER_UNIQUE_WRITE] = "unique_write";
          key_map_[common::OPER_UNLINK] = "unlink";
          key_map_[common::OPER_UNIQUE_UNLINK] = "unique_unlink";
        }

        virtual ~ISessionTask()
        {
        }

        void stop()
        {
          stop_ = true;
        }

        void start()
        {
          stop_ = false;
        }

        virtual void runTimerTask() = 0;
        int update_session_info(const int32_t app_id, const std::string& session_id,
            const common::KeepAliveInfo& keep_alive_info, common::UpdateFlag update_flag);

      protected:
        static void display(const int32_t app_id, const common::SessionStat& s_stat, const int64_t cache_hit_ratio = 0);
        int update_session_info_ex(const int32_t app_id, const std::string& session_id,
            const common::KeepAliveInfo& keep_alive_info, common::UpdateFlag update_flag);

      protected:
        SessionManager& manager_;
        bool stop_;
        common::AppSessionMap app_sessions_;
        common::RWLock rw_lock_;
        static std::map<common::OperType, std::string> key_map_;
    };

    class SessionMonitorTask : public ISessionTask
    {
      public:
        explicit SessionMonitorTask(SessionManager& manager)
          : ISessionTask(manager)
        {
        }

        virtual ~SessionMonitorTask()
        {
        }

        virtual void runTimerTask();

      private:
        IResourceManager* resource_manager_;
    };
    typedef tbutil::Handle<SessionMonitorTask> SessionMonitorTaskPtr;

    class SessionStatTask : public ISessionTask
    {
      public:
        SessionStatTask(SessionManager& manager, IResourceManager* resource_manager)
          : ISessionTask(manager), resource_manager_(resource_manager)
        {
        }
        virtual ~SessionStatTask()
        {
        }

        virtual void runTimerTask();

      private:
        void extract(const common::AppSessionMap& app_sessions, std::vector<common::SessionBaseInfo>& v_session_infos,
            std::map<std::string, common::SessionStat>& m_session_stats, MIdAppStat& m_app_stat);
        void rollback(const common::AppSessionMap& app_sessions);

      private:
        IResourceManager* resource_manager_;
    };
    typedef tbutil::Handle<SessionStatTask> SessionStatTaskPtr;

    class SessionManager
    {
      public:
        SessionManager(IResourceManager* resource_manager, tbutil::TimerPtr timer);
        ~SessionManager();

      public:
        int initialize(const bool reload_flag = false);
        void clean_task();
        void stop();
        int start();

        int login(const std::string& app_key, const int64_t session_ip,
            std::string& session_id, common::BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const common::KeepAliveInfo& keep_alive_info,
            bool& update_flag, common::BaseInfo& base_info);
        int logout(const std::string& session_id, const common::KeepAliveInfo& keep_alive_info);

      private:
        int update_session_info(const int32_t app_id, const std::string& session_id,
            const common::KeepAliveInfo& keep_alive_info, common::UpdateFlag update_flag);

      private:
        DISALLOW_COPY_AND_ASSIGN(SessionManager);

        IResourceManager* resource_manager_;
        tbutil::TimerPtr timer_;

        SessionMonitorTaskPtr monitor_task_;
        SessionStatTaskPtr stat_task_;

        bool is_init_;
        tbutil::Mutex mutex_;
    };

  }
}
#endif //TFS_RCSERVER_SESSIONMANAGER_H_
