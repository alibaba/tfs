/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: resource_manager.h 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_RESOURCEMANAGER_H_
#define TFS_RCSERVER_RESOURCEMANAGER_H_

#include <map>
#include <set>
#include <string>
#include <tbsys.h>
#include <Timer.h>
#include "common/internal.h"
#include "i_resource_manager.h"

namespace tfs
{
  namespace rcserver
  {

    class AppResource;
    class BaseResource;
    class DatabaseHelper;
    class ResourceManager;
    class ResourceUpdateTask : public tbutil::TimerTask
    {
      public:
        ResourceUpdateTask(ResourceManager& resource_manager)
          : stop_(false), manager_(resource_manager)
        {
        }
        virtual ~ResourceUpdateTask()
        {
        }

        virtual void runTimerTask();
        void stop()
        {
          stop_ = true;
        }

        void start()
        {
          stop_ = false;
        }
      private:
        bool stop_;
        ResourceManager& manager_;
    };
    typedef tbutil::Handle<ResourceUpdateTask> ResourceUpdateTaskPtr;

    class ResourceManager : public IResourceManager
    {
      public:
        explicit ResourceManager(tbutil::TimerPtr timer);
        virtual ~ResourceManager();

      public:
        virtual int initialize();
        virtual void clean_task();
        virtual void stop();
        virtual int start();
        virtual int load();

        virtual int login(const std::string& app_key, int32_t& app_id, common::BaseInfo& base_info);
        virtual int check_update_info(const int32_t app_id,
            const int64_t modify_time, bool& update_flag, common::BaseInfo& base_info);
        virtual int sort_ns_by_distance(const int32_t app_id, const std::string& app_ip,
            common::BaseInfo& in_base_info, common::BaseInfo& out_base_info);

        virtual int get_app_name(const int32_t app_id, std::string& app_name) const;

        virtual int update_session_info(const std::vector<common::SessionBaseInfo>& session_infos);
        virtual int update_session_stat(const std::map<std::string, common::SessionStat>& session_stats);
        virtual int update_app_stat(const MIdAppStat& app_stats);

        bool need_reload();

      private:
        int get_base_info(const int32_t app_id, const int64_t modify_time, common::BaseInfo& base_info);
        void clean_resource();

      protected:
        DISALLOW_COPY_AND_ASSIGN(ResourceManager);

        DatabaseHelper* database_helper_;
        tbsys::CRWLock resorce_mutex_;
        AppResource* app_resource_manager_;
        BaseResource* base_resource_manager_;
        bool have_inited_;

        tbutil::TimerPtr timer_;
        ResourceUpdateTaskPtr resource_update_task_;

    };

  }
}
#endif //TFS_RCSERVER_RESOURCEMANAGER_H_
