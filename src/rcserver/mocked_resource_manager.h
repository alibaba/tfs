/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mocked_resource_manager.h 523 2011-06-20 07:06:21Z daoan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
#define TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_

#include "common/error_msg.h"
#include "i_resource_manager.h"

namespace tfs
{
  namespace rcserver
  {
    class MockedResourceManager : public IResourceManager
    {
      public:
        MockedResourceManager() : flag_(false) {}
        ~MockedResourceManager() {}

      public:
        int initialize()
        {
          apps_["pic"] = 10001;
          apps_["ic"] = 10002;
          apps_["uc"] = 10003;
          apps_["dc"] = 10004;
          apps_["cc"] = 10005;
          apps_["fc"] = 10006;
          apps_["gc"] = 10007;

          old_info_.report_interval_ = 10;
          new_info_.report_interval_ = 20;

          id_2_mtimes_[10001] = 100000;
          id_2_mtimes_[10002] = 200000;
          id_2_mtimes_[10003] = 300000;
          id_2_mtimes_[10004] = 400000;
          id_2_mtimes_[10005] = 500000;
          id_2_mtimes_[10006] = 600000;
          id_2_mtimes_[10007] = 700000;

          return common::TFS_SUCCESS;
        }

        int load()
        {
          return common::TFS_SUCCESS;
        }

        void clean_task()
        {
          return;
        }

        void stop()
        {
          return;
        }

        int start()
        {
          return common::TFS_SUCCESS;
        }

        int login(const std::string& app_key, int32_t& app_id, common::BaseInfo& base_info)
        {
          int ret = common::TFS_SUCCESS;
          std::map<std::string, int32_t>::iterator mit = apps_.find(app_key);
          if (mit == apps_.end())
          {
            ret = common::EXIT_APP_NOTEXIST_ERROR;
          }
          else
          {
            app_id = mit->second;
            base_info = old_info_;
          }
          return ret;
        }

        int check_update_info(const int32_t app_id, const int64_t modify_time, bool& update_flag, common::BaseInfo& base_info)
        {
          int ret = common::TFS_SUCCESS;
          std::map<int32_t, int64_t>::iterator mit = id_2_mtimes_.find(app_id);
          if (mit == id_2_mtimes_.end())
          {
            ret = common::EXIT_APP_NOTEXIST_ERROR;
          }
          else
          {
            if (modify_time < mit->second) //need update
            {
              update_flag = true;
              base_info = new_info_; //copy info
            }
            else // >=
            {
              update_flag = false;
            }
          }

          return ret;
        }

        int logout(const std::string& session_id)
        {
          UNUSED(session_id);
          int ret = common::TFS_SUCCESS;
          return ret;
        }

        int update_session_info(const std::vector<common::SessionBaseInfo>& session_infos)
        {
          int ret = common::TFS_SUCCESS;
          if (flag_)
          {
            std::vector<common::SessionBaseInfo>::const_iterator sit = session_infos.begin();
            for ( ; sit != session_infos.end(); ++sit)
            {
              TBSYS_LOG(INFO, "update db session info. session_id: %s, client_version: %s, cache_size: %"PRI64_PREFIX"d,"
                  " cache_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, is_logout: %d",
                  (*sit).session_id_.c_str(), (*sit).client_version_.c_str(), (*sit).cache_size_,
                  (*sit).cache_time_, (*sit).modify_time_, (*sit).is_logout_);
            }
            flag_ = false;
          }
          else
          {
            ret = common::TFS_ERROR;
            flag_ = true;
          }
          return ret;
        }

        int update_session_stat(const std::map<std::string, common::SessionStat>& session_stats)
        {
          int ret = common::TFS_SUCCESS;
          std::map<std::string, common::SessionStat>::const_iterator stat_it = session_stats.begin();
          for ( ; stat_it != session_stats.end(); ++stat_it)
          {
            std::map<common::OperType, common::AppOperInfo>::const_iterator mit = stat_it->second.app_oper_info_.begin();
            for ( ; mit != stat_it->second.app_oper_info_.end(); ++mit)
            {
              TBSYS_LOG(INFO, "update db session stat. session_id: %s, oper_type: %d, oper_times: %"PRI64_PREFIX"d, oper_size: %"PRI64_PREFIX"d,"
                  " oper_rt: %"PRI64_PREFIX"d, oper_succ: %"PRI64_PREFIX"d",
                  stat_it->first.c_str(), mit->first, mit->second.oper_times_,
                  mit->second.oper_size_, mit->second.oper_rt_, mit->second.oper_succ_);
            }
          }
          flag_ = false;
          return ret;
        }

        int update_app_stat(const MIdAppStat& app_stats)
        {
          int ret = common::TFS_SUCCESS;
          std::map<int32_t, AppStat>::const_iterator mit = app_stats.begin();
          for ( ; mit != app_stats.end(); ++mit)
          {
            TBSYS_LOG(INFO, "update db app stat. app_id: %d, id: %d, file_count: %"PRI64_PREFIX"d, used_capacity: %"PRI64_PREFIX"d",
                mit->first, mit->second.id_, mit->second.file_count_, mit->second.used_capacity_);
          }
          flag_ = false;
          return ret;
        }
        int sort_ns_by_distance(const int32_t app_id, const std::string& app_ip,
            common::BaseInfo& in_base_info, common::BaseInfo& out_base_info)
        {
          out_base_info = in_base_info;
          return common::TFS_SUCCESS;
        }
      private:
        DISALLOW_COPY_AND_ASSIGN(MockedResourceManager);
        std::map<std::string, int32_t> apps_;
        std::map<int32_t, int64_t> id_2_mtimes_;
        common::BaseInfo old_info_, new_info_;
        bool flag_;
    };
  }
}
#endif //TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
