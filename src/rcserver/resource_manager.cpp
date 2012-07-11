/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: resource_manager.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include <algorithm>
#include "resource_manager.h"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "app_resource.h"
#include "base_resource.h"
#include "mysql_database_helper.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    ResourceManager::ResourceManager(tbutil::TimerPtr timer):
      database_helper_(NULL),
      app_resource_manager_(NULL),
      base_resource_manager_(NULL),
      have_inited_(false),
      timer_(timer),
      resource_update_task_(0)
    {
    }

    void ResourceManager::clean_resource()
    {
      if (NULL != app_resource_manager_)
      {
        delete app_resource_manager_;
        app_resource_manager_ = NULL;
      }
      if (NULL != base_resource_manager_)
      {
        delete base_resource_manager_;
        base_resource_manager_ = NULL;
      }
    }

    ResourceManager::~ResourceManager()
    {
      clean_resource();
      clean_task();
      if (NULL != database_helper_)
      {
        delete database_helper_;
        database_helper_ = NULL;
      }
      have_inited_ = false;
    }

    int ResourceManager::initialize()
    {
      int ret = TFS_SUCCESS;
      if (NULL != database_helper_)
      {
        delete database_helper_;
        database_helper_ = NULL;
      }
      database_helper_ = new MysqlDatabaseHelper();
      database_helper_->set_conn_param(SYSPARAM_RCSERVER.db_info_.c_str(),
          SYSPARAM_RCSERVER.db_user_.c_str(), SYSPARAM_RCSERVER.db_pwd_.c_str());
      ret = database_helper_->connect();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "connect database error ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        have_inited_ = true;
        ret = load();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load data error ret: %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        if (0 == resource_update_task_)
        {
          resource_update_task_ = new ResourceUpdateTask(*this);
        }

        ret = start();
      }
      return ret;
    }

    void ResourceManager::clean_task()
    {
      resource_update_task_ = 0;
    }

    void ResourceManager::stop()
    {
      if (0 != resource_update_task_)
      {
        resource_update_task_->stop();
        if (0 != timer_)
        {
          timer_->cancel(resource_update_task_);
        }
      }
    }

    int ResourceManager::start()
    {
      int ret = TFS_SUCCESS;
      if (0 == resource_update_task_ || 0 == timer_)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "call ResourceManager::start failed, update task or timer is null, ret: %d", ret);
      }
      else if ((ret = timer_->scheduleRepeated(resource_update_task_, tbutil::Time::seconds(SYSPARAM_RCSERVER.update_interval_))) != 0)
      {
        TBSYS_LOG(ERROR, "call scheduleRepeated failed, update_interval_: %"PRI64_PREFIX"d, ret: %d",
            SYSPARAM_RCSERVER.update_interval_, ret);
        stop();
        ret = TFS_ERROR;
      }
      return ret;
    }

    int ResourceManager::load()
    {
      TBSYS_LOG(DEBUG, "ResourceManager::load()");
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_)
      {
        AppResource* tmp_app_resource = new AppResource(*database_helper_);
        BaseResource* tmp_base_resource = new BaseResource(*database_helper_);
        ret = tmp_app_resource->load();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load app source error ret is %d", ret);
          goto EXIT;
        }
        ret = tmp_base_resource->load();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load base source error ret is %d", ret);
          goto EXIT;
        }
        {
          tbsys::CWLockGuard guard(resorce_mutex_);
          clean_resource();
          app_resource_manager_ = tmp_app_resource;
          base_resource_manager_ = tmp_base_resource;
        }
        tmp_app_resource = NULL;
        tmp_base_resource = NULL;

EXIT:
        if (NULL != tmp_app_resource)
        {
          delete tmp_app_resource;
          tmp_app_resource = NULL;
        }
        if (NULL != tmp_base_resource)
        {
          delete tmp_base_resource;
          tmp_base_resource = NULL;
        }
      }
      return ret;
    }

    int ResourceManager::login(const std::string& app_key, int32_t& app_id, BaseInfo& base_info)
    {
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_ && NULL != app_resource_manager_ && NULL != base_resource_manager_)
      {
        int64_t last_app_modify_time = 0;
        int64_t last_base_modify_time = 0;
        tbsys::CRLockGuard guard(resorce_mutex_);
        ret = app_resource_manager_->get_app_id(app_key, app_id);
        // sync time between client and server
        if (TFS_SUCCESS == ret)
        {
          ret = app_resource_manager_->get_last_modify_time(app_id, last_app_modify_time);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = base_resource_manager_->get_last_modify_time(last_base_modify_time);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = get_base_info(app_id, std::max(last_app_modify_time, last_base_modify_time), base_info);
        }
      }
      return ret;
    }

    int ResourceManager::check_update_info(const int32_t app_id,
            const int64_t modify_time, bool& update_flag, BaseInfo& base_info)
    {
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_ && NULL != app_resource_manager_ && NULL != base_resource_manager_)
      {
        update_flag = false;
        int64_t last_app_modify_time = 0;
        int64_t last_base_modify_time = 0;
        tbsys::CRLockGuard guard(resorce_mutex_);
        ret = app_resource_manager_->get_last_modify_time(app_id, last_app_modify_time);
        if (TFS_SUCCESS == ret)
        {
          ret = base_resource_manager_->get_last_modify_time(last_base_modify_time);
        }
        if (TFS_SUCCESS == ret)
        {
          if (modify_time < last_app_modify_time || modify_time < last_base_modify_time)
          {
            update_flag = true;
            ret = get_base_info(app_id, std::max(last_app_modify_time, last_base_modify_time), base_info);
          }
        }
      }
      return ret;
    }

    int ResourceManager::sort_ns_by_distance(const int32_t app_id, const std::string& app_ip,
            common::BaseInfo& in_base_info, common::BaseInfo& out_base_info)
    {
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_ && NULL != base_resource_manager_)
      {
        tbsys::CRLockGuard guard(resorce_mutex_);
        ret = base_resource_manager_->sort_ns_by_distance(app_id,
            app_ip, in_base_info, out_base_info);
      }
      return ret;
    }

    int ResourceManager::get_app_name(const int32_t app_id, std::string& app_name) const
    {
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_)
      {
        tbsys::CRLockGuard guard(resorce_mutex_);
        ret = app_resource_manager_->get_app_name(app_id, app_name);
      }
      return ret;
    }

    int ResourceManager::get_base_info(const int32_t app_id, const int64_t modify_time, BaseInfo& base_info)
    {
      int ret = EXIT_NOT_INIT_ERROR;
      if (have_inited_ && NULL != app_resource_manager_ && NULL != base_resource_manager_)
      {
        AppInfo app_info;
        ret = app_resource_manager_->get_app_info(app_id, app_info);
        if (TFS_SUCCESS == ret)
        {
          ret = base_resource_manager_->get_resource_servers(base_info.rc_server_infos_);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = base_resource_manager_->get_cluster_infos(app_info.cluster_group_id_, base_info.cluster_infos_, base_info.cluster_infos_for_update_);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = base_resource_manager_->get_meta_root_server(app_id, base_info.meta_root_server_);
        }
        if (TFS_SUCCESS == ret)
        {
          base_info.report_interval_ = app_info.report_interval_;
          base_info.modify_time_ = modify_time;
          base_info.use_remote_cache_ = app_info.use_remote_cache_;
          std::vector<ClusterRackData>::iterator it = base_info.cluster_infos_.begin();
          for (; it != base_info.cluster_infos_.end(); it++)
          {
            it->need_duplicate_ = app_info.need_duplicate_;
          }
        }
      }
      return ret;
    }
    int ResourceManager::update_session_info(const std::vector<SessionBaseInfo>& session_infos)
    {
      return database_helper_->update_session_info(session_infos);
    }

    int ResourceManager::update_session_stat(const std::map<std::string, SessionStat>& session_stats)
    {
      return database_helper_->update_session_stat(session_stats);
    }

    int ResourceManager::update_app_stat(const MIdAppStat& app_stats)
    {
      return database_helper_->update_app_stat(app_stats);
    }

    bool ResourceManager::need_reload()
    {
      bool ret = false;
      if (have_inited_ )
      {
        BaseInfoUpdateTime outparam;
        if (TFS_SUCCESS == database_helper_->select(outparam))
        {
          int64_t last_base_modify_time = 0;
          tbsys::CRLockGuard guard(resorce_mutex_);
          if (TFS_SUCCESS == base_resource_manager_->get_last_modify_time(last_base_modify_time))
          {
            if (app_resource_manager_->need_reload(outparam.app_last_update_time_)
                || outparam.base_last_update_time_ > last_base_modify_time)
            {
              ret = true;
            }
          }
        }
      }
      return ret;
    }

    void ResourceUpdateTask::runTimerTask()
    {
      if (!stop_ && manager_.need_reload())
      {
        manager_.load();
      }
      return;
    }

  }
}
