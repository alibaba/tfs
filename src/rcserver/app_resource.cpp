/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: app_resource.cpp 378 2011-05-30 07:16:34Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "app_resource.h"
#include <tbsys.h>
#include "mysql_database_helper.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
   
    int AppResource::load()
    {
      int ret = TFS_SUCCESS;
      m_id_appinfo_.clear();
      ret = database_helper_.scan(m_id_appinfo_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load app_info error ret is %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        BaseInfoUpdateTime outparam;
        ret = database_helper_.select(outparam);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load base_info_update_time error ret is %d", ret);
        }
        else
        {
          app_last_update_time_ = outparam.app_last_update_time_;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        app_ids_.clear();
        MIdAppInfo::const_iterator it = m_id_appinfo_.begin();
        for (; it != m_id_appinfo_.end(); it++)
        {
          app_ids_[it->second.key_] = it->first;
        }

      }
      return ret;
    }

    int AppResource::get_app_id(const std::string& app_key, int32_t& app_id) const
    {
      int ret = TFS_SUCCESS;
      std::map<std::string, int32_t>::const_iterator it = app_ids_.find(app_key);
      if (it == app_ids_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        app_id = it->second;
      }
      return ret;
    }

    int AppResource::get_app_info(const int32_t app_id, AppInfo& app_info) const
    {
      int ret = TFS_SUCCESS;
      MIdAppInfo::const_iterator it = m_id_appinfo_.find(app_id);
      if (it == m_id_appinfo_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        app_info = it->second;
      }
      return ret;
    }

    int AppResource::get_last_modify_time(const int32_t app_id, int64_t& last_modify_time) const
    {
      int ret = TFS_SUCCESS;
      MIdAppInfo::const_iterator it = m_id_appinfo_.find(app_id);
      if (it == m_id_appinfo_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        last_modify_time = it->second.modify_time_;
      }
      return ret;
    }

    bool AppResource::need_reload(const int64_t update_time_in_db) const
    {
      return update_time_in_db > app_last_update_time_;
    }

    int AppResource::get_app_name(const int32_t app_id, std::string& app_name) const
    {
      int ret = TFS_SUCCESS;
      MIdAppInfo::const_iterator it = m_id_appinfo_.find(app_id);
      if (it == m_id_appinfo_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        app_name = it->second.app_name_;
      }
      return ret;
    }
  }
}
