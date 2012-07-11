/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: app_resource.h 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_APPRESOURCE_H_
#define TFS_RCSERVER_APPRESOURCE_H_

#include <map>
#include <string>
#include "common/internal.h"
#include "resource.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    class AppResource : public IResource
    {
      public:
        explicit AppResource(DatabaseHelper& database_helper) :
          IResource(database_helper), app_last_update_time_(-1)
        {
        }
        virtual ~AppResource()
        {
        }

      public:
        virtual int load();

        int get_app_id(const std::string& app_key, int32_t& app_id) const;
        int get_app_info(const int32_t app_id, AppInfo& app_info) const;
        int get_last_modify_time(const int32_t app_id, int64_t& last_modify_time) const;
        bool need_reload(const int64_t update_time_in_db) const;
        int get_app_name(const int32_t app_id, std::string& app_name) const;

      private:
        int64_t app_last_update_time_;
        // app_id <-> AppInfo
        MIdAppInfo m_id_appinfo_;
        // app_key <-> app_id
        std::map<std::string, int32_t> app_ids_;
    };
  }
}
#endif //TFS_RCSERVER_APPRESOURCE_H_
