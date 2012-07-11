/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#ifndef TFS_RCSERVER_MYSQL_DATABASE_HELPER_H_
#define TFS_RCSERVER_MYSQL_DATABASE_HELPER_H_
#include <tbsys.h>
#include <Mutex.h>
#include "database_helper.h"
namespace tfs
{
  namespace rcserver
  {
    class MysqlDatabaseHelper :public DatabaseHelper
    {
      public:
        virtual ~MysqlDatabaseHelper();
        virtual int connect();
        virtual int close();

        //ResourceServerInfo
        virtual int select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam);
        virtual int update(const ResourceServerInfo& inparam);
        virtual int remove(const ResourceServerInfo& inparam);
        virtual int scan(VResourceServerInfo& outparam);

        virtual int scan(VMetaRootServerInfo& outparam);

        //ClusterRackInfo
        virtual int select(const ClusterRackInfo& inparam, ClusterRackInfo& outparam);
        virtual int update(const ClusterRackInfo& inparam);
        virtual int remove(const ClusterRackInfo& inparam);
        virtual int scan(VClusterRackInfo& outparam);

        //ClusterRackGroup
        virtual int scan(VClusterRackGroup& outparam);

        //ip transfer info
        virtual int scan(IpReplaceHelper::VIpTransferItem& outparam);

        //app ip turn info
        virtual int scan(std::map<int32_t, IpReplaceHelper::VIpTransferItem>& outparam);

        //ClusterRackDuplicateServer
        virtual int scan(VClusterRackDuplicateServer& outparam);

        //BaseInfoUpdateTime
        virtual int select(BaseInfoUpdateTime& outparam);

        //AppInfo
        virtual int select(const AppInfo& inparam, AppInfo& outparam);
        virtual int update(const AppInfo& inparam);
        virtual int remove(const AppInfo& inparam);
        virtual int scan(MIdAppInfo& outparam);

        //update
        virtual int update_session_info(const std::vector<common::SessionBaseInfo>& session_infos);
        virtual int update_session_stat(const std::map<std::string, common::SessionStat>& session_stats);
        virtual int update_app_stat(const MIdAppStat& app_stats);

        virtual int scan_cache_info(std::vector<std::string>& outparam);

      private:
        int exect_update_sql(const char* sql);

      private:
        tbutil::Mutex mutex_;

    };

  }
}
#endif
