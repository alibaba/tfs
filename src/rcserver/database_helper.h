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
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 */

#ifndef TFS_RCSERVER_DATABASE_HELPER_H_
#define TFS_RCSERVER_DATABASE_HELPER_H_
#include "common/internal.h"
#include "resource_server_data.h"
#include "ip_replace_helper.h"
namespace tfs
{
  namespace rcserver
  {
    class DatabaseHelper
    {
      public:
        DatabaseHelper();
        virtual ~DatabaseHelper();

        bool is_connected() const;
        int set_conn_param(const char* conn_str, const char* user_name, const char* passwd);

        virtual int connect() = 0;
        virtual int close() = 0;

        //ResourceServerInfo
        virtual int select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam) = 0;
        virtual int update(const ResourceServerInfo& inparam) = 0;
        virtual int remove(const ResourceServerInfo& inparam) = 0;
        virtual int scan(VResourceServerInfo& outparam) = 0;

        //MetaRootServerInfo
        virtual int scan(VMetaRootServerInfo& outparam) = 0;

        //ClusterRackInfo
        virtual int select(const ClusterRackInfo& inparam, ClusterRackInfo& outparam) = 0;
        virtual int update(const ClusterRackInfo& inparam) = 0;
        virtual int remove(const ClusterRackInfo& inparam) = 0;
        virtual int scan(VClusterRackInfo& outparam) = 0;

        //ClusterRackGroup
        virtual int scan(VClusterRackGroup& outparam) = 0;
        //ip transfer info
        virtual int scan(IpReplaceHelper::VIpTransferItem& outparam) = 0;
        //app ip turn info
        virtual int scan(std::map<int32_t, IpReplaceHelper::VIpTransferItem>& outparam) = 0;

        //ClusterRackDuplicateServer
        virtual int scan(VClusterRackDuplicateServer& outparam) = 0;

        //BaseInfoUpdateTime
        virtual int select(BaseInfoUpdateTime& outparam) = 0;

        //AppInfo
        virtual int select(const AppInfo& inparam, AppInfo& outparam) = 0;
        virtual int update(const AppInfo& inparam) = 0;
        virtual int remove(const AppInfo& inparam) = 0;
        virtual int scan(MIdAppInfo& outparam) = 0;

        virtual int update_session_info(const std::vector<common::SessionBaseInfo>& session_infos) = 0;
        virtual int update_session_stat(const std::map<std::string, common::SessionStat>& session_stats) = 0;
        virtual int update_app_stat(const MIdAppStat& app_stats) = 0;

        //ns cache info
        virtual int scan_cache_info(std::vector<std::string>& outparam) = 0;


      protected:
        enum {
          CONN_STR_LEN = 256,
        };
        char conn_str_[CONN_STR_LEN];
        char user_name_[CONN_STR_LEN];
        char passwd_[CONN_STR_LEN];
        bool is_connected_;

      private:
        DISALLOW_COPY_AND_ASSIGN(DatabaseHelper);
    };
  }
}
#endif
