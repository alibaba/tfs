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
#ifndef TFS_TEST_RCSERVER_TEST_DATABASE_HELPER_H_
#define TFS_TEST_RCSERVER_TEST_DATABASE_HELPER_H_
#include <time.h>
#include <tbsys.h>
#include <Mutex.h>
#include "database_helper.h"
#include "resource_manager.h"
#include "common/define.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    class TestDatabaseHelper :public DatabaseHelper
    {
      public:
        virtual ~TestDatabaseHelper()
        {
        }
        virtual int connect()
        {
          return TFS_SUCCESS;
        }
        virtual int close()
        {
          return TFS_SUCCESS;
        }

        //ResourceServerInfo
        virtual int select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam)
        {
          UNUSED(inparam);
          UNUSED(outparam);
          return TFS_SUCCESS;
        }
        virtual int update(const ResourceServerInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int remove(const ResourceServerInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int scan(VResourceServerInfo& outparam)
        {
          ResourceServerInfo tmp;
          tmp.stat_ = 1;
          sprintf(tmp.addr_info_, "%s", "10.232.35.41:2123");
          outparam.push_back(tmp);
          tmp.stat_ = 0;
          sprintf(tmp.addr_info_, "%s", "10.232.35.40:2123");
          outparam.push_back(tmp);
          return TFS_SUCCESS;
        }
        virtual int scan(IpReplaceHelper::VIpTransferItem& outparam)
        {
          return TFS_SUCCESS;
        }
        virtual int scan(std::map<int32_t, IpReplaceHelper::VIpTransferItem>& outparam)
        {
          return TFS_SUCCESS;
        }
        virtual int scan(VMetaRootServerInfo& outparam)
        {
          MetaRootServerInfo tmp;
          tmp.app_id_ = 5;
          tmp.stat_ = 0;
          sprintf(tmp.addr_info_, "%s", "10.232.35.41:2123");
          outparam.push_back(tmp);
          tmp.app_id_ = 0;
          tmp.stat_ = 1;
          sprintf(tmp.addr_info_, "%s", "10.232.35.40:2123");
          outparam.push_back(tmp);
          tmp.app_id_ = 5;
          tmp.stat_ = 1;
          sprintf(tmp.addr_info_, "%s", "10.232.35.42:2123");
          outparam.push_back(tmp);
          return TFS_SUCCESS;
        }

        //ClusterRackInfo
        virtual int select(const ClusterRackInfo& inparam, ClusterRackInfo& outparam)
        {
          UNUSED(inparam);
          UNUSED(outparam);
          return TFS_SUCCESS;
        }
        virtual int update(const ClusterRackInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int remove(const ClusterRackInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int scan(VClusterRackInfo& outparam)
        {
          ClusterRackInfo tmp;
          tmp.cluster_rack_id_ = 1;
          tmp.cluster_data_.cluster_stat_ = 1;
          tmp.cluster_data_.cluster_id_ ="T1M";
          tmp.cluster_data_.ns_vip_ ="10.10.10.10:1100";
          outparam.push_back(tmp);

          tmp.cluster_rack_id_ = 1;
          tmp.cluster_data_.cluster_stat_ = 2;
          tmp.cluster_data_.cluster_id_ ="T5M";
          tmp.cluster_data_.ns_vip_ ="10.10.10.15:1100";
          outparam.push_back(tmp);

          tmp.cluster_rack_id_ = 2;
          tmp.cluster_data_.cluster_stat_ = 2;
          tmp.cluster_data_.cluster_id_ ="T2M";
          tmp.cluster_data_.ns_vip_ ="10.10.10.12:1100";
          outparam.push_back(tmp);

          tmp.cluster_rack_id_ = 2;
          tmp.cluster_data_.cluster_stat_ = 2;
          tmp.cluster_data_.cluster_id_ ="T3M";
          tmp.cluster_data_.ns_vip_ ="10.10.10.13:1100";
          outparam.push_back(tmp);

          tmp.cluster_rack_id_ = 2;
          tmp.cluster_data_.cluster_stat_ = 2;
          tmp.cluster_data_.cluster_id_ ="T3B";
          tmp.cluster_data_.ns_vip_ ="10.10.10.14:1100";
          outparam.push_back(tmp);
          return TFS_SUCCESS;
        }


        //ClusterRackGroup
        virtual int scan(VClusterRackGroup& outparam)
        {
          ClusterRackGroup tmp;
          tmp.cluster_group_id_ = 1;
          tmp.cluster_rack_id_ = 1;
          tmp.cluster_rack_access_type_ = 1;
          outparam.push_back(tmp);

          tmp.cluster_group_id_ = 1;
          tmp.cluster_rack_id_ = 2;
          tmp.cluster_rack_access_type_ = 2;
          outparam.push_back(tmp);

          tmp.cluster_group_id_ = 2;
          tmp.cluster_rack_id_ = 2;
          tmp.cluster_rack_access_type_ = 1;
          outparam.push_back(tmp);
          return TFS_SUCCESS;
        }

        //ClusterRackDuplicateServer
        virtual int scan(VClusterRackDuplicateServer& outparam)
        {
          ClusterRackDuplicateServer tmp;
          tmp.cluster_rack_id_ = 1;
          sprintf(tmp.dupliate_server_addr_, "%s", "1.1.1.1:1010");
          outparam.push_back(tmp);

          tmp.cluster_rack_id_ = 2;
          sprintf(tmp.dupliate_server_addr_, "%s", "2.1.1.1:1010");
          outparam.push_back(tmp);
          return TFS_SUCCESS;
        }


        //BaseInfoUpdateTime
        virtual int select(BaseInfoUpdateTime& outparam)
        {
          int64_t now = 100;
          outparam.base_last_update_time_ = outparam.app_last_update_time_ = now;
          return TFS_SUCCESS;
        }

        //AppInfo
        virtual int select(const AppInfo& inparam, AppInfo& outparam)
        {
          UNUSED(inparam);
          UNUSED(outparam);
          return TFS_SUCCESS;
        }
        virtual int update(const AppInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int remove(const AppInfo& inparam)
        {
          UNUSED(inparam);
          return TFS_SUCCESS;
        }
        virtual int scan(MIdAppInfo& outparam)
        {
          AppInfo tmp;
          tmp.id_ = 1;
          tmp.quto_ = 10;
          tmp.cluster_group_id_ = 1;
          tmp.report_interval_ = 10;
          tmp.need_duplicate_ = 0;
          tmp.modify_time_ = 50;
          snprintf(tmp.key_, APP_KEY_LEN, "%s", "appkey1");
          outparam[tmp.id_] = tmp;

          tmp.id_ = 2;
          tmp.quto_ = 20;
          tmp.cluster_group_id_ = 1;
          tmp.report_interval_ = 20;
          tmp.need_duplicate_ = 1;
          tmp.modify_time_ = 100;
          snprintf(tmp.key_, APP_KEY_LEN, "%s", "appkey2");
          outparam[tmp.id_] = tmp;
          return TFS_SUCCESS;
        }
        virtual int scan_cache_info(std::vector<std::string>& outparam)
        {UNUSED(outparam); return TFS_SUCCESS;}
        virtual int update_session_info(const std::vector<SessionBaseInfo>& session_infos)
        {UNUSED(session_infos); return TFS_SUCCESS;}
        virtual int update_session_stat(const std::map<std::string, SessionStat>& session_stats)
        {UNUSED(session_stats); return TFS_SUCCESS;}
        virtual int update_app_stat(const MIdAppStat& app_stats)
        {UNUSED(app_stats); return TFS_SUCCESS;}

    };
    class TestResourceManager: public ResourceManager
    {
      public:
        explicit TestResourceManager(tbutil::TimerPtr ptr):ResourceManager(ptr){}
        virtual int initialize()
        {
          database_helper_ = new TestDatabaseHelper();
          have_inited_ = true;
          load();
          return TFS_SUCCESS;
        }
        virtual ~TestResourceManager(){};
    };

  }
}
#endif
