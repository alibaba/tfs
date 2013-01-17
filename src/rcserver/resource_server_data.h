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
#ifndef TFS_RCSERVER_RESOURCE_SERVER_DATA_H_
#define TFS_RCSERVER_RESOURCE_SERVER_DATA_H_
#include <stdint.h>
#include <assert.h>
#include <vector>
#include <map>
#include <string>
#include <set>
#include <algorithm>
#include "common/rc_define.h"

namespace tfs
{
  namespace rcserver
  {
    const int ADDR_INFO_LEN = 256;
    const int REM_LEN = 256;
    const int CLUSTER_ID_LEN = 8;
    const int APP_KEY_LEN = 256;
    const int SESSION_ID_LEN = 256;
    const int CLIENT_VERSION_LEN = 64;

    struct ResourceServerInfo
    {
      int32_t stat_;
      char addr_info_[ADDR_INFO_LEN];
      char rem_[REM_LEN];
      ResourceServerInfo()
      {
        stat_ = -1;
        addr_info_[0] = '\0';
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ResourceServerInfo> VResourceServerInfo;
    struct MetaRootServerInfo
    {
      int32_t app_id_;
      int32_t stat_;
      char addr_info_[ADDR_INFO_LEN];
      char rem_[REM_LEN];
      MetaRootServerInfo()
      {
        app_id_ = -1;
        stat_ = -1;
        addr_info_[0] = '\0';
        rem_[0] = '\0';
      }
    };
    typedef std::vector<MetaRootServerInfo> VMetaRootServerInfo;

    struct ClusterRackInfo
    {
      int32_t cluster_rack_id_;
      common::ClusterData cluster_data_;
      char rem_[REM_LEN];
      ClusterRackInfo()
      {
        cluster_rack_id_ = -1;
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackInfo> VClusterRackInfo;

    struct ClusterRackGroup
    {
      int32_t cluster_group_id_;
      int32_t cluster_rack_id_;
      int32_t cluster_rack_access_type_;
      char rem_[REM_LEN];
      ClusterRackGroup()
      {
        cluster_group_id_ = -1;
        cluster_rack_id_ = -1;
        cluster_rack_access_type_ = -1;
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackGroup> VClusterRackGroup;

    struct ClusterRackDuplicateServer
    {
      int32_t cluster_rack_id_;
      char dupliate_server_addr_[ADDR_INFO_LEN];
      ClusterRackDuplicateServer()
      {
        cluster_rack_id_ = -1;
        dupliate_server_addr_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackDuplicateServer> VClusterRackDuplicateServer;

    struct BaseInfoUpdateTime
    {
      int64_t base_last_update_time_;
      int64_t app_last_update_time_;
      BaseInfoUpdateTime()
      {
        base_last_update_time_ = -1;
        app_last_update_time_ = -1;
      }
    };

    struct AppInfo
    {
      int64_t quto_;
      int32_t id_;
      int32_t cluster_group_id_;
      int32_t use_remote_cache_;
      int32_t report_interval_;
      int32_t need_duplicate_;
      int64_t modify_time_;
      char key_[APP_KEY_LEN];
      char app_name_[REM_LEN];
      char app_owner_[REM_LEN];
      char rem_[REM_LEN];
      AppInfo()
      {
        quto_ = -1;
        id_ = -1;
        cluster_group_id_ = -1;
        use_remote_cache_ = 0;
        report_interval_ = -1;
        need_duplicate_ = -1;
        modify_time_ = -1;
        key_[0] = '\0';
        app_name_[0] = '\0';
        app_owner_[0] = '\0';
        rem_[0] = '\0';
      }
    };
    typedef std::map<int32_t, AppInfo> MIdAppInfo;  //map<id, appinfo>

    struct AppStat
    {
      int32_t id_;
      int64_t file_count_;
      int64_t used_capacity_;
      AppStat()
      {
        id_ = -1;
        file_count_ = 0;
        used_capacity_ = 0;
      }

      explicit AppStat(const int32_t id)
      {
        id_ = id;
        file_count_ = 0;
        used_capacity_ = 0;
      }

      void add(const common::SessionStat& s_stat)
      {
        const std::map<common::OperType, common::AppOperInfo>& app_oper_info = s_stat.app_oper_info_;
        std::map<common::OperType, common::AppOperInfo>::const_iterator sit = app_oper_info.begin();
        for ( ; sit != app_oper_info.end(); ++sit)
        {
          if (common::OPER_WRITE == sit->first || common::OPER_UNIQUE_WRITE == sit->first)
          {
            file_count_ += sit->second.oper_succ_;
            used_capacity_ += sit->second.oper_size_;
          }
          else if (common::OPER_UNLINK == sit->first || common::OPER_UNIQUE_UNLINK == sit->first)
          {
            file_count_ -= sit->second.oper_succ_;
            used_capacity_ -= sit->second.oper_size_;
          }
        }
      }
    };
    typedef std::map<int32_t, AppStat> MIdAppStat;
  }
}
#endif
