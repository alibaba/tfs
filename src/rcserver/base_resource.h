/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_resource.h 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_BASERESOURCE_H_
#define TFS_RCSERVER_BASERESOURCE_H_

#include <map>
#include <set>
#include <string>
#include <vector>
#include "common/internal.h"
#include "common/rc_define.h"
#include "resource.h"
#include "resource_server_data.h"
#include "ip_replace_helper.h"

namespace tfs
{
  namespace rcserver
  {
    class BaseResource : public IResource
    {
      public:

        explicit BaseResource(DatabaseHelper& database_helper) :
          IResource(database_helper), base_last_update_time_(-1)
        {
        }
        virtual ~BaseResource()
        {
        }

      public:
        virtual int load();
        bool need_reload(const int64_t update_time_in_db) const;
        int get_meta_root_server(const int32_t app_id, int64_t& roo_server) const;
        int get_resource_servers(std::vector<uint64_t>& resource_servers) const;
        int get_cluster_infos(const int32_t cluster_group_id,
            std::vector<common::ClusterRackData>& cluster_rack_datas,
            std::vector<common::ClusterData>& cluster_datas_for_update) const;
        int get_last_modify_time(int64_t& last_modify_time) const;
        int sort_ns_by_distance(const int32_t app_id, const std::string& app_ip,
            const common::BaseInfo& in_base_info, common::BaseInfo& out_base_info);

      private:
        int64_t base_last_update_time_;
        VResourceServerInfo v_resource_server_info_;
        VMetaRootServerInfo v_meta_root_server_info_;
        VClusterRackInfo v_cluster_rack_info_;
        VClusterRackGroup v_cluster_rack_group_;
        VClusterRackDuplicateServer v_cluster_rack_duplicate_server_;
      private:
        //vector<pair<tair_info, tair_caculate_ip> >
        typedef std::vector<std::pair<std::string, std::string> > VNsCacheInfo;
        VNsCacheInfo v_ns_cache_info_;
      private:
        IpReplaceHelper::VIpTransferItem v_ip_transfer_table_;
        std::map<int32_t, IpReplaceHelper::VIpTransferItem> m_app_ip_turn_table_;
        std::map<std::string, std::string> m_ns_caculate_ip_;
        void fill_ip_caculate_map();
    };
  }
}
#endif //TFS_RCSERVER_BASERESOURCE_H_
