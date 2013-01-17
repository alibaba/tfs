/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_resource.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "base_resource.h"

#include <algorithm>
#include <tbsys.h>
#include "common/func.h"
#include "mysql_database_helper.h"
namespace
{
  const int SERVER_STAT_AVALIABLE = 1;
  const int GROUP_ACCESS_TYPE_FORBIDEN = 0;
  const int GROUP_ACCESS_TYPE_READ_ONLY = 1;
  const int GROUP_ACCESS_TYPE_READ_AND_WRITE = 2;
  const int CLUSTER_ACCESS_TYPE_FORBIDEN = 0;
  const int CLUSTER_ACCESS_TYPE_READ_ONLY = 1;
  const int CLUSTER_ACCESS_TYPE_READ_AND_WRITE = 2;
}
namespace tfs
{
  namespace rcserver
  {
    using namespace common;

    int BaseResource::load()
    {
      v_resource_server_info_.clear();
      v_meta_root_server_info_.clear();
      v_cluster_rack_info_.clear();
      v_cluster_rack_group_.clear();
      v_cluster_rack_duplicate_server_.clear();
      v_ip_transfer_table_.clear();
      m_app_ip_turn_table_.clear();
      m_ns_caculate_ip_.clear();
      v_ns_cache_info_.clear();
      int ret = TFS_SUCCESS;
      ret = database_helper_.scan(v_resource_server_info_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load resource_server_info error ret is %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_meta_root_server_info_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load meta_root_server_info error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_info_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_info error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_group_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_group error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_duplicate_server_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_duplicate_server error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_ip_transfer_table_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load ip_transfer_table error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(m_app_ip_turn_table_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load app_ip_turn_table error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        std::vector<std::string> tmp;
        ret = database_helper_.scan_cache_info(tmp);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cache info error ret is %d", ret);
        }
        else
        {
          for (size_t i = 0; i < tmp.size(); i++)
          {
            v_ns_cache_info_.push_back(std::make_pair(tmp[i], std::string()));
          }
        }
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
          base_last_update_time_ = outparam.base_last_update_time_;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        fill_ip_caculate_map();
      }
      return ret;
    }

    bool BaseResource::need_reload(const int64_t update_time_in_db) const
    {
      return update_time_in_db > base_last_update_time_;
    }

    int BaseResource::get_meta_root_server(const int32_t app_id, int64_t& root_server) const
    {
      int ret = TFS_SUCCESS;
      root_server = 0;
      VMetaRootServerInfo::const_iterator it = v_meta_root_server_info_.begin();
      for (; it != v_meta_root_server_info_.end(); it++)
      {
        if (1 != it->stat_) continue;
        if (0 == it->app_id_)
        {
          if (0 == root_server)
          {
            root_server = tbsys::CNetUtil::strToAddr(it->addr_info_, 0);
          }
        }
        if (app_id == it->app_id_)
        {
          root_server = tbsys::CNetUtil::strToAddr(it->addr_info_, 0);
          break;
        }
      }
      return ret;
    }
    int BaseResource::get_resource_servers(std::vector<uint64_t>& resource_servers) const
    {
      int ret = TFS_SUCCESS;
      resource_servers.clear();
      VResourceServerInfo::const_iterator it = v_resource_server_info_.begin();
      for (; it != v_resource_server_info_.end(); it++)
      {
        if (SERVER_STAT_AVALIABLE == it->stat_)
        {
          uint64_t server = tbsys::CNetUtil::strToAddr(it->addr_info_, 0);
          if (0 == server)
          {
            TBSYS_LOG(WARN, "unknow server %s", it->addr_info_);
          }
          else
          {
            std::vector<uint64_t>::const_iterator vit = find(resource_servers.begin(), resource_servers.end(), server);
            if (vit != resource_servers.end())
            {
              TBSYS_LOG(WARN, "server repeated %s", it->addr_info_);
            }
            else
            {
              resource_servers.push_back(server);
            }
          }
        }
      }
      return ret;
    }

    int BaseResource::get_cluster_infos(const int32_t cluster_group_id,
        std::vector<ClusterRackData>& cluster_rack_datas, std::vector<ClusterData>& cluster_datas_for_update) const
    {
      cluster_rack_datas.clear();
      cluster_datas_for_update.clear();
      int ret = TFS_SUCCESS;
      VClusterRackDuplicateServer::const_iterator rack_duplicate_it;
      VClusterRackInfo::const_iterator rack_info_it;
      VClusterRackGroup::const_iterator rack_group_it = v_cluster_rack_group_.begin();

      for (; rack_group_it != v_cluster_rack_group_.end(); rack_group_it++)
      {
        //find right group
        if (cluster_group_id == rack_group_it->cluster_group_id_
                && GROUP_ACCESS_TYPE_FORBIDEN != rack_group_it->cluster_rack_access_type_)
        {
          int32_t current_rack_id = rack_group_it->cluster_rack_id_;
          ClusterRackData tmp_rack;
          bool read_only = true;
          for (rack_info_it = v_cluster_rack_info_.begin();
              rack_info_it != v_cluster_rack_info_.end(); rack_info_it++)
          {
            //find right cluster rack
            if (rack_info_it->cluster_rack_id_ == current_rack_id
                && CLUSTER_ACCESS_TYPE_FORBIDEN != rack_info_it->cluster_data_.cluster_stat_)
            {
              //found one available cluster;
              ClusterData tmp_cluster;
              tmp_cluster = rack_info_it->cluster_data_;
              if (tmp_cluster.cluster_stat_ > rack_group_it->cluster_rack_access_type_)
              {
                tmp_cluster.access_type_ = rack_group_it->cluster_rack_access_type_;
              }
              else
              {
                tmp_cluster.access_type_ = tmp_cluster.cluster_stat_;
              }
              if (CLUSTER_ACCESS_TYPE_READ_AND_WRITE == tmp_cluster.access_type_)
              {
                read_only = false;
              }
              tmp_rack.cluster_data_.push_back(tmp_cluster);
              if (GROUP_ACCESS_TYPE_READ_AND_WRITE == rack_group_it->cluster_rack_access_type_)
              {
                cluster_datas_for_update.push_back(tmp_cluster);
              }
            }
          }
          if (!tmp_rack.cluster_data_.empty())
          {
            //find duplicate server info
            rack_duplicate_it = v_cluster_rack_duplicate_server_.begin();
            for (; rack_duplicate_it != v_cluster_rack_duplicate_server_.end(); rack_duplicate_it++)
            {
              if (rack_duplicate_it->cluster_rack_id_ == current_rack_id && !read_only)
              {
                tmp_rack.dupliate_server_addr_ = rack_duplicate_it->dupliate_server_addr_;
                tmp_rack.need_duplicate_ = true;
              }
            }
            cluster_rack_datas.push_back(tmp_rack);
          }
        }
      }
      return ret;
    }

    int BaseResource::get_last_modify_time(int64_t& last_modify_time) const
    {
      last_modify_time = base_last_update_time_;
      return TFS_SUCCESS;
    }
    int BaseResource::sort_ns_by_distance(const int32_t app_id, const std::string& app_ip,
            const common::BaseInfo& in_base_info, common::BaseInfo& out_base_info)
    {
      std::string app_caculate_ip;
      std::string app_turn_ip;
      std::map<int32_t, IpReplaceHelper::VIpTransferItem>::const_iterator it;
      it = m_app_ip_turn_table_.find(app_id);
      bool have_been_replaced = false;
      if (it != m_app_ip_turn_table_.end())
      {
        if (TFS_SUCCESS == IpReplaceHelper::replace_ip(it->second, app_ip, app_turn_ip))
        {
          have_been_replaced = true;
        }
      }
      if (!have_been_replaced)
      {
        it = m_app_ip_turn_table_.find(0);
        if (it != m_app_ip_turn_table_.end())
        {
          if (TFS_SUCCESS == IpReplaceHelper::replace_ip(it->second, app_ip, app_turn_ip))
          {
            have_been_replaced = true;
          }
        }
      }
      if (!have_been_replaced)
      {
        app_turn_ip = app_ip;
      }
      TBSYS_LOG(DEBUG, "app_id %d app_ip %s turn ip %s", app_id, app_ip.c_str(), app_turn_ip.c_str());

      if (TFS_SUCCESS !=
          IpReplaceHelper::replace_ip(v_ip_transfer_table_, app_turn_ip, app_caculate_ip))
      {
        TBSYS_LOG(WARN, "ip %s can not found caculate ip", app_turn_ip.c_str());
        app_caculate_ip = app_turn_ip;
      }
      TBSYS_LOG(DEBUG, "app_id %d app_ip %s caculate ip %s", app_id, app_ip.c_str(), app_caculate_ip.c_str());

      out_base_info = in_base_info;
      out_base_info.cluster_infos_.clear();

      std::vector<ClusterRackData>::const_iterator cluster_rack_it = in_base_info.cluster_infos_.begin();
      for (; cluster_rack_it != in_base_info.cluster_infos_.end(); cluster_rack_it++)
      {
        //every cluster rack
        ClusterRackData out_rack_dat(*cluster_rack_it);
        //remove all cluster_data, so we can put soreted cluster_data into out_rack_dat.cluster_data_
        out_rack_dat.cluster_data_.clear();

        std::vector<ClusterData>::const_iterator cluster_it = cluster_rack_it->cluster_data_.begin();
        std::multimap<uint32_t, std::vector<ClusterData>::const_iterator> sorted_helper;
        for (; cluster_it != cluster_rack_it->cluster_data_.end(); cluster_it++)
        {
          uint32_t distance = 0;
          std::string caculate_ns_ip;
          std::map<std::string, std::string>::const_iterator m_caculate_ip_it =
            m_ns_caculate_ip_.find(cluster_it->ns_vip_);
          if (m_caculate_ip_it != m_ns_caculate_ip_.end())
          {
            caculate_ns_ip = m_caculate_ip_it->second;
          }
          else
          {
            TBSYS_LOG(WARN, "can not found caculate ns ip %s", cluster_it->ns_vip_.c_str());
            caculate_ns_ip = cluster_it->ns_vip_;
          }
          distance = IpReplaceHelper::calculate_distance(app_caculate_ip, caculate_ns_ip);
          TBSYS_LOG(DEBUG, "ip %s %s distance %u",
              app_caculate_ip.c_str(), caculate_ns_ip.c_str(), distance);
          sorted_helper.insert(std::make_pair(distance, cluster_it));
        }
        std::multimap<uint32_t, std::vector<ClusterData>::const_iterator>::const_iterator sorted_it;
        for (sorted_it = sorted_helper.begin(); sorted_it != sorted_helper.end(); sorted_it++)
        {
          TBSYS_LOG(DEBUG, "cluster_data_  %s", (sorted_it->second)->ns_vip_.c_str());
          out_rack_dat.cluster_data_.push_back(*(sorted_it->second));
        }
        out_base_info.cluster_infos_.push_back(out_rack_dat);

        VNsCacheInfo::const_iterator ns_cache_info_it = v_ns_cache_info_.begin();
        uint32_t distance = 0;
        uint32_t min_distance = 0xffffffff;  // server ip never be 255.255.255.255
        out_base_info.ns_cache_info_.clear();
        for (; ns_cache_info_it != v_ns_cache_info_.end(); ns_cache_info_it++)
        {
          distance = IpReplaceHelper::calculate_distance(app_caculate_ip, ns_cache_info_it->second);
          TBSYS_LOG(DEBUG, "mindistance %d app_caculate_ip %s cache_caculate_ip %s distance %u",
              min_distance, app_caculate_ip.c_str(), ns_cache_info_it->second.c_str(), distance);
          if (distance < min_distance)
          {
            min_distance = distance;
            out_base_info.ns_cache_info_ = ns_cache_info_it->first;
          }
        }
      }

      return TFS_SUCCESS;
    }

    void BaseResource::fill_ip_caculate_map()
    {
      m_ns_caculate_ip_.clear();
      VClusterRackInfo::const_iterator it = v_cluster_rack_info_.begin();
      for (; it != v_cluster_rack_info_.end(); it++)
      {
        std::string dest;
        std::vector<std::string> ip_items;
        common::Func::split_string(it->cluster_data_.ns_vip_.c_str(), ':', ip_items);
        if (!ip_items.empty())
        {
          if (TFS_SUCCESS != IpReplaceHelper::replace_ip(v_ip_transfer_table_, ip_items[0], dest))
          {
            TBSYS_LOG(WARN, "can not get caculate ip by ns ip :%s will use the original value", ip_items[0].c_str());
            dest = ip_items[0];
          }
          m_ns_caculate_ip_[it->cluster_data_.ns_vip_] = dest;
        }
      }
      std::map<std::string, std::string>::const_iterator mit = m_ns_caculate_ip_.begin();
      for (; mit != m_ns_caculate_ip_.end(); mit++)
      {
        TBSYS_LOG(INFO, "ns_ip %s will be used as %s for distance caculate",
            mit->first.c_str(), mit->second.c_str());
      }
      VNsCacheInfo::iterator ns_cache_it = v_ns_cache_info_.begin();
      for (; ns_cache_it != v_ns_cache_info_.end(); ns_cache_it++)
      {
        std::string dest;
        std::vector<std::string> ip_items;
        common::Func::split_string(ns_cache_it->first.c_str(), ':', ip_items);
        if (!ip_items.empty())
        {
          if (TFS_SUCCESS != IpReplaceHelper::replace_ip(v_ip_transfer_table_, ip_items[0], dest))
          {
            TBSYS_LOG(WARN, "can not get caculate ip by cache ip :%s will use the original value", ip_items[0].c_str());
            dest = ip_items[0];
          }
          ns_cache_it->second = dest;
        }

      }

    }
  }
}
