#include "serialization.h"
#include "internal.h"
#include "rc_define.h"
#include <tbsys.h>

namespace tfs
{
  namespace common
  {
    using namespace std;
    int ClusterData::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, cluster_stat_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, access_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, cluster_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, ns_vip_);
      }
      return ret;
    }

    int ClusterData::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &cluster_stat_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &access_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, cluster_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, ns_vip_);
      }
      return ret;
    }

    int64_t ClusterData::length() const
    {
      //int64_t length = INT_SIZE * 2 + Serialization::get_string_length(cluster_id_) + Serialization::get_string_length(ns_vip_);
      //TBSYS_LOG(DEBUG, "ClusterData::length: %"PRI64_PREFIX"d, cluster_id_ length: %"PRI64_PREFIX"d, ns_vip_ length: %"PRI64_PREFIX"d",
      //    length, Serialization::get_string_length(cluster_id_), Serialization::get_string_length(ns_vip_));
      //return length;
      return INT_SIZE * 2 + Serialization::get_string_length(cluster_id_) + Serialization::get_string_length(ns_vip_);
    }

    void ClusterData::dump() const
    {
      TBSYS_LOG(DEBUG, "cluster_stat: %d, access_type: %d, cluster_id: %s, ns_vip: %s",
          cluster_stat_, access_type_, cluster_id_.c_str(), ns_vip_.c_str());
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    int ClusterRackData::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, need_duplicate_);
      }
      if (need_duplicate_ && TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, dupliate_server_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::serialize_list(data, data_len, pos, cluster_data_);
      }
      return ret;
    }

    int ClusterRackData::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&need_duplicate_));
      }
      if (need_duplicate_ && TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, dupliate_server_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::deserialize_list(data, data_len, pos, cluster_data_);
      }
      return ret;
    }

    int64_t ClusterRackData::length() const
    {
      int64_t length = 0;
      if (need_duplicate_)
        length = INT8_SIZE + Serialization::get_string_length(dupliate_server_addr_) + Serialization::get_list_length(cluster_data_);
      else
        length = INT8_SIZE + Serialization::get_list_length(cluster_data_);

      //int64_t length = INT8_SIZE + Serialization::get_string_length(dupliate_server_addr_) + Serialization::get_list_length(cluster_data_);
      //TBSYS_LOG(DEBUG, "ClusterRackData::length: %"PRI64_PREFIX"d, dupliate_server_addr_ length: %"PRI64_PREFIX"d, cluster_data_ length: %"PRI64_PREFIX"d",
      //    length, Serialization::get_string_length(dupliate_server_addr_), Serialization::get_list_length(cluster_data_));
      return length;
    }

    void ClusterRackData::dump() const
    {
      TBSYS_LOG(DEBUG, "need_duplicate: %d, dupliate_server_addr: %s", need_duplicate_, dupliate_server_addr_.c_str());
      for (size_t i = 0; i < cluster_data_.size(); ++i)
      {
        cluster_data_[i].dump();
      }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int BaseInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_vint64(data, data_len, pos, rc_server_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::serialize_list(data, data_len, pos, cluster_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, report_interval_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, meta_root_server_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, ns_cache_info_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::serialize_list(data, data_len, pos, cluster_infos_for_update_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, use_remote_cache_);
      }
      return ret;
    }

    int BaseInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_vint64(data, data_len, pos, rc_server_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::deserialize_list(data, data_len, pos, cluster_infos_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &report_interval_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &meta_root_server_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, ns_cache_info_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::deserialize_list(data, data_len, pos, cluster_infos_for_update_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &use_remote_cache_);
      }
      return ret;
    }

    int64_t BaseInfo::length() const
    {
      int64_t length = INT_SIZE + INT64_SIZE + Serialization::get_vint64_length(rc_server_infos_) + Serialization::get_list_length(cluster_infos_) + INT64_SIZE +
        Serialization::get_string_length(ns_cache_info_) + Serialization::get_list_length(cluster_infos_for_update_) + INT_SIZE;
      //TBSYS_LOG(DEBUG, "BaseInfo::length: %"PRI64_PREFIX"d, rc_server_infos_ length: %"PRI64_PREFIX"d, cluster_infos_ length: %"PRI64_PREFIX"d",
      //    length, Serialization::get_vint64_length(rc_server_infos_), Serialization::get_list_length(cluster_infos_));
      return length;
      //return Serialization::get_vint64_length(rc_server_infos_) + Serialization::get_list_length(cluster_infos_);
    }

    void BaseInfo::dump() const
    {
      for (size_t i = 0; i < rc_server_infos_.size(); ++i)
      {
        TBSYS_LOG(DEBUG, "rc_server %zd: %s", i, tbsys::CNetUtil::addrToString(rc_server_infos_[i]).c_str());
      }
      for (size_t i = 0; i < cluster_infos_.size(); ++i)
      {
        cluster_infos_[i].dump();
      }
      TBSYS_LOG(DEBUG, "report_interval: %d", report_interval_);
      TBSYS_LOG(DEBUG, "modify_time: %"PRI64_PREFIX"d", modify_time_);
      TBSYS_LOG(DEBUG, "root_server: %s", tbsys::CNetUtil::addrToString(meta_root_server_).c_str());
      TBSYS_LOG(DEBUG, "ns_cache_info: %s", ns_cache_info_.c_str());
      for (size_t i = 0; i < cluster_infos_for_update_.size(); ++i)
      {
        cluster_infos_for_update_[i].dump();
      }
      TBSYS_LOG(DEBUG, "use_remote_cache: %d", use_remote_cache_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int SessionBaseInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, session_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_string(data, data_len, pos, client_version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, is_logout_);
      }
      return ret;
    }

    int SessionBaseInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, session_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_string(data, data_len, pos, client_version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&is_logout_));
      }
      return ret;
    }

    int64_t SessionBaseInfo::length() const
    {
      return Serialization::get_string_length(session_id_) + Serialization::get_string_length(client_version_) +
        INT64_SIZE * 3 + INT8_SIZE;
    }

    void SessionBaseInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "session_id: %s, client_version: %s, cache_size: %"PRI64_PREFIX"d, cache_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, is_logout: %d",
          session_id_.c_str(), client_version_.c_str(), cache_size_, cache_time_, modify_time_, is_logout_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int AppOperInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, oper_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_times_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_rt_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, oper_succ_);
      }
      return ret;
    }

    int AppOperInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&oper_type_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_times_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_rt_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &oper_succ_);
      }
      return ret;
    }

    int64_t AppOperInfo::length() const
    {
      return INT_SIZE + INT64_SIZE * 4;
    }

    void AppOperInfo::dump() const
    {
      TBSYS_LOG(DEBUG, "oper_type: %d, oper_times: %"PRI64_PREFIX"d, oper_size: %"PRI64_PREFIX"d, oper_rt: %"PRI64_PREFIX"d, oper_succ: %"PRI64_PREFIX"d",
          oper_type_, oper_times_, oper_size_, oper_rt_, oper_succ_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int SessionStat::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        // set map size
        ret = Serialization::set_int32(data, data_len, pos, app_oper_info_.size());
      }

      if (TFS_SUCCESS == ret)
      {
        std::map<OperType, AppOperInfo>::const_iterator mit = app_oper_info_.begin();
        for ( ; mit != app_oper_info_.end(); ++mit)
        {
          ret = Serialization::set_int32(data, data_len, pos, mit->first);
          if (TFS_SUCCESS == ret)
          {
            ret = mit->second.serialize(data, data_len, pos);
          }

          if (TFS_SUCCESS != ret)
          {
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, cache_hit_ratio_);
      }
      return ret;
    }

    int SessionStat::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      int32_t len = 0;
      if (TFS_SUCCESS == ret)
      {
        // get map size
        ret = Serialization::get_int32(data, data_len, pos, &len);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < len; ++i)
        {
          OperType oper_type;
          AppOperInfo app_oper;
          ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&oper_type));
          if (TFS_SUCCESS == ret)
          {
            ret = app_oper.deserialize(data, data_len, pos);
          }
          if (TFS_SUCCESS == ret)
          {
            if (!app_oper_info_.insert(pair<OperType, AppOperInfo>(oper_type, app_oper)).second)
            {
              TBSYS_LOG(ERROR, "insert the same type: %d, size: %d", oper_type, len);
              ret = TFS_ERROR;
            }
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &cache_hit_ratio_);
      }
      return ret;
    }

    int64_t SessionStat::length() const
    {
      int64_t len = INT_SIZE + INT64_SIZE;
      std::map<OperType, AppOperInfo>::const_iterator mit = app_oper_info_.begin();
      for ( ; mit != app_oper_info_.end(); ++mit)
      {
        len += INT_SIZE;
        len += mit->second.length();
      }
      return len;
    }

    void SessionStat::dump() const
    {
      map<OperType, AppOperInfo>::const_iterator mit = app_oper_info_.begin();
      for ( ; mit != app_oper_info_.end(); ++mit)
      {
        mit->second.dump();
      }
      TBSYS_LOG(DEBUG, "cache_hit_ratio: %"PRI64_PREFIX"d", cache_hit_ratio_);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    int KeepAliveInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = s_base_info_.serialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = s_stat_.serialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, last_report_time_);
      }
      return ret;
    }

    int KeepAliveInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = s_base_info_.deserialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = s_stat_.deserialize(data, data_len, pos);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &last_report_time_);
      }
      return ret;
    }

    int64_t KeepAliveInfo::length() const
    {
      return s_base_info_.length() + s_stat_.length() + INT64_SIZE;
    }

    void KeepAliveInfo::dump() const
    {
      s_base_info_.dump();
      s_stat_.dump();
      TBSYS_LOG(DEBUG, "last_report_time: %"PRI64_PREFIX"d", last_report_time_);
    }
  }
}
