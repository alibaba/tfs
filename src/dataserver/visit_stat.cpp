/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: visit_stat.cpp 544 2011-06-23 02:32:20Z duanfei@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
//#include "common/config.h"
#include "common/func.h"
#include "common/parameter.h"
#include "common/config_item.h"
#include "visit_stat.h"
#include "dataservice.h"

namespace tfs
{
  namespace dataserver
  {

    using namespace common;
    uint32_t getip(const uint64_t ipport)
    {
      return ipport & 0xFFFFFFFF;
    }

    void VisitStat::check_visit_stat()
    {
      int current_time = time(NULL);
      if (SYSPARAM_DATASERVER.dump_vs_interval_ > 0 && current_time - last_dump_vs_time_
          >= SYSPARAM_DATASERVER.dump_vs_interval_)
      {
        last_dump_vs_time_ = current_time;
        dump_visit_stat();
      }
      if (current_time - last_vs_time_ >= 86400)
      {
        last_vs_time_ = current_time;
        TBSYS_LOG(INFO, "---->RESTART VISIT_STAT.\n");
        visit_stat_map_.clear();
        cache_hit_map_.clear();
      }
      return;
    }

    int VisitStat::stat_visit_count(const int32_t size)
    {
      int32_t key = filesize_category(size);
      return incr_visit_count(key, size);
    }

    int VisitStat::incr_visit_count(const int32_t key, const int64_t count)
    {
      std::map<int32_t, int64_t>::iterator it = visit_stat_map_.find(key);
      if (it != visit_stat_map_.end())
      {
        it->second += count;
        return it->second;
      }
      else
      {
        visit_stat_map_[key] = count;
        return count;
      }
    }

    void VisitStat::dump_visit_stat()
    {
      std::map<int32_t, int64_t>::const_iterator it = visit_stat_map_.begin();
      std::map<int32_t, int64_t>::const_iterator itr = cache_hit_map_.end();
      TBSYS_LOG(INFO, "---->BEGIN DUMP_VISIT_STAT.\n");
      while (it != visit_stat_map_.end())
      {
        itr = cache_hit_map_.find(it->first);
        TBSYS_LOG(INFO, "DUMP_VISIT_STAT: %s  %10" PRI64_PREFIX "u CACHE_HIT: %10" PRI64_PREFIX "u\n",
            filesize_category_desc(it->first).c_str(), it->second, itr == cache_hit_map_.end() ? 0 : itr->second);
        ++it;
      }
      TBSYS_LOG(INFO, "---->END DUMP_VISIT_STAT.\n");
    }

    int32_t VisitStat::filesize_category(int32_t size)
    {
      int32_t ret = 0;
      if (size >= 0 && size < 10 * 1024) // 0 ~ 10k
      {
        ret = size / 1024; // every kilo bytes
      }
      else if (size >= 10240 && size < 102400) // 10k ~ 100k
      {
        ret = (size / 10240) * 10;
        if (size == 10240)
          ret = 10;
      }
      else if (size >= 102400 && size < 1000 * 1024) // 100k ~ 1000k
      {
        ret = (size / 102400) * 100;
        if (size == 102400)
          ret = 100;
      }
      else if (size >= 1000 * 1024)
      {
        ret = 1000;
      }
      return ret;
    }

    std::string VisitStat::filesize_category_desc(int32_t category)
    {
      char buf[STAT_BUF_LEN];
      int32_t i = 1;
      if (category >= 1000)
        return ">1000k";
      for (i = 1; i <= 1000; i *= 10)
      {
        int32_t cat = category / i;
        if (cat == 0)
          break;
      }
      if (category == 0)
        i = 10;
      int32_t base = category / (i / 10);
      sprintf(buf, "%dk~%dk", category, (base + 1) * (i / 10));
      return std::string(buf);
    }

    // -- AccessStatByClientIp
    Throughput& AccessStat::get_ref(const uint32_t key)
    {
      ThroughputStat::iterator it = stats_.find(key);
      if (it == stats_.end())
      {
        Throughput tp;
        memset(&tp, 0, sizeof(tp));
        stats_.insert(ThroughputStat::value_type(key, tp));
        it = stats_.find(key);
      }
      return it->second;
    }

    int32_t AccessStat::incr(const uint64_t ipport, const ThroughputType type, const int64_t count)
    {
      uint32_t ip = getip(ipport);
      switch (type)
      {
      case READ_COUNT:
        get_ref(ip).read_file_count_ += count;
        break;
      case READ_BYTES:
        get_ref(ip).read_byte_ += count;
        break;
      case WRITE_COUNT:
        get_ref(ip).write_file_count_ += count;
        break;
      case WRITE_BYTES:
        get_ref(ip).write_byte_ += count;
        break;
      default:
        break;
      }

      return stats_.size();
    }

    Throughput AccessStat::get_value(const uint32_t ip) const
    {
      Throughput null;
      memset(&null, 0, sizeof(Throughput));
      ThroughputStat::const_iterator it = stats_.find(ip);
      if (it == stats_.end())
      {
        return null;
      }
      return it->second;
    }

    // -- AccessControl
    AccessControl::AccessControl() :
      flag_(0)
    {
      load();
    }

    AccessControl::~AccessControl()
    {
    }

    int AccessControl::read_file_ip_list(const char* filename, std::set<uint32_t>& hosts)
    {
      FILE* fp = fopen(filename, "r");

      if (NULL == fp)
      {
        TBSYS_LOG(ERROR, "cannot open file %s, %s\n", filename, strerror(errno));
        return TFS_ERROR;
      }

      char* line = NULL;
      size_t length = 0;
      ssize_t read = 0;
      while ((read = getline(&line, &length, fp)) != -1)
      {
        if (line[strlen(line) - 1] == '\n')
        {
          line[strlen(line) - 1] = 0;
        }
        if (strlen(line) < 4)
          continue;
        uint32_t server_id = tbsys::CNetUtil::getAddr(line);
        if (server_id != 0)
        {
          TBSYS_LOG(DEBUG, "get access_control_file line: %s", line);
          hosts.insert(server_id);
        }
      }

      fclose(fp);
      if (line)
        free(line);
      return TFS_SUCCESS;
    }

    int32_t AccessControl::load(const char* aclipmask, const char* aclfile)
    {
      if (flag_)
        return TFS_ERROR;
      if (aclipmask)
      {
        std::vector < std::string > vi;
        std::vector < std::string > acip;
        Func::split_string(aclipmask, ';', vi);
        for (uint32_t i = 0; i < vi.size(); ++i)
        {
          acip.clear();
          Func::split_string(vi[i].c_str(), '|', acip);
          if (acip.size() != 2)
            continue;
          uint32_t ip = tbsys::CNetUtil::getAddr(const_cast<char*> (acip[0].c_str()));
          uint32_t mask = tbsys::CNetUtil::getAddr(const_cast<char*> (acip[1].c_str()));
          if (ip && mask)
            acl_.push_back(IpMask((ip & mask), mask));
          TBSYS_LOG(DEBUG, "load access control ipmask: %u, %u", ip, mask);
        }
      }
      if (aclfile)
      {
        read_file_ip_list(aclfile, ipset_);
      }
      return ipset_.size() || acl_.size();
    }

    int32_t AccessControl::load()
    {
      const char* aclipmask = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, "access_control_ipmask");
      const char* aclfile = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, "access_control_file");
      return load(aclipmask, aclfile);
    }

    int32_t AccessControl::reload()
    {
      if (clear() < 0)
        return TFS_ERROR;

      tbsys::CConfig oneconf;
      DataService* service = dynamic_cast<DataService*>(BaseMain::instance());
      if (oneconf.load(service->config_file_.c_str()) != TFS_SUCCESS)
      {
        return TFS_ERROR;
      }
      const char* aclipmask = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, "access_control_ipmask");
      const char* aclfile = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, "access_control_file");
      return load(aclipmask, aclfile);
    }

    int32_t AccessControl::insert_ipmask(const uint32_t ip, const uint32_t mask)
    {
      if (flag_)
        return TFS_ERROR;
      if (ip && mask)
        acl_.push_back(IpMask((ip & mask), mask));
      return acl_.size();
    }

    int32_t AccessControl::insert_iplist(const uint32_t ip)
    {
      if (flag_)
        return TFS_ERROR;
      if (ip)
        ipset_.insert(ip);
      return ipset_.size();
    }

    int32_t AccessControl::clear()
    {
      if (flag_)
        return TFS_ERROR;
      acl_.clear();
      ipset_.clear();
      return TFS_SUCCESS;
    }

    bool AccessControl::deny(const uint64_t ipport, const int32_t op)
    {
      if (!(flag_ & op))
        return false;
      uint32_t ip = getip(ipport);
      if (!ip)
        return false;

      if (ipset_.size() && ipset_.find(ip) != ipset_.end())
        return true;
      for (uint32_t i = 0; i < acl_.size(); ++i)
      {
        if ((ip & acl_[i].mask) == acl_[i].ip)
          return true;
      }
      return false;
    }

  }
}
