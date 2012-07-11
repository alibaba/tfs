/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: visit_stat.h 326 2011-05-24 07:18:18Z daoan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_VISITSTAT_H_
#define TFS_DATASERVER_VISITSTAT_H_

#include "common/internal.h"
#include <tbtimeutil.h>
#include <set>
#include <vector>
#include <string>

namespace tfs
{
  namespace dataserver
  {
    uint32_t getip(const uint64_t ipport);

    class VisitStat
    {
      public:
        VisitStat() :
          last_vs_time_(time(NULL)), last_dump_vs_time_(time(NULL))
        {
        }
        ~VisitStat()
        {
        }

      public:
        void check_visit_stat();
        int stat_visit_count(const int32_t size);

      private:
        void dump_visit_stat();
        int incr_visit_count(const int32_t key, const int64_t count);
        int32_t filesize_category(const int32_t size);
        std::string filesize_category_desc(const int32_t category);

      private:
        static const int32_t STAT_BUF_LEN = 32;
      private:
        int last_vs_time_;
        int last_dump_vs_time_;

        std::map<int32_t, int64_t> cache_hit_map_; // cache hit stat.
        std::map<int32_t, int64_t> visit_stat_map_; // visit count stat.
    };

    class AccessStat
    {
      public:
        typedef __gnu_cxx ::hash_map<uint32_t, common::Throughput> ThroughputStat;
        enum ThroughputType
        {
          READ_COUNT = 1,
          READ_BYTES,
          WRITE_COUNT,
          WRITE_BYTES
        };

      public:
        int32_t incr(const uint64_t ipport, const ThroughputType type, const int64_t count);
        common::Throughput get_value(const uint32_t ip) const;

        const ThroughputStat & get_stat() const
        {
          return stats_;
        }
      private:
        common::Throughput& get_ref(uint32_t key);
      private:
        ThroughputStat stats_;
    };

    class AccessControl
    {
      public:
        enum AclFlagType
        {
          ACL_FLAG = 1,
          ACL_IPMASK,
          ACL_IPLIST,
          ACL_CLEAR,
          ACL_RELOAD
        };

        enum AclOperType
        {
          READ = 0x1,
          WRITE = 0x2,
          UNLINK = 0x4
        };

        struct IpMask
        {
            IpMask() :
              ip(0), mask(0)
            {
            }

            IpMask(uint32_t i, uint32_t m) :
              ip(i), mask(m)
            {
            }

            uint32_t ip;
            uint32_t mask;
        };

      public:
        AccessControl();
        ~AccessControl();

      public:
        int32_t load();
        int32_t reload();
        int32_t load(const char* aclipmask, const char* aclfile);
        bool deny(uint64_t ipport, int32_t op);
        int32_t set_flag(int32_t f)
        {
          flag_ = f;
          return flag_;
        }
        int32_t insert_ipmask(uint32_t ip, uint32_t mask);
        int32_t insert_iplist(uint32_t ip);
        int32_t clear();

      private:
        int32_t flag_;
        std::vector<IpMask> acl_;
        std::set<uint32_t> ipset_;
      private:
        static int read_file_ip_list(const char* filename, std::set<uint32_t>& hosts);
    };

#define TIMER_START()\
    TimeStat time_stat;\
    time_stat.start()

#define TIMER_END() time_stat.end()
#define TIMER_DURATION() time_stat.duration()

    class TimeStat
    {
      public:
        TimeStat() : start_(0), end_(0)
        {
        }
        ~TimeStat()
        {
        }

        inline void start()
        {
          start_ = tbsys::CTimeUtil::getTime();
        }

        inline void end()
        {
          end_ = tbsys::CTimeUtil::getTime();
        }

        inline int64_t duration()
        {
          return end_ - start_;
        }

      private:
        int64_t start_;
        int64_t end_;
    };
  }
}

#endif //TFS_DATASERVER_VISITSTAT_H_
