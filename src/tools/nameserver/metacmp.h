/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_METACMP_H_
#define TFS_TOOLS_METACMP_H_

#include <stdio.h>
#include <map>
#include "cmp_factory.h"

namespace tfs
{
  namespace tools
  {
    class CmpInfo
    {
      public:
        CmpInfo();
        virtual ~CmpInfo();

        int set_ns_ip(const std::string& master_ip_port, const std::string& slave_ip_port);
        int compare(ComType cmp_type, const int8_t type, const int32_t num);

      private:
        int process_data(common::SSMScanParameter& param, tbnet::DataBuffer& data, const int8_t role, int32_t& map_count);
        int init_param(ComType cmp_type, const int8_t type, const int32_t num, common::SSMScanParameter& param);
        int get_cmp_map(common::SSMScanParameter& param, const int8_t role, bool& need_loop, int32_t& map_count);
        void print_head(ComType cmp_type, const int8_t type) const;
        std::map<uint64_t, ServerCmp> master_server_map_;
        std::map<uint64_t, ServerCmp> slave_server_map_;
        std::map<uint32_t, BlockCmp> master_block_map_;
        std::map<uint32_t, BlockCmp> slave_block_map_;
        uint64_t master_ns_ip_;
        uint64_t slave_ns_ip_;
        template<class M> void do_cmp(M& master_map, M& slave_map, StatCmp& stat, int8_t type, bool final_cmp = false)
        {
          typename M::iterator m_iter, s_iter;
          std::bitset<MAX_BITS_SIZE> flag;

          m_iter = master_map.begin();
          for (; m_iter != master_map.end();)
          {
            s_iter = slave_map.find(m_iter->first);
            if (s_iter != slave_map.end())
            {
              flag.set( distance(slave_map.begin(), s_iter) );
              if (m_iter->second.cmp(s_iter->second, type))
              {
                flag.set( distance(slave_map.begin(), s_iter) );
                m_iter->second.dump(s_iter->second, type);
                stat.diff_count_++;
              }
              master_map.erase(m_iter++);
              slave_map.erase(s_iter);
            }
            else
            {
              //TBSYS_LOG(DEBUG, "more id: %lu", m_iter->second.get_id());
              if (final_cmp)
              {
                stat.push_back(m_iter->second.get_id(), PUSH_MORE);
              }
              ++m_iter;
            }
          }

          flag.flip();
          int32_t diff_count = flag.count();
          s_iter = slave_map.begin();
          for (int32_t i = 0; (s_iter != slave_map.end()) && (diff_count > 0); i++, s_iter++)
          {
            if (flag.test(i))
            {
              diff_count--;
              if (final_cmp)
              {
                //TBSYS_LOG(DEBUG, "less id: %lu", s_iter->second.get_id());
                stat.push_back(s_iter->second.get_id(), PUSH_LESS);
              }
            }
          }
        }
    };
  }
}

#endif
