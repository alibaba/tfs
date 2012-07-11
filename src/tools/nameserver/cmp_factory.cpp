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
#include "metacmp.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace tools
  {
    static char suffix(int bit) {return bit ? '*' : ' ';}
    template<class V> int find_key(typename V::value_type key, const V& container)
    {
      typename V::const_iterator iter = container.begin();
      for (; iter != container.end(); iter++)
      {
        if (*iter == key)
        {
          return iter - container.begin();
        }
      }
      return -1;
    }

    template<class V> void cmp_disorder_container(const V& container_a, const V& container_b, bitset<MAX_BITS_SIZE>& flag_a, bitset<MAX_BITS_SIZE>& flag_b)
    {
      int index = 0, count = 0;
      typename V::const_iterator iter = container_a.begin();
      for (; iter != container_a.end(); iter++)
      {
        if ((index = find_key(*iter, container_b)) != -1 )
        {
          count++;
          flag_a.set( distance(container_a.begin(), iter) );
          flag_b.set(index);
        }
      }
      for (int i = container_a.size(); i < MAX_BITS_SIZE; i++)
      {
        flag_a.set(i);
      }
      for (int j = container_b.size(); j < MAX_BITS_SIZE; j++)
      {
        flag_b.set(j);
      }
      flag_a.flip();
      flag_b.flip();
    }
    template<class V> void cmp_order_container(const V& container_a, const V& container_b, bitset<MAX_BITS_SIZE>& flag_a, bitset<MAX_BITS_SIZE>& flag_b)
    {
      typename V::const_iterator ita_begin, it_a;
      ita_begin = it_a = container_a.begin();
      typename V::const_iterator itb_begin, it_b;
      itb_begin = it_b = container_b.begin();

      while ((it_a != container_a.end()) && (it_b != container_b.end()))
      {
        if (*it_a > *it_b)
        {
          flag_b.set(distance(itb_begin, it_b));
          it_b++;
        }
        else if (*it_a < *it_b)
        {
          flag_a.set(distance(ita_begin, it_a));
          it_a++;
        }
        else
        {
          it_a++;
          it_b++;
        }
      }

      while (it_a != container_a.end())
      {
        flag_a.set(distance(ita_begin, it_a++));
      }
      while (it_b != container_b.end())
      {
        flag_b.set(distance(itb_begin, it_b++));
      }
    }
    template<class V> void print_container(V& container, bitset<MAX_BITS_SIZE>& flag, bool reverse)
    {
      if (reverse)
      {
        flag.flip();
      }
      typename V::iterator it = container.begin();

      if (sizeof(typename V::value_type) == sizeof(uint32_t)) // uint32_t: print different block
      {
        int32_t count = static_cast<int32_t> (flag.count());
        for (int32_t i = 0; (it != container.end()) && (count > 0); i++, it++)
        {
          if (flag.test(i))
          {
            cout << *it << " ";
            count--;
          }
        }
        printf(" [%d]\n", static_cast<int32_t>((flag.count() - count)));
      }
      else
      {
        for (int32_t i = 0; it != container.end(); i++, it++)
        {
          printf("%23s%c", tbsys::CNetUtil::addrToString(*it).c_str(),
              suffix(flag[i]));
        }
        printf(" [%zd]\n", container.size());
      }
    }
    inline double get_percent(int i, int t)
    {
      return t > 0 ? (double)i / (double)t * 100 : 0;
    }
    ServerCmp::ServerCmp()
    {
    }
    ServerCmp::~ServerCmp()
    {
    }
    int ServerCmp::cmp(ServerCmp& server_b, const int8_t type)
    {
      int32_t b = 0;
      if (type & SERVER_TYPE_SERVER_INFO)
      {
        if (use_capacity_ != server_b.use_capacity_) { flag_.set(b); server_b.flag_.set(b); }
        if (total_capacity_ != server_b.total_capacity_) { flag_.set(b + 1); server_b.flag_.set(b + 1); server_b.flag_.set(b + 1);}
        if (current_load_ != server_b.current_load_) { flag_.set(b + 2); server_b.flag_.set(b + 2); server_b.flag_.set(b + 2);}
        if (block_count_ != server_b.block_count_) { flag_.set(b + 3); server_b.flag_.set(b + 3);}
        if (total_tp_.write_byte_ != server_b.total_tp_.write_byte_) { flag_.set(b + 4); server_b.flag_.set(b + 4);}
        if (total_tp_.write_file_count_ != server_b.total_tp_.write_file_count_) { flag_.set(b + 5); server_b.flag_.set(b + 5); }
        if (total_tp_.read_byte_ != server_b.total_tp_.read_byte_) { flag_.set(b + 6); server_b.flag_.set(b + 6);}
        if (total_tp_.read_file_count_ != server_b.total_tp_.read_file_count_) { flag_.set(b + 7); server_b.flag_.set(b + 7);}
        if (startup_time_ != server_b.startup_time_) { flag_.set(b + 8); server_b.flag_.set(b + 8);}
        if (hold_ != server_b.hold_) { flag_.set(b + 9); server_b.flag_.set(b + 9); }
        if (writable_ != server_b.writable_) { flag_.set(b + 10); server_b.flag_.set(b + 10); }
        if (master_!= server_b.master_) { flag_.set(b + 11); server_b.flag_.set(b + 11);}
      }

      if (type & SERVER_TYPE_BLOCK_LIST)
      {
        cmp_order_container(hold_, (server_b.hold_), flag_, server_b.flag_);
      }
      if (type & SERVER_TYPE_BLOCK_WRITABLE)
      {
        cmp_order_container(writable_, (server_b.writable_), flag_, server_b.flag_);
      }
      if (type & SERVER_TYPE_BLOCK_MASTER)
      {
        cmp_order_container(master_, (server_b.master_), flag_, server_b.flag_);
      }
      return (flag_.count() || server_b.flag_.count());
    }
    void ServerCmp::dump(ServerCmp& server_b, const int8_t type)
    {
      dump(type);
      server_b.dump(type);
      if (type & SERVER_TYPE_BLOCK_LIST)
      {
        printf("!!!!!!Diff BLOCK: (%zd/%zd = %.2lf%%) BLOCK: (%zd/%zd = %.2lf%%)\n\n",
            flag_.count(), hold_.size(),
            get_percent(flag_.count(), hold_.size()),
            server_b.flag_.count(), server_b.hold_.size(),
            get_percent(server_b.flag_.count(), server_b.hold_.size()));
      }
      if (type & SERVER_TYPE_BLOCK_WRITABLE)
      {
        printf("!!!!!!Diff WRITABLE: (%zd/%zd = %.2lf%%) WRITALBE: (%zd/%zd = %.2lf%%)\n\n",
            flag_.count(), writable_.size(), get_percent(flag_.count(), writable_.size()),
            server_b.flag_.count(), server_b.writable_.size(), get_percent(server_b.flag_.count(), server_b.writable_.size())
            );
      }
      if (type & SERVER_TYPE_BLOCK_MASTER)
      {
        printf("!!!!!!Diff MASTER: (%zd/%zd = %.2lf%%) MASTER: (%zd/%zd = %.2lf%%)\n\n",
            flag_.count(), master_.size(), get_percent(flag_.count(), master_.size()),
            server_b.flag_.count(), server_b.master_.size(), get_percent(server_b.flag_.count(), server_b.master_.size())
            );
      }
    }
    void ServerCmp::dump(const int8_t type)
    {
      int32_t b = 0;
      if (flag_.count() > 0)
      {
        if (type & SERVER_TYPE_SERVER_INFO)
        {
          printf("%-21s %6s%c %4s%c %5u%c %3u%c %5s%c %4ld%c %5s%c %4ld%c %14s%c %4zd%c %4zd%c %4zd%c\n",
            tbsys::CNetUtil::addrToString(id_).c_str(),
            Func::format_size(use_capacity_).c_str(), suffix(flag_[b]),
            Func::format_size(total_capacity_).c_str(), suffix(flag_[b+1]),
            block_count_, suffix(flag_[b+2]),
            current_load_, suffix(flag_[b+3]),
            Func::format_size(total_tp_.write_byte_).c_str(), suffix(flag_[b + 4]),
            total_tp_.write_file_count_, suffix(flag_[b + 5]),
            Func::format_size(total_tp_.read_byte_).c_str(), suffix(flag_[b + 6]),
            total_tp_.read_file_count_, suffix(flag_[b + 7]),
            Func::time_to_str(startup_time_).c_str(), suffix(flag_[b + 8]),
            hold_.size(), suffix(flag_[b + 9]),
            writable_.size(), suffix(flag_[b + 10]),
            master_.size(), suffix(flag_[b + 11])
            );
        }
        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          printf("%-21s  ", tbsys::CNetUtil::addrToString(id_).c_str());
          print_container(hold_, flag_);
        }
        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          printf("%-21s  ", tbsys::CNetUtil::addrToString(id_).c_str());
          print_container(writable_, flag_);
        }
        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          printf("%-21s  ", tbsys::CNetUtil::addrToString(id_).c_str());
          print_container(master_, flag_);
        }
      }
    }
    BlockCmp::BlockCmp()
    {
    }
    BlockCmp::~BlockCmp()
    {
    }
    int BlockCmp::cmp(BlockCmp& block_b, int8_t type)
    {
      int32_t b = 0;
      if (type & BLOCK_CMP_ALL_INFO)
      {
        if (info_.version_ != block_b.info_.version_) { flag_.set(b);  block_b.flag_.set(b); }
        if (info_.file_count_ != block_b.info_.file_count_) { flag_.set(b+1);  block_b.flag_.set(b + 1); }
        if (info_.size_ != block_b.info_.size_) { flag_.set(b + 2);  block_b.flag_.set(b + 2); }
        if (info_.del_file_count_ != block_b.info_.del_file_count_) { flag_.set(b + 3);  block_b.flag_.set(b + 3); }
        if (info_.del_size_ != block_b.info_.del_size_) { flag_.set(b + 4);  block_b.flag_.set(b + 4); }
        if (info_.seq_no_ != block_b.info_.seq_no_) { flag_.set(b + 5); block_b.flag_.set(b + 5); }
        if (server_list_.size() != block_b.server_list_.size()) { flag_.set(b + 6);  block_b.flag_.set(b + 6); }
      }

      if (type & BLOCK_CMP_PART_INFO)
      {
        if (info_.version_ != block_b.info_.version_) { flag_.set(b); block_b.flag_.set(b); }
        if (info_.file_count_ != block_b.info_.file_count_) { flag_.set(b + 1); block_b.flag_.set(b + 1); }
        if (info_.size_ != block_b.info_.size_) { flag_.set(b + 2); block_b.flag_.set(b + 2); }
        if (server_list_.size() != block_b.server_list_.size()) { flag_.set(b + 3);  block_b.flag_.set(b + 3); }
      }

      if (type & BLOCK_CMP_SERVER)
      {
        cmp_disorder_container(server_list_, (block_b.server_list_), flag_, block_b.flag_);
      }
      return (flag_.count() || block_b.flag_.count());
    }
    void BlockCmp::dump(BlockCmp& block_b, const int8_t type)
    {
      dump(type);
      block_b.dump(type);
      if (type & BLOCK_CMP_SERVER)
      {
        printf("!!!!!!Diff SERVER: (%zd/%zd = %.2lf%%) SERVER: (%zd/%zd = %.2lf%%)\n\n",
          flag_.count(), server_list_.size(), get_percent(flag_.count(), server_list_.size()),
          block_b.flag_.count(), block_b.server_list_.size(), get_percent(block_b.flag_.count(), block_b.server_list_.size())
            );
      }
    }
    void BlockCmp::dump(const int8_t type)
    {
      int b = 0;
      if (type & BLOCK_CMP_PART_INFO)
      {
        printf("%-10u %6d%c %10d%c %10d%c %5Zd%c\n",
          info_.block_id_, info_.version_, suffix(flag_[b]), info_.file_count_, suffix(flag_[b]),
          info_.size_, suffix(flag_[b]), server_list_.size(), suffix(flag_[b])
          );
      }
      if (type & BLOCK_CMP_ALL_INFO)
      {
        printf("%-10u %6d%c %10d%c %10d%c %8d%c %10d%c %5u%c %5Zd%c\n",
          info_.block_id_, info_.version_, suffix(flag_[b]), info_.file_count_, suffix(flag_[b+1]),
          info_.size_,  suffix(flag_[b+2]), info_.del_file_count_, suffix(flag_[b+3]),
          info_.del_size_, suffix(flag_[b+4]), info_.seq_no_, suffix(flag_[b+5]),
          server_list_.size(), suffix(flag_[b+6])
          );
      }
      if (type & BLOCK_CMP_SERVER)
      {
        printf("%-10u", info_.block_id_);
        print_container(server_list_, flag_);
      }
    }

    StatCmp::StatCmp() :
      diff_count_(0), total_count_(0)
    {
      more_server_.clear();
      less_server_.clear();
      more_block_.clear();
      less_block_.clear();
    }
    StatCmp::~StatCmp()
    {
    }
    void StatCmp::print_stat(const int8_t type)
    {
      const char* title = (type & BLOCK_TYPE) ? "BLOCK" : "DATASERVER";

      bitset<MAX_BITS_SIZE> flag_more;
      bitset<MAX_BITS_SIZE> flag_less;
      printf("\nIn More %s: ", title);
      (type & BLOCK_TYPE) ? print_container(more_block_, flag_more, true) : print_container(more_server_, flag_more, true);
      printf("\nIn Less %s: ", title);
      (type & BLOCK_TYPE) ? print_container(less_block_, flag_less, true) : print_container(less_server_, flag_less, true);

      //TBSYS_LOG(DEBUG, "block size: %d", more_block_.size());
      diff_count_ += (type & BLOCK_TYPE)? more_block_.size() : more_server_.size();
      printf("\n\n!!!!!!(MasterNs VS SlaveNs) %s Diff Count :  %d / %d = %.2lf%%\n\n", title, diff_count_, total_count_,
          get_percent(diff_count_, total_count_));
    }
  }
}
