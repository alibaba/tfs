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
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMESERVER_SERVER_MANAGER_H_
#define TFS_NAMESERVER_SERVER_MANAGER_H_

#include <map>
#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include "gc.h"
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    struct ServerIdCompare
    {
      bool operator ()(const ServerCollect* lhs, const ServerCollect* rhs) const
      {
        assert(NULL != lhs);
        assert(NULL != rhs);
        return lhs->id() < rhs->id();
      }
    };
    class ServerManager
    {
      typedef std::multimap<int64_t, ServerCollect*> SORT_MAP;
      typedef SORT_MAP::iterator SORT_MAP_ITER;
      typedef SORT_MAP::const_iterator SORT_MAP_CONST_ITER;
      typedef std::map<int64_t, SORT_MAP > GROUP_MAP;
      typedef GROUP_MAP::iterator GROUP_MAP_ITER;
      typedef GROUP_MAP::const_iterator GROUP_MAP_CONST_ITER;
      typedef common::TfsSortedVector<ServerCollect*,ServerIdCompare> SERVER_TABLE;
      typedef SERVER_TABLE::iterator SERVER_TABLE_ITER;
      #ifdef TFS_GTEST
      friend class ServerManagerTest;
      friend class LayoutManagerTest;
      void clear_();
      FRIEND_TEST(ServerManagerTest, add_remove_get);
      FRIEND_TEST(ServerManagerTest, get_range_servers_);
      FRIEND_TEST(ServerManagerTest, pop_from_dead_queue);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      #endif
      public:
      explicit ServerManager(LayoutManager& manager);
      virtual ~ServerManager();
      int add(const common::DataServerStatInfo& info, const time_t now, bool& isnew);
      int remove(const uint64_t server, const time_t now);
      void update_last_time(const uint64_t server, const time_t now);

      ServerCollect* get(const uint64_t server) const;
      int pop_from_dead_queue(common::ArrayHelper<ServerCollect*>& results, const time_t now);
      int64_t size() const;

      int add_report_block_server(ServerCollect* server, const time_t now, const bool rb_expire = false);
      int del_report_block_server(ServerCollect* server);
      int get_and_move_report_block_server(common::ArrayHelper<ServerCollect*>& servers, const int64_t max_slot_num);
      bool report_block_server_queue_empty() const;
      bool has_report_block_server() const;
      void clear_report_block_server_table();
      int64_t get_report_block_server_queue_size() const;

      int get_alive_servers(std::vector<uint64_t>& servers) const;
      int get_dead_servers(common::ArrayHelper<uint64_t>& servers, NsGlobalStatisticsInfo& info, const time_t now) const;
      bool get_range_servers(common::ArrayHelper<ServerCollect*>& result, const uint64_t begin, const int32_t count) const;

      int move_statistic_all_server_info(int64_t& total_capacity, int64_t& total_use_capacity,
          int64_t& alive_server_nums) const;
      int move_split_servers(std::multimap<int64_t, ServerCollect*>& source,
          SERVER_TABLE& targets, const double percent) const;

      int scan(common::SSMScanParameter& param, int32_t& should, int32_t& start, int32_t& next, bool& all_over) const;

      void set_all_server_next_report_time(const time_t now = common::Func::get_monotonic_time());

      int build_relation(ServerCollect* server, const BlockCollect* block,
          const bool writable, const bool master);

      bool relieve_relation(ServerCollect* server, const BlockCollect* block);

      int choose_writable_block(BlockCollect*& result);

      //choose one or more servers to create new block
      int choose_create_block_target_server(common::ArrayHelper<ServerCollect*>& result,
          common::ArrayHelper<ServerCollect*>& news, const int32_t count) const;

      //choose a server to replicate or move
      //replicate method
      int choose_replicate_source_server(ServerCollect*& result, const common::ArrayHelper<ServerCollect*>& source) const;
      int choose_replicate_target_server(ServerCollect*& result, const common::ArrayHelper<ServerCollect*>& except) const;

      //move method
      int choose_move_target_server(ServerCollect*& result,SERVER_TABLE& source,
          common::ArrayHelper<ServerCollect*>& except) const;

      //delete method
      int choose_excess_backup_server(ServerCollect*& result, const common::ArrayHelper<ServerCollect*>& sources) const;

      int expand_ratio(int32_t& index, const float expand_ratio = 0.1);
      private:
      DISALLOW_COPY_AND_ASSIGN(ServerManager);
      ServerCollect* get_(const uint64_t server) const;

      void get_lans_(std::set<uint32_t>& lans, const common::ArrayHelper<ServerCollect*>& source) const;

      bool relieve_relation_(ServerCollect* server, const time_t now);

      bool get_range_servers_(common::ArrayHelper<ServerCollect*>& result, const uint64_t begin, const int32_t count) const;

      void move_split_servers_(std::multimap<int64_t, ServerCollect*>& source,
          std::multimap<int64_t, ServerCollect*>& outside,
          SERVER_TABLE& targets, const ServerCollect* server, const double percent) const;

      int choose_writable_server_lock_(ServerCollect*& result);
      int choose_writable_server_random_lock_(ServerCollect*& result);

      int choose_replciate_random_choose_server_base_lock_(ServerCollect*& result,
          const common::ArrayHelper<ServerCollect*>& except, const std::set<uint32_t>& lans) const;
      int choose_replciate_random_choose_server_extend_lock_(ServerCollect*& result,
          const common::ArrayHelper<ServerCollect*>& except, const std::set<uint32_t>& lans) const;

      int del_report_block_server_(ServerCollect* server);
      private:
      LayoutManager& manager_;
      SERVER_TABLE servers_;
      SERVER_TABLE dead_servers_;
      SERVER_TABLE wait_report_block_servers_;
      SERVER_TABLE current_reporting_block_servers_;
      tbutil::Mutex wait_report_block_server_mutex_;
      mutable common::RWLock rwmutex_;
      int32_t write_index_;
    };
  }/** nameserver **/
}/** tfs **/

#endif /* SERVER_MANAGER_H_*/
