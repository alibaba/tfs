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

#ifndef TFS_NAMESERVER_LAYOUT_MANAGER_H_
#define TFS_NAMESERVER_LAYOUT_MANAGER_H_

#include <pthread.h>
#include <Timer.h>
#include "gc.h"
#include "common/lock.h"
#include "block_collect.h"
#include "server_collect.h"
#include "common/base_packet.h"
#include "oplog_sync_manager.h"
#include "client_request_server.h"
#include "task_manager.h"
#include "server_manager.h"
#include "block_manager.h"
#include "common/tfs_vector.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    class LayoutManager
    {
      #ifdef TFS_GTEST
      friend class LayoutManagerTest;
      FRIEND_TEST(LayoutManagerTest, update_relation);
      FRIEND_TEST(LayoutManagerTest, update_block_info);
      FRIEND_TEST(LayoutManagerTest, repair);
      FRIEND_TEST(LayoutManagerTest, build_emergency_replicate_);
      FRIEND_TEST(LayoutManagerTest, check_emergency_replicate_);
      FRIEND_TEST(LayoutManagerTest, build_replicate_task_);
      FRIEND_TEST(LayoutManagerTest, build_compact_task_);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      FRIEND_TEST(LayoutManagerTest, build_redundant_);
      #endif
      friend class ClientRequestServer;
      friend int OpLogSyncManager::replay_helper_do_oplog(const time_t now, const int32_t type, const char* const data, const int64_t length , int64_t& pos);
      typedef common::TfsSortedVector<ServerCollect*,ServerIdCompare> SERVER_TABLE;
      typedef SERVER_TABLE::iterator SERVER_TABLE_ITER;
      public:
      explicit LayoutManager(NameServer& manager);
      virtual ~LayoutManager();

      int initialize();
      void wait_for_shut_down();
      void destroy();

      inline NameServer& get_name_server() { return manager_;}

      inline ClientRequestServer& get_client_request_server() { return client_request_server_;}

      inline BlockManager& get_block_manager() { return block_manager_;}

      inline ServerManager& get_server_manager() { return server_manager_;}

      inline TaskManager& get_task_manager() { return task_manager_;}

      inline OpLogSyncManager& get_oplog_sync_mgr() { return oplog_sync_mgr_;}

      inline GCObjectManager& get_gc_manager() { return gc_manager_;}

      int update_relation(ServerCollect* server,const std::set<common::BlockInfo>& blocks, const time_t now);
      bool build_relation(BlockCollect* block, ServerCollect* server, const time_t now, const bool set = false);
      bool relieve_relation(BlockCollect* block, ServerCollect* server, const time_t now, const int8_t flag);

      int update_block_info(const common::BlockInfo& info, const uint64_t server, const time_t now, const bool addnew);

      int repair(char* msg, const int32_t lenght, const uint32_t block_id,
          const uint64_t server, const int32_t flag, const time_t now);

      int scan(common::SSMScanParameter& stream);

      int handle_task_complete(common::BasePacket* msg);

      int open_helper_create_new_block_by_id(const uint32_t block_id);

      int block_oplog_write_helper(const int32_t cmd, const common::BlockInfo& info,
          const std::vector<uint32_t>& blocks, const std::vector<uint64_t>& servers, const time_t now);

      int set_runtime_param(const uint32_t index, const uint32_t value, const int64_t length, char *retstr);

      void switch_role(const time_t now = common::Func::get_monotonic_time());
      private:
      void rotate_(const time_t now);
      uint32_t get_alive_block_id_();
      void build_();
      void balance_();
      void timeout_();
      void redundant_();
      void check_all_server_isalive_();
      void add_block_in_all_server_();
      void check_all_server_report_block_();
      int touch_(bool& promote, const common::ArrayHelper<ServerCollect*>& servers, const time_t now);

      int add_new_block_helper_write_log_(const uint32_t block_id, const common::ArrayHelper<ServerCollect*>& server, const time_t now);
      int add_new_block_helper_send_msg_(const uint32_t block_id, const common::ArrayHelper<ServerCollect*>& servers);
      int add_new_block_helper_build_relation_(BlockCollect* block, const common::ArrayHelper<ServerCollect*>& server, const time_t now);
      BlockCollect* add_new_block_(uint32_t& block_id, ServerCollect* server = NULL, const time_t now = common::Func::get_monotonic_time());
      BlockCollect* add_new_block_helper_create_by_id_(const uint32_t block_id, const time_t now);
      BlockCollect* add_new_block_helper_create_by_system_(uint32_t& block_id, ServerCollect* server, const time_t now);

      bool build_emergency_replicate_(int64_t& need, const time_t now);
      bool check_emergency_replicate_(common::ArrayHelper<BlockCollect*>& result,const int32_t count, const time_t now);
      bool build_replicate_task_(int64_t& need, const BlockCollect* block, const time_t now);
      bool build_compact_task_(const BlockCollect* block, const time_t now);
      bool build_balance_task_(int64_t& need, common::TfsSortedVector<ServerCollect*,ServerIdCompare>& targets,
          const ServerCollect* source, const BlockCollect* block, const time_t now);
      bool build_redundant_(int64_t& need, const time_t now);
      int64_t has_space_in_task_queue_() const;

      class BuildPlanThreadHelper: public tbutil::Thread
      {
        public:
          explicit BuildPlanThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~BuildPlanThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(BuildPlanThreadHelper);
          LayoutManager& manager_;
      };
      typedef tbutil::Handle<BuildPlanThreadHelper> BuildPlanThreadHelperPtr;

      class RunPlanThreadHelper : public tbutil::Thread
      {
        public:
          explicit RunPlanThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~RunPlanThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(RunPlanThreadHelper);
          LayoutManager& manager_;
      };
      typedef tbutil::Handle<RunPlanThreadHelper> RunPlanThreadHelperPtr;

      class CheckDataServerThreadHelper: public tbutil::Thread
      {
        public:
          explicit CheckDataServerThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~CheckDataServerThreadHelper(){}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(CheckDataServerThreadHelper);
      };
      typedef tbutil::Handle<CheckDataServerThreadHelper> CheckDataServerThreadHelperPtr;

      class AddBlockInAllServerThreadHelper : public tbutil::Thread
      {
        public:
          explicit AddBlockInAllServerThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~AddBlockInAllServerThreadHelper() {}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(AddBlockInAllServerThreadHelper);
      };
      typedef tbutil::Handle<AddBlockInAllServerThreadHelper> AddBlockInAllServerThreadHelperPtr;

      class CheckDataServerReportBlockThreadHelper: public tbutil::Thread
      {
        public:
          explicit CheckDataServerReportBlockThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~CheckDataServerReportBlockThreadHelper() {}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(CheckDataServerReportBlockThreadHelper);
      };
      typedef tbutil::Handle<CheckDataServerReportBlockThreadHelper> CheckDataServerReportBlockThreadHelperPtr;

      class BuildBalanceThreadHelper: public tbutil::Thread
      {
        public:
          explicit BuildBalanceThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~BuildBalanceThreadHelper() {}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(BuildBalanceThreadHelper);
      };
      typedef tbutil::Handle<BuildBalanceThreadHelper> BuildBalanceThreadHelperPtr;

      class TimeoutThreadHelper: public tbutil::Thread
      {
        public:
          explicit TimeoutThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~TimeoutThreadHelper() {}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(TimeoutThreadHelper);
      };
      typedef tbutil::Handle<TimeoutThreadHelper> TimeoutThreadHelperPtr;

      class RedundantThreadHelper: public tbutil::Thread
      {
        public:
          explicit RedundantThreadHelper(LayoutManager& manager):
            manager_(manager) {start(THREAD_STATCK_SIZE);}
          virtual ~RedundantThreadHelper() {}
          void run();
        private:
          LayoutManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(RedundantThreadHelper);
      };
      typedef tbutil::Handle<RedundantThreadHelper> RedundantThreadHelperPtr;
      private:
      BuildPlanThreadHelperPtr build_plan_thread_;
      RunPlanThreadHelperPtr run_plan_thread_;
      CheckDataServerThreadHelperPtr check_dataserver_thread_;
      AddBlockInAllServerThreadHelperPtr add_block_in_all_server_thread_;
      CheckDataServerReportBlockThreadHelperPtr check_dataserver_report_block_thread_;
      BuildBalanceThreadHelperPtr balance_thread_;
      TimeoutThreadHelperPtr timeout_thread_;
      RedundantThreadHelperPtr redundant_thread_;

      time_t  zonesec_;
      time_t  last_rotate_log_time_;
      int32_t plan_run_flag_;

      NameServer& manager_;
      BlockManager block_manager_;
      ServerManager server_manager_;
      TaskManager task_manager_;
      OpLogSyncManager oplog_sync_mgr_;
      ClientRequestServer client_request_server_;
      GCObjectManager gc_manager_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* LAYOUTMANAGER_H_ */
