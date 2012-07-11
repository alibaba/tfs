/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   chuyu
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <Handle.h>
#include <Shared.h>
#include <Memory.hpp>
#include "nameserver.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include "server_collect.h"
#include "block_collect.h"
#include "nameserver.h"
#include "block_manager.h"
#include "layout_manager.h"
#include "task.h"
#include "task_manager.h"
#include <vector>
#include <bitset>

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    class LayoutManagerTest: public virtual ::testing::Test
    {
      public:
        LayoutManagerTest():
          manager_(ns_)
        {

        }
        ~LayoutManagerTest()
        {

        }
      protected:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {
          GFactory::destroy();
        }
        virtual void SetUp()
        {
          SYSPARAM_NAMESERVER.max_replication_ = 2;
          SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
          SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
          SYSPARAM_NAMESERVER.max_block_size_ = 100;
          SYSPARAM_NAMESERVER.max_write_file_count_= 10;
          SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
          SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
          SYSPARAM_NAMESERVER.group_count_ = 1;
          SYSPARAM_NAMESERVER.group_seq_ = 0;
          SYSPARAM_NAMESERVER.heart_interval_ = 2;
          SYSPARAM_NAMESERVER.replicate_wait_time_ = 1;
          SYSPARAM_NAMESERVER.group_mask_= 0xFAFFFFFF;
          SYSPARAM_NAMESERVER.task_expired_time_ = 1;
          SYSPARAM_NAMESERVER.add_primary_block_count_ = 3;
          SYSPARAM_NAMESERVER.safe_mode_time_ = -1;
          SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
          SYSPARAM_NAMESERVER.group_count_ = 1;
          SYSPARAM_NAMESERVER.group_seq_ = 0;
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
          ngi.owner_role_ = NS_ROLE_MASTER;
          ngi.owner_status_ = NS_STATUS_INITIALIZED;
          ngi.lease_id_ = 1;
          ngi.lease_expired_time_ = 0xfffffffff;
        }

        virtual void TearDown()
        {
          manager_.get_server_manager().clear_();
          manager_.get_block_manager().clear_();
          manager_.get_task_manager().clear();
        }
        NameServer ns_;
        LayoutManager manager_;
    };

    TEST_F(LayoutManagerTest, update_relation)
    {
      srand(time(NULL));
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      info.use_capacity_ = 0xfffff;
      info.total_capacity_ = 0xfffffff;
      info.status_ = DATASERVER_STATUS_ALIVE;

      bool isnew = false;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      EXPECT_TRUE(isnew);
      ServerCollect* server = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(1, manager_.get_server_manager().size());

      std::set<BlockInfo> blocks;
      int32_t i = 0;
      uint32_t BASE_BLOCK_ID = 100;
      int32_t COUNT = random() % 10000 + MAX_BLOCK_CHUNK_NUMS;
      for (i =0; i < COUNT; ++i)
      {
        BlockInfo info;
        memset(&info, 0, sizeof(info));
        info.block_id_ = BASE_BLOCK_ID + i;
        blocks.insert(info);
      }
      EXPECT_EQ(EXIT_PARAMETER_ERROR, manager_.update_relation(NULL, blocks, now));
      EXPECT_EQ(TFS_SUCCESS, manager_.update_relation(server, blocks, now));
      EXPECT_EQ(COUNT, server->hold_->size());
      for (i = 0; i < COUNT; i++)
      {
        BlockCollect* pblock = manager_.get_block_manager().get(BASE_BLOCK_ID + i);
        EXPECT_TRUE(NULL != pblock);
        EXPECT_EQ(1, pblock->get_servers_size());
      }
    }

    TEST_F(LayoutManagerTest, update_block_info)
    {
      srand(time(NULL));
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      info.use_capacity_ = 0xfffff;
      info.total_capacity_ = 0xfffffff;
      info.status_ = DATASERVER_STATUS_ALIVE;

      bool isnew = false;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      EXPECT_TRUE(isnew);
      ServerCollect* server = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(1, manager_.get_server_manager().size());

      BlockInfo blkinfo;
      memset(&blkinfo, 0, sizeof(blkinfo));
      blkinfo.block_id_ = 100;
      blkinfo.version_  = 10;
      BlockCollect* pblock = manager_.get_block_manager().insert(blkinfo.block_id_, now);
      EXPECT_TRUE(NULL != pblock);

      blkinfo.block_id_ += 1;
      EXPECT_EQ(EXIT_DATASERVER_NOT_FOUND, manager_.update_block_info(blkinfo, info.id_ + 1, now, false));
      EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, manager_.update_block_info(blkinfo, info.id_, now, false));
      blkinfo.block_id_ -= 1;
      EXPECT_EQ(TFS_SUCCESS, manager_.update_block_info(blkinfo, info.id_, now, false));
    }

    TEST_F(LayoutManagerTest, repair)
    {
      srand(time(NULL));
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      info.use_capacity_ = 0xfffff;
      info.total_capacity_ = 0xfffffff;
      info.status_ = DATASERVER_STATUS_ALIVE;

      uint32_t block_id = 100;
      int32_t flag = UPDATE_BLOCK_MISSING;

      const int32_t MSG_LEN = 256;
      char msg[MSG_LEN];

      // block not exist
      EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, manager_.repair(msg, MSG_LEN, block_id, info.id_, flag, now));
      BlockCollect* pblock = manager_.get_block_manager().insert(block_id, now);
      EXPECT_TRUE(NULL != pblock);
      // server not find
      EXPECT_EQ(EXIT_DATASERVER_NOT_FOUND, manager_.repair(msg, MSG_LEN, block_id, info.id_, flag, now));

      bool isnew = false;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      EXPECT_TRUE(isnew);
      ServerCollect* server = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(1, manager_.get_server_manager().size());

      EXPECT_EQ(EXIT_DATASERVER_NOT_FOUND, manager_.repair(msg, MSG_LEN, block_id, info.id_, flag, now));
    }

    TEST_F(LayoutManagerTest, build_emergency_replicate_)
    {
      bool isnew = false;
      const int32_t MASK = 5;
      uint64_t BASE_SERVER_ID = tbsys::CNetUtil::strToAddr("172.24.80.0", 3200);
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.id_ = BASE_SERVER_ID;
      info.use_capacity_ = 0xfffff;
      info.total_capacity_ = 0xfffffff;
      info.current_load_ = 10;
      info.startup_time_ = now;
      info.last_update_time_ = now;
      info.current_time_ = now;
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.total_tp_.write_byte_ = 0xffffffff;
      info.total_tp_.write_file_count_ = 0xfff;
      info.total_tp_.read_byte_ = 0xffffffff;
      info.total_tp_.read_file_count_ = 0xfffff;

      std::deque<BlockCollect*> queue;
      int64_t need = 0xff;
      EXPECT_TRUE(manager_.build_emergency_replicate_(need, queue, now));
      EXPECT_EQ(0xff, need);

      uint32_t block_id = 100;
      BlockCollect* block = manager_.get_block_manager().insert(block_id, now);
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(PLAN_PRIORITY_NONE == block->check_replicate(now));

      char addr[32];
      snprintf(addr, 32, "172.24.80.%d", 0);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      manager_.get_server_manager().add(info, now, isnew);
      ServerCollect* server = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server);

      EXPECT_TRUE(manager_.build_relation(block, server, now));

      EXPECT_TRUE(PLAN_PRIORITY_NONE == block->check_replicate(now));
      now +=  SYSPARAM_NAMESERVER.replicate_wait_time_;

      EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, block->check_replicate(now));

      EXPECT_TRUE(manager_.build_emergency_replicate_(need, queue, now));
      EXPECT_EQ(0xff, need);

      snprintf(addr, 32, "172.24.80.%d", 1);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      manager_.get_server_manager().add(info, now, isnew);

      queue.push_back(block);
      EXPECT_TRUE(manager_.build_emergency_replicate_(need, queue, now));
      EXPECT_EQ(0xff, need);
      EXPECT_EQ(1U, queue.size());

      uint32_t COUNT = random() % 128 + 16;
      for (uint32_t i = 0; i < COUNT + 1; i++)
      {
        snprintf(addr, 32, "172.24.80.%d", i % MASK);
        info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
        manager_.get_server_manager().add(info, now, isnew);
        info.id_ = tbsys::CNetUtil::strToAddr(addr, 3202);
        manager_.get_server_manager().add(info, now, isnew);
      }

      EXPECT_TRUE(manager_.build_emergency_replicate_(need, queue, now));
      EXPECT_EQ(0xff - 1, need);
    }

    TEST_F(LayoutManagerTest, check_emergency_replicate_)
    {

    }

    TEST_F(LayoutManagerTest, build_replicate_task_)
    {
      bool isnew = false;
      const int32_t MASK = 5;
      uint64_t BASE_SERVER_ID = tbsys::CNetUtil::strToAddr("172.24.80.0", 3200);
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.id_ = BASE_SERVER_ID;
      info.use_capacity_ = 0xfffff;
      info.total_capacity_ = 0xfffffff;
      info.current_load_ = 10;
      info.startup_time_ = now;
      info.last_update_time_ = now;
      info.current_time_ = now;
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.total_tp_.write_byte_ = 0xffffffff;
      info.total_tp_.write_file_count_ = 0xfff;
      info.total_tp_.read_byte_ = 0xffffffff;
      info.total_tp_.read_file_count_ = 0xfffff;

      uint32_t block_id = 100;
      BlockCollect* block = manager_.get_block_manager().insert(block_id, now);
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(PLAN_PRIORITY_NONE == block->check_replicate(now));

      char addr[32];
      snprintf(addr, 32, "172.24.80.%d", 0);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      manager_.get_server_manager().add(info, now, isnew);
      ServerCollect* server = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server);

      EXPECT_TRUE(manager_.build_relation(block, server, now));

      EXPECT_TRUE(PLAN_PRIORITY_NONE == block->check_replicate(now));
      now +=  SYSPARAM_NAMESERVER.replicate_wait_time_;

      EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, block->check_replicate(now));

      int64_t need = 0xff;
      EXPECT_FALSE(manager_.build_replicate_task_(need, block, now));

      snprintf(addr, 32, "172.24.80.%d", 1);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      manager_.get_server_manager().add(info, now, isnew);
      EXPECT_FALSE(manager_.build_replicate_task_(need, block, now));

      uint32_t COUNT = random() % 128 + 16;
      for (uint32_t i = 0; i < COUNT + 1; i++)
      {
        snprintf(addr, 32, "172.24.80.%d", i % MASK);
        info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
        manager_.get_server_manager().add(info, now, isnew);
        info.id_ = tbsys::CNetUtil::strToAddr(addr, 3202);
        manager_.get_server_manager().add(info, now, isnew);
      }
      EXPECT_TRUE(manager_.build_replicate_task_(need, block, now));
      Task* task = manager_.get_task_manager().get_(block->id());
      EXPECT_TRUE(NULL != task);
      EXPECT_EQ(2U, task->runer_.size());
      std::vector<ServerCollect*>::iterator iter = task->runer_.begin();
      uint32_t lan =  Func::get_lan(server->id(), SYSPARAM_NAMESERVER.group_mask_);
      ++iter;
      for (; iter != task->runer_.end(); ++iter)
      {
        ServerCollect* pserver = (*iter);
        uint32_t tmp=  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
        EXPECT_TRUE(tmp != lan);
      }
    }

    TEST_F(LayoutManagerTest, build_compact_task_)
    {
      bool isnew = false;
      uint64_t BASE_SERVER_ID = tbsys::CNetUtil::strToAddr("172.24.80.0", 3200);
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.id_ = BASE_SERVER_ID;

      uint32_t block_id = 100;
      BlockCollect* block = manager_.get_block_manager().insert(block_id, now);
      EXPECT_TRUE(NULL != block);
      EXPECT_FALSE(manager_.build_compact_task_(block, now));

      BlockInfo block_info;
      block_info.block_id_ = 100;
      block_info.file_count_ = 0;
      block_info.version_ = 100;
      block->update(block_info);

      EXPECT_FALSE(block->check_compact());

      block_info.file_count_ = 1;
      block_info.del_file_count_ = 1;
      block_info.size_ = 100;
      block_info.del_size_ = 80;
      block->update(block_info);
      EXPECT_FALSE(manager_.build_compact_task_(block, now));

      info.id_++;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      ServerCollect* server1 = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server1);
      info.id_++;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      ServerCollect* server2 = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server2);
      EXPECT_TRUE(manager_.build_relation(block, server1, now));
      EXPECT_TRUE(manager_.build_relation(block, server2, now));
      EXPECT_TRUE(manager_.build_compact_task_(block, now));
    }

    TEST_F(LayoutManagerTest, build_balance_task_)
    {
      bool isnew = false;
      uint64_t BASE_SERVER_ID = tbsys::CNetUtil::strToAddr("172.24.80.0", 3200);
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.id_ = BASE_SERVER_ID;

      uint32_t block_id = 100;
      BlockCollect* block = manager_.get_block_manager().insert(block_id, now);
      EXPECT_TRUE(NULL != block);

      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      ServerCollect* server1 = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server1);
      char addr[32];
      snprintf(addr, 32, "172.24.80.%d", 1);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      ServerCollect* server2 = manager_.get_server_manager().get(info.id_);
      EXPECT_TRUE(NULL != server2);
      EXPECT_TRUE(manager_.build_relation(block, server1, now));
      EXPECT_TRUE(manager_.build_relation(block, server2, now));
      EXPECT_TRUE(block->check_balance());
      int64_t need = 0xff;
      ServerManager::SERVER_TABLE targets(1024, 1024,0);
      EXPECT_FALSE(manager_.build_balance_task_(need,targets, server1, block,now));

      snprintf(addr, 32, "172.24.80.%d", 3);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      server2 = manager_.get_server_manager().get(info.id_);
      targets.insert(server2);

      snprintf(addr, 32, "172.24.80.%d", 2);
      info.id_ = tbsys::CNetUtil::strToAddr(addr, 3200);
      EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
      server2 = manager_.get_server_manager().get(info.id_);
      targets.insert(server2);
      EXPECT_TRUE(manager_.build_balance_task_(need,targets, server1, block,now));
      Task* task = manager_.get_task_manager().get_(block->id());
      EXPECT_TRUE(NULL != task);
      EXPECT_EQ(2U, task->runer_.size());
      std::vector<ServerCollect*>::iterator iter = task->runer_.begin();
      uint32_t lan =  Func::get_lan(server1->id(), SYSPARAM_NAMESERVER.group_mask_);
      ++iter;
      for (; iter != task->runer_.end(); ++iter)
      {
        ServerCollect* pserver = (*iter);
        uint32_t tmp=  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
        EXPECT_TRUE(tmp != lan);
      }
    }

    TEST_F(LayoutManagerTest, build_redundant_)
    {

    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

