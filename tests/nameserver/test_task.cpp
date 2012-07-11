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
 *   duanfei
 *      - initial release
 *
 */
#include <Memory.hpp>
#include <gtest/gtest.h>
#include <tbnet.h>
#include "ns_define.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "server_collect.h"
#include "block_collect.h"
#include "nameserver.h"
#include "block_manager.h"
#include "layout_manager.h"
#include "task_manager.h"
#include "task.h"

#include "message/compact_block_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace nameserver
  {
    class TaskTest:public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
          //GFactory::initialize();
        }
        static void TearDownTestCase()
        {

        }

        TaskTest():
          manager_(ns_)
      {

      }
        virtual ~TaskTest()
        {

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
          SYSPARAM_NAMESERVER.replicate_wait_time_ = 10;
          SYSPARAM_NAMESERVER.task_expired_time_ = 1;
          SYSPARAM_NAMESERVER.group_mask_= 0xffffffff;
        }

        virtual void TearDown()
        {

        }
        NameServer ns_;
        LayoutManager manager_;
    };

    TEST_F(TaskTest, dump)
    {
      bool isnew = false;
      uint64_t BASE_SERVER_ID = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = BASE_SERVER_ID;
      ServerCollect* server = NULL;
      int32_t COUNT = random() % 16 + 20;
      for (int32_t i = 0; i < COUNT; i++, isnew=false)
      {
        EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
        EXPECT_TRUE(isnew);
        server = manager_.get_server_manager().get(info.id_);
        EXPECT_TRUE(NULL != server && server->id() == info.id_);
        info.id_++;
      }
      EXPECT_EQ(COUNT, manager_.get_server_manager().size());
      std::vector<ServerCollect*> runer;
      for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        uint64_t id = random() % COUNT + BASE_SERVER_ID;
        server = manager_.get_server_manager().get(id);
        EXPECT_TRUE(NULL != server);
        runer.push_back(server);
      }
      uint32_t block_id = 0xffff;
      CompactTask task(manager_.get_task_manager(), PLAN_PRIORITY_NORMAL, block_id, runer);

      tbnet::DataBuffer stream;
      task.dump(stream);
      EXPECT_EQ(task.type_, stream.readInt8());
      EXPECT_EQ(task.status_, stream.readInt8());
      EXPECT_EQ(task.priority_, stream.readInt8());
      EXPECT_EQ(task.block_id_, stream.readInt32());
      EXPECT_TRUE(task.last_update_time_ == (time_t)stream.readInt64());
      stream.readInt64();
      uint32_t size = stream.readInt8();
      EXPECT_EQ(task.runer_.size(), size);
      std::vector<ServerCollect*>::iterator iter = runer.begin();
      for (; iter != runer.end(); ++iter)
      {
        server = (*iter);
        EXPECT_EQ(server->id(), stream.readInt64());
      }
      task.dump(TBSYS_LOG_LEVEL_DEBUG);
    }

    TEST_F(TaskTest, compact_handle)
    {
      bool isnew = false;
      uint64_t BASE_SERVER_ID = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = BASE_SERVER_ID;
      ServerCollect* server = NULL;
      int32_t COUNT = SYSPARAM_NAMESERVER.max_replication_;
      for (int32_t i = 0; i < COUNT; i++, isnew=false)
      {
        EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
        EXPECT_TRUE(isnew);
        server = manager_.get_server_manager().get(info.id_);
        EXPECT_TRUE(NULL != server && server->id() == info.id_);
        info.id_++;
      }
      EXPECT_EQ(COUNT, manager_.get_server_manager().size());
      std::vector<ServerCollect*> runer;
      for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        server = manager_.get_server_manager().get(BASE_SERVER_ID + i);
        EXPECT_TRUE(NULL != server);
        runer.push_back(server);
      }

      uint32_t block_id = 0xffff;
      CompactTask task(manager_.get_task_manager(), PLAN_PRIORITY_NORMAL, block_id,runer);
      EXPECT_EQ(TFS_SUCCESS, task.handle());
      EXPECT_EQ(PLAN_STATUS_BEGIN, task.status_);
      EXPECT_EQ(block_id, task.block_id_);
      task.dump(TBSYS_LOG_LEVEL_DEBUG);
    }

    TEST_F(TaskTest, compact_task_do_complete)
    {
      bool isnew = false;
      uint64_t BASE_SERVER_ID = 0xffffff0;
      time_t now = Func::get_monotonic_time();
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = BASE_SERVER_ID;
      info.current_load_ = 30;
      info.total_capacity_ = 0xfffff;
      std::vector<ServerCollect*> runer;
      std::vector<uint64_t> servers;

      ServerCollect* server = NULL;
      int32_t COUNT = SYSPARAM_NAMESERVER.max_replication_;
      for (int32_t i = 0; i < COUNT; i++, isnew=false)
      {
        EXPECT_EQ(TFS_SUCCESS, manager_.get_server_manager().add(info, now, isnew));
        EXPECT_TRUE(isnew);
        server = manager_.get_server_manager().get(info.id_);
        EXPECT_TRUE(NULL != server && server->id() == info.id_);
        info.id_++;
        runer.push_back(server);
      }

      uint32_t block_id = 100;
      BlockCollect* block = manager_.get_block_manager().insert(block_id, now);
      assert(NULL != block);
      CompactTask task(manager_.get_task_manager(), PLAN_PRIORITY_NORMAL, block_id, runer);
      CompactTask::CompactComplete value(BASE_SERVER_ID, block_id, PLAN_STATUS_END);
      task.block_info_.block_id_ = block_id;
      task.block_info_.size_ = 0xfff;

      // compact has not completed, then do nothing
      value.is_complete_ = false;
      value.has_success_ = false;
      value.all_success_ = false;
      value.current_complete_result_ = false;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));
      ngi.owner_role_ = NS_ROLE_MASTER;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));

      // compact has not completed, only one server has done
      value.is_complete_ = false;
      value.has_success_ = true;
      value.all_success_ = false;
      value.current_complete_result_ = true;
      ngi.owner_role_ = NS_ROLE_SLAVE;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));
      EXPECT_EQ(0xfff, block->size());
      ngi.owner_role_ = NS_ROLE_MASTER;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));

      // compact has completed, when all servers compact success, add block as writable to servers
      value.is_complete_ = true;
      value.has_success_ = true;
      value.all_success_ = true;
      ngi.owner_role_ = NS_ROLE_SLAVE;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));
      std::vector<uint64_t> tmp;
      block->get_servers(tmp);
      for (std::vector<uint64_t>::iterator iter = tmp.begin(); iter != tmp.end(); ++iter)
      {
        server = manager_.get_server_manager().get((*iter));
        assert(NULL != server);
        EXPECT_TRUE(server->exist_writable(block));
      }

      ngi.owner_role_ = NS_ROLE_MASTER;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));

      // compact has completed, current server has not complete yet
      value.block_id_ = block_id;
      value.is_complete_ = true;
      value.has_success_ = false;
      value.all_success_ = false;
      value.id_ = BASE_SERVER_ID + 2;
      servers.push_back(BASE_SERVER_ID + 2);
      servers.push_back(BASE_SERVER_ID + 3);
      value.status_ = PLAN_STATUS_TIMEOUT;
      ngi.owner_role_ = NS_ROLE_SLAVE;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));
      ngi.owner_role_ = NS_ROLE_MASTER;
      EXPECT_EQ(TFS_SUCCESS, task.do_complete(value, servers));
    }

    TEST_F(TaskTest, compact_task_check_complete)
    {
      DataServerStatInfo info;
      info.id_ = 0xfffffff0;
      uint32_t block_id = 129;
      std::vector<uint64_t> servers;
      std::vector<ServerCollect*> runer;
      std::pair<uint64_t, PlanStatus> entry;
      CompactTask task(manager_.get_task_manager(), PLAN_PRIORITY_NORMAL, block_id,runer);
      memset(&task.block_info_, 0, sizeof(BlockInfo));
      task.block_info_.block_id_ = block_id;
      task.block_info_.size_ = 0xff;
      for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        entry.first = ++info.id_;
        entry.second = PLAN_STATUS_BEGIN;
        task.complete_status_.push_back(entry);
      }

      CompactTask::CompactComplete complete(info.id_, block_id, PLAN_STATUS_TIMEOUT);
      complete.block_info_.block_id_ = block_id;
      complete.block_info_.size_ = 0xffff;

      task.check_complete(complete, servers);
      EXPECT_TRUE(complete.block_info_.block_id_ == task.block_info_.block_id_);
      EXPECT_TRUE(complete.block_info_.size_ != task.block_info_.size_);
      EXPECT_TRUE(complete.is_complete_ == false);
      EXPECT_TRUE(complete.has_success_ == false);
      EXPECT_TRUE(complete.all_success_ == false);
      EXPECT_TRUE(complete.current_complete_result_ == false);

      complete.status_ = PLAN_STATUS_END;
      task.check_complete(complete, servers);
      EXPECT_TRUE(complete.block_info_.block_id_ == task.block_info_.block_id_);
      EXPECT_TRUE(complete.block_info_.size_ == task.block_info_.size_);
      EXPECT_TRUE(complete.is_complete_ == false);
      EXPECT_TRUE(complete.has_success_ == true);
      EXPECT_TRUE(complete.all_success_ == false);
      EXPECT_TRUE(complete.current_complete_result_ == true);

      CompactTask::CompactComplete completev2(info.id_ - 1, block_id, PLAN_STATUS_TIMEOUT);
      completev2.block_info_.block_id_ = block_id;
      completev2.block_info_.size_ = 0xfffff;

      complete.status_ = PLAN_STATUS_TIMEOUT;
      task.check_complete(complete, servers);

      task.check_complete(completev2, servers);
      EXPECT_TRUE(completev2.block_info_.block_id_ == task.block_info_.block_id_);
      EXPECT_TRUE(completev2.block_info_.size_ != task.block_info_.size_);
      EXPECT_TRUE(completev2.is_complete_ == true);
      EXPECT_TRUE(completev2.has_success_ == false);
      EXPECT_TRUE(completev2.all_success_ == false);
      EXPECT_TRUE(completev2.current_complete_result_ == false);

      complete.status_ = PLAN_STATUS_END;
      task.check_complete(complete, servers);

      completev2.status_ = PLAN_STATUS_END;
      task.check_complete(completev2, servers);
      EXPECT_TRUE(completev2.block_info_.block_id_ == task.block_info_.block_id_);
      EXPECT_TRUE(completev2.block_info_.size_ == task.block_info_.size_);
      EXPECT_TRUE(completev2.is_complete_ == true);
      EXPECT_TRUE(completev2.has_success_ == true);
      EXPECT_TRUE(completev2.all_success_ == true);
      EXPECT_TRUE(completev2.current_complete_result_ == true);
    }

    TEST_F(TaskTest, replicate_task_handle_complete)
    {

    }
  }
}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
