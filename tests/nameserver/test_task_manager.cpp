/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_task_manager.cpp 5 2011-03-29 11:11:56Z
 *
 * Authors:
 *  duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "server_collect.h"
#include "block_collect.h"
#include "nameserver.h"
#include "block_manager.h"
#include "layout_manager.h"
#include "task.h"
#include "task_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    class TaskManagerTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {

        }
        TaskManagerTest():
          manager_(ns_)
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
          SYSPARAM_NAMESERVER.max_task_in_machine_nums_ = 12;
        }

        ~TaskManagerTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {
          manager_.get_task_manager().clear();
        }
      protected:
        NameServer ns_;
        LayoutManager manager_;
    };

    TEST_F(TaskManagerTest, add)
    {
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;

      ServerCollect server(info, now);
      info.id_++;
      ServerCollect server2(info, now);
      std::vector<ServerCollect*> runer;
      runer.push_back(&server);
      runer.push_back(&server2);

      uint32_t id = 100;
      PlanType type = PLAN_TYPE_REPLICATE;
      PlanPriority priority = PLAN_PRIORITY_NORMAL;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(runer.size(), manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());

      uint64_t seqno = manager_.get_task_manager().seqno_;
      Task* task = manager_.get_task_manager().get_by_id(seqno);
      EXPECT_TRUE(NULL != task);
      Task* ptask = manager_.get_task_manager().get_(id);
      EXPECT_TRUE(NULL != ptask);
      EXPECT_TRUE(task == ptask);
      EXPECT_TRUE(task->seqno_ == ptask->seqno_);
      EXPECT_TRUE(task->type_ == ptask->type_);
      EXPECT_TRUE(task->block_id_== ptask->block_id_);
      EXPECT_TRUE(task->need_add_server_to_map());

      EXPECT_EQ(EXIT_TASK_EXIST_ERROR, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(runer.size(), manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());
      EXPECT_TRUE(seqno == manager_.get_task_manager().seqno_);
      ptask = manager_.get_task_manager().get_by_id(manager_.get_task_manager().seqno_);
      EXPECT_TRUE(NULL != ptask);
      EXPECT_TRUE(task == ptask);
      EXPECT_TRUE(task->seqno_ == ptask->seqno_);
      EXPECT_TRUE(task->type_ == ptask->type_);
      EXPECT_TRUE(task->block_id_== ptask->block_id_);
      EXPECT_TRUE(task->need_add_server_to_map());

      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().remove(task));
      manager_.get_task_manager().pending_queue_.clear();
      task = manager_.get_task_manager().get_by_id(manager_.get_task_manager().seqno_);
      EXPECT_TRUE(NULL == task);
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());

      id = 101;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(runer.size(), manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());
      EXPECT_TRUE((seqno+1) == manager_.get_task_manager().seqno_);
      seqno = manager_.get_task_manager().seqno_;
      task = manager_.get_task_manager().get_by_id(seqno);

      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().remove(task));
      manager_.get_task_manager().pending_queue_.clear();
      task = manager_.get_task_manager().get_by_id(manager_.get_task_manager().seqno_);
      EXPECT_TRUE(NULL == task);
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());

      type = PLAN_TYPE_COMPACT;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(runer.size(), manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());
      EXPECT_TRUE((seqno+1) == manager_.get_task_manager().seqno_);
      seqno = manager_.get_task_manager().seqno_;
      task = manager_.get_task_manager().get_by_id(seqno);
      EXPECT_TRUE(task->type_ == PLAN_TYPE_COMPACT);
    }

    TEST_F(TaskManagerTest, remove_get_exist)
    {
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;

      ServerCollect server(info, now);
      info.id_++;
      ServerCollect server2(info, now);
      std::vector<ServerCollect*> runer;
      runer.push_back(&server);
      runer.push_back(&server2);

      uint32_t id = 100;
      PlanType type = PLAN_TYPE_REPLICATE;
      PlanPriority priority = PLAN_PRIORITY_NORMAL;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(runer.size(), manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());

      uint64_t seqno = manager_.get_task_manager().seqno_;
      Task* task = manager_.get_task_manager().get_by_id(seqno);
      EXPECT_TRUE(NULL != task);
      Task* block_to_task = manager_.get_task_manager().get_(id);
      Task* server_to_task = manager_.get_task_manager().get_(info.id_);
      EXPECT_TRUE(task == block_to_task);
      EXPECT_TRUE(task == server_to_task);
      server_to_task = manager_.get_task_manager().get_(info.id_, true);
      EXPECT_TRUE(NULL == server_to_task);
      EXPECT_TRUE(manager_.get_task_manager().exist(id));
      EXPECT_TRUE(manager_.get_task_manager().exist(info.id_));
      EXPECT_TRUE(manager_.get_task_manager().exist(info.id_ - 1));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_, true));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_ - 1, true));

      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().remove(task));
      manager_.get_task_manager().pending_queue_.clear();
      task = manager_.get_task_manager().get_by_id(manager_.get_task_manager().seqno_);
      EXPECT_TRUE(NULL == task);
      EXPECT_EQ(0U, manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());
      block_to_task = manager_.get_task_manager().get_(id);
      server_to_task = manager_.get_task_manager().get_(info.id_);
      EXPECT_TRUE(NULL == block_to_task);
      EXPECT_TRUE(NULL == server_to_task);
      server_to_task = manager_.get_task_manager().get_(info.id_, true);
      EXPECT_TRUE(NULL == server_to_task);
      EXPECT_FALSE(manager_.get_task_manager().exist(id));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_ - 1));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_, true));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_ - 1, true));

      type = PLAN_TYPE_COMPACT;
      EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add(id, runer, type, priority));
      EXPECT_EQ(runer.size(), manager_.get_task_manager().server_to_tasks_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().machine_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_EQ(1U, manager_.get_task_manager().pending_queue_.size());
      EXPECT_EQ(0U, manager_.get_task_manager().running_queue_.size());
      block_to_task = manager_.get_task_manager().get_(id);
      server_to_task = manager_.get_task_manager().get_(info.id_, true);
      EXPECT_TRUE(NULL != block_to_task);
      EXPECT_TRUE(NULL != server_to_task);
      server_to_task = manager_.get_task_manager().get_(info.id_);
      EXPECT_TRUE(NULL == server_to_task);
      EXPECT_TRUE(manager_.get_task_manager().exist(id));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_));
      EXPECT_FALSE(manager_.get_task_manager().exist(info.id_ - 1));
      EXPECT_TRUE(manager_.get_task_manager().exist(info.id_, true));
      EXPECT_TRUE(manager_.get_task_manager().exist(info.id_ - 1, true));
    }

    TEST_F(TaskManagerTest, has_space_do_task_in_machine)
    {
      uint64_t BASE_SERVER_ID = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200);
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = BASE_SERVER_ID;

      uint32_t id = 100;
      uint32_t COUNT = SYSPARAM_NAMESERVER.max_task_in_machine_nums_ - 1;
      PlanType type = PLAN_TYPE_REPLICATE;
      PlanPriority priority = PLAN_PRIORITY_NORMAL;
      uint32_t i = 0;
      for (i = 0; i < COUNT; i++)
      {
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.21", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server(info, now);
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server2(info, now);
        std::vector<ServerCollect*> runer;
        runer.push_back(&server);
        runer.push_back(&server2);
        EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add((id+i), runer, type, priority));
      }
      EXPECT_EQ(COUNT, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID));

      COUNT = SYSPARAM_NAMESERVER.max_task_in_machine_nums_ + 1;
      for (; i < COUNT; i++)
      {
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.21", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server(info, now);
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server2(info, now);
        std::vector<ServerCollect*> runer;
        runer.push_back(&server);
        runer.push_back(&server2);
        EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add((id+i), runer, type, priority));
      }
      EXPECT_EQ(COUNT, manager_.get_task_manager().block_to_tasks_.size());
      EXPECT_FALSE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID));
      EXPECT_FALSE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, true));
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, false));
      manager_.get_task_manager().clear();

      COUNT = SYSPARAM_NAMESERVER.max_task_in_machine_nums_ /2;
      for (; i < COUNT; i++)
      {
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.21", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server(info, now);
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server2(info, now);
        std::vector<ServerCollect*> runer;
        runer.push_back(&server);
        runer.push_back(&server2);
        EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add((id+i), runer, type, priority));
      }
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, true));
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, false));

      manager_.get_task_manager().clear();
      COUNT = SYSPARAM_NAMESERVER.max_task_in_machine_nums_ /2 + 1;
      for (i = 0; i < COUNT; i++)
      {
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.21", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server(info, now);
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server2(info, now);
        std::vector<ServerCollect*> runer;
        if (i % 2 == 0)
        {
          runer.push_back(&server);
          runer.push_back(&server2);
        }
        else
        {
          runer.push_back(&server2);
          runer.push_back(&server);
        }
        EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add((id+i), runer, type, priority));
      }
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, true));
      EXPECT_TRUE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, false));

      manager_.get_task_manager().clear();
      COUNT = SYSPARAM_NAMESERVER.max_task_in_machine_nums_  + 4;
      for (i = 0; i < COUNT; i++)
      {
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.21", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server(info, now);
        info.id_ = tbsys::CNetUtil::strToAddr("172.24.80.22", 3200 + i);
        TBSYS_LOG(DEBUG, "addr: %s", tbsys::CNetUtil::addrToString(info.id_).c_str());
        ServerCollect server2(info, now);
        std::vector<ServerCollect*> runer;
        if (i % 2 == 0)
        {
          runer.push_back(&server);
          runer.push_back(&server2);
        }
        else
        {
          runer.push_back(&server2);
          runer.push_back(&server);
        }
        EXPECT_EQ(TFS_SUCCESS, manager_.get_task_manager().add((id+i), runer, type, priority));
      }
      EXPECT_FALSE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, true));
      EXPECT_FALSE(manager_.get_task_manager().has_space_do_task_in_machine(BASE_SERVER_ID, false));
      uint32_t mask = tbsys::CNetUtil::getAddr("255.255.255.250");
      uint32_t mask2 = Func::get_addr("255.255.255.250");
      uint32_t ip = tbsys::CNetUtil::getAddr("172.24.80.0");
      uint32_t ip2 = tbsys::CNetUtil::getAddr("172.24.80.1");
      TBSYS_LOG(DEBUG, "addr 255.255.255.250 === > %u", mask);
      TBSYS_LOG(DEBUG, "addr 255.255.255.250 === > %u", mask2);
      TBSYS_LOG(DEBUG, "addr 172.24.80.1 === > %u", ip & mask);
      TBSYS_LOG(DEBUG, "addr 172.24.80.2 === > %u", ip2 & mask);
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
