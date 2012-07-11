/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_server_manager.cpp 5 2011-03-26 17:55:56Z
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
#include "server_manager.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    class ServerManagerTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {

        }
        ServerManagerTest():
          layout_manager_(ns_),
          server_manager_(layout_manager_)
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
        }

        ~ServerManagerTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {
          server_manager_.clear_();
        }
      protected:
        NameServer ns_;
        LayoutManager layout_manager_;
        ServerManager server_manager_;
    };

    TEST_F(ServerManagerTest, add_remove_get)
    {
      srand(time(NULL));
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
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

      ServerCollect query(info.id_);
      ServerCollect* server = server_manager_.get(info.id_);
      EXPECT_TRUE(NULL == server);

      bool isnew = false;
      EXPECT_EQ(TFS_SUCCESS, server_manager_.add(info, now, isnew));
      EXPECT_TRUE(isnew);
      server = server_manager_.get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(1, server_manager_.servers_.size());

      //update
      isnew = false;
      info.use_capacity_ = 0xffff;
      EXPECT_EQ(TFS_SUCCESS, server_manager_.add(info, now, isnew));
      EXPECT_FALSE(isnew);
      server = server_manager_.get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(info.use_capacity_, server->use_capacity());
      EXPECT_EQ(1, server_manager_.servers_.size());

      //down
      EXPECT_EQ(TFS_SUCCESS, server_manager_.remove(info.id_, now));
      server = server_manager_.get(info.id_);
      EXPECT_TRUE(NULL == server);
      EXPECT_EQ(0, server_manager_.servers_.size());
      EXPECT_EQ(1, server_manager_.dead_servers_.size());
      isnew = false;
      info.use_capacity_ = 0xffffa;
      EXPECT_EQ(TFS_SUCCESS, server_manager_.add(info, now, isnew));
      EXPECT_TRUE(isnew);
      server = server_manager_.get(info.id_);
      EXPECT_TRUE(NULL != server);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(info.use_capacity_, server->use_capacity());
      EXPECT_EQ(1, server_manager_.servers_.size());
      EXPECT_EQ(0, server_manager_.dead_servers_.size());
    }

    TEST_F(ServerManagerTest, get_range_servers_)
    {
      bool isnew = false;
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      ServerCollect* server = NULL;
      int32_t COUNT = random() % 10240 + MAX_PROCESS_NUMS;
      for (int32_t i = 0; i < COUNT; i++, isnew=false)
      {
        info.id_++;
        EXPECT_EQ(TFS_SUCCESS, server_manager_.add(info, now, isnew));
        EXPECT_TRUE(isnew);
        server = server_manager_.get(info.id_);
        EXPECT_TRUE(NULL != server && server->id() == info.id_);
      }
      EXPECT_EQ(COUNT, server_manager_.size());

      bool complete = false;
      uint64_t begin = 0;
      int32_t actual = 0;
      const int32_t MAX_SLOT_NUMS = 32;
      ServerCollect* servers[MAX_SLOT_NUMS];
      ArrayHelper<ServerCollect*> helper(MAX_SLOT_NUMS, servers);
      do
      {
        helper.clear();
        complete = server_manager_.get_range_servers_(helper, begin, MAX_SLOT_NUMS);
        if (!helper.empty())
        {
          server = *helper.at(helper.get_array_index() - 1);
          begin = server->id();
          actual += helper.get_array_index();
        }
      }
      while (!complete);
      EXPECT_EQ(info.id_, server->id());
      EXPECT_EQ(COUNT, actual);
    }

    TEST_F(ServerManagerTest, pop_from_dead_queue)
    {
      bool isnew = false;
      time_t now = Func::get_monotonic_time();
      uint64_t base_id = 0xfffffff0;
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = base_id;
      ServerCollect* server = NULL;
      int32_t i = 0;
      int32_t COUNT = random() % 16 + 8;
      for (i = 0; i < COUNT; info.id_++, i++, isnew=false)
      {
        EXPECT_EQ(TFS_SUCCESS, server_manager_.add(info, now, isnew));
        EXPECT_TRUE(isnew);
        server = server_manager_.get(info.id_);
        EXPECT_TRUE(NULL != server && server->id() == info.id_);
      }
      EXPECT_EQ(COUNT, server_manager_.size());
      int32_t REMOVE_COUNT = random() % 6  + 4;
      for (i = 0; i < REMOVE_COUNT; i++)
      {
        server = server_manager_.get((base_id + i));
        EXPECT_EQ(TFS_SUCCESS, server_manager_.remove((base_id + i), now));
      }
      EXPECT_EQ(COUNT - REMOVE_COUNT, server_manager_.servers_.size());
      EXPECT_EQ(REMOVE_COUNT, server_manager_.dead_servers_.size());

      int32_t actual = 0;
      now += SYSPARAM_NAMESERVER.replicate_wait_time_ + 1;
      TBSYS_LOG(DEBUG, "remove count : %d", REMOVE_COUNT);
      while (NULL != server_manager_.pop_from_dead_queue(now))
      {
        actual++;
      }
      EXPECT_EQ(REMOVE_COUNT, actual);
      EXPECT_EQ(0, server_manager_.dead_servers_.size());
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
