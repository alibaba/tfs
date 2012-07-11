/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_collect.cpp 5 2011-03-20 08:55:56Z
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
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {

    class BlockCollectTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        BlockCollectTest()
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
        }

        ~BlockCollectTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(BlockCollectTest, add)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;
      EXPECT_FALSE(block.add(writable, master, NULL));
      EXPECT_FALSE(master);
      EXPECT_FALSE(writable);

      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_TRUE(writable);

      ServerCollect** result = block.get_(&server);
      EXPECT_TRUE(&server == *result);
      EXPECT_TRUE(server.id() == (*result)->id());
      EXPECT_TRUE(block.exist(&server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      //已经存在了
      EXPECT_FALSE(block.add(writable, master, &server));
      result = block.get_(&server);
      EXPECT_TRUE(&server == *result);
      EXPECT_TRUE(server.id() == (*result)->id());
      EXPECT_TRUE(block.exist(&server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      info.id_++;
      ServerCollect other(info, now);
      EXPECT_TRUE(block.add(writable, master, &other));
      EXPECT_EQ(2, block.get_servers_size());
      result = block.get_(&other);
      EXPECT_TRUE(&other== *result);
      EXPECT_TRUE(other.id() == (*result)->id());
      EXPECT_TRUE(block.exist(&other));
      EXPECT_EQ(now, block.last_update_time_);

      EXPECT_FALSE(block.add(writable, master, &other));
    }

    TEST_F(BlockCollectTest, remove)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;
      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_TRUE(writable);
      ServerCollect** result = block.get_(&server);
      EXPECT_TRUE(&server == *result);
      EXPECT_TRUE(server.id() == (*result)->id());
      EXPECT_TRUE(block.exist(&server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      EXPECT_TRUE(block.remove(&server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(&server));

      EXPECT_TRUE(block.remove(&server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(&server));
    }

    TEST_F(BlockCollectTest, exist)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;
      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_TRUE(writable);
      ServerCollect** result = block.get_(&server);
      EXPECT_TRUE(&server == *result);
      EXPECT_TRUE(server.id() == (*result)->id());
      EXPECT_TRUE(block.exist(&server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      EXPECT_TRUE(block.remove(&server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(&server));

      EXPECT_TRUE(block.remove(&server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(&server));
    }

    TEST_F(BlockCollectTest, check_version)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;
      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_EQ(1, block.get_servers_size());

      // construct block info
      BlockInfo block_info;
      bool isnew = false;
      bool expire_self;
      NameServer ns;
      LayoutManager manager(ns);
      NsRole role = NS_ROLE_MASTER;

      ServerCollect* removes[SYSPARAM_NAMESERVER.max_replication_];
      ArrayHelper<ServerCollect*> helper1(SYSPARAM_NAMESERVER.max_replication_, removes);

      ServerCollect* other_expired[SYSPARAM_NAMESERVER.max_replication_];
      ArrayHelper<ServerCollect*> helper2(SYSPARAM_NAMESERVER.max_replication_, other_expired);

      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, NULL, role, isnew, block_info, now));

      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, &server, role, isnew, block_info, now));

      info.id_++;
      ServerCollect other(info, now);
      block_info.block_id_ = 100;
      block_info.file_count_ = 10;
      block_info.version_ = 100;
      block_info.size_ = 110;

      //新的版本比nameserver上的版本高，接受新的版本并失效老的版本
      EXPECT_TRUE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_EQ(1, helper1.get_array_index());
      EXPECT_EQ(1, helper2.get_array_index());
      EXPECT_TRUE(block_info.version_== block.version());
      EXPECT_TRUE(block_info.size_ == block.size());

      //当前没任何dataserver在列表，接受当前版本
      block_info.version_ = block_info.version_ - BlockCollect::VERSION_AGREED_MASK - 1;
      helper1.clear();
      helper2.clear();
      EXPECT_TRUE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ == block.version());
      EXPECT_TRUE(block_info.size_ ==  block.size());
      EXPECT_EQ(0, helper1.get_array_index());
      EXPECT_EQ(0, helper2.get_array_index());


      //当前ataserver在列表，失效前版本
      block_info.version_ = block_info.version_ - BlockCollect::VERSION_AGREED_MASK - 1;
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ != block.version());
      EXPECT_EQ(0, helper1.get_array_index());
      EXPECT_EQ(0, helper2.get_array_index());

      block_info.version_ += 2;
      EXPECT_TRUE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ != block.version());//nameserver版本

      block_info.version_ = block_info.version_ + BlockCollect::VERSION_AGREED_MASK  + 1;
      EXPECT_TRUE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ == block.version());//新版本

      block.remove(&server, now);
      block_info.version_ = block_info.version_ + BlockCollect::VERSION_AGREED_MASK  + 1;
      EXPECT_TRUE(block.check_version(manager, helper1, expire_self, helper2, &other, role, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ == block.version());//新版本


      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_TRUE(block.add(writable, master, &other));

      info.id_++;
      ServerCollect other2(info, now);
      block_info.block_id_ = 100;
      block_info.file_count_ = 10;
      block_info.version_ = 100;
      block_info.size_ = 110;

      block_info.file_count_ -= 1;
      helper1.clear();
      helper2.clear();
      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, &other2, role, isnew, block_info, now));
      EXPECT_EQ(0, helper1.get_array_index());
      EXPECT_EQ(0, helper2.get_array_index());

      block_info.file_count_ += 1;
      block_info.size_ -= 1;
      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, &other2, role, isnew, block_info, now));
      EXPECT_EQ(0, helper1.get_array_index());
      EXPECT_EQ(0, helper2.get_array_index());

      expire_self = false;
      block_info.file_count_ += 10;
      TBSYS_LOG(DEBUG, "expire_self: %d", expire_self);
      EXPECT_FALSE(block.check_version(manager, helper1, expire_self, helper2, &other2, role, isnew, block_info, now));
      TBSYS_LOG(DEBUG, "expire_self: %d", expire_self);
      EXPECT_TRUE(expire_self == true);
    }

    TEST_F(BlockCollectTest, check_replicate)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      SYSPARAM_NAMESERVER.max_replication_ = 4;
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;

      EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now));

      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, block.check_replicate(now));

      info.id_++;
      ServerCollect server2(info, now);
      EXPECT_TRUE(block.add(writable, master, &server2));
      EXPECT_EQ(2, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now));

      info.id_++;
      ServerCollect server3(info, now);
      EXPECT_TRUE(block.add(writable, master, &server3));
      TBSYS_LOG(DEBUG, "SIZE = %d", block.get_servers_size());
      EXPECT_EQ(3, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now));

      info.id_++;
      ServerCollect server4(info, now);
      EXPECT_TRUE(block.add(writable, master, &server4));
      EXPECT_EQ(4, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now));

      SYSPARAM_NAMESERVER.max_replication_ = 2;
    }

    TEST_F(BlockCollectTest, check_compact)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;

      ServerCollect server(info, now);
      EXPECT_TRUE(block.add(writable, master, &server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_FALSE(block.check_compact());

      info.id_++;
      ServerCollect server2(info, now);
      EXPECT_TRUE(block.add(writable, master, &server2));
      EXPECT_EQ(2, block.get_servers_size());
      EXPECT_FALSE(block.check_compact());

      BlockInfo block_info;
      block_info.block_id_ = 100;
      block_info.file_count_ = 0;
      block_info.version_ = 100;
      block.update(block_info);

      EXPECT_FALSE(block.check_compact());

      block_info.file_count_ = 10;
      block_info.del_file_count_ = 1;
      block.update(block_info);
      EXPECT_FALSE(block.check_compact());

      block_info.del_file_count_ = 8;
      block_info.size_ = 110;
      block_info.del_size_ = 1;
      block.update(block_info);
      EXPECT_TRUE(block.check_compact());

      block_info.del_file_count_ = 1;
      block_info.size_ = 100;
      block_info.del_size_ = 1;
      block.update(block_info);
      EXPECT_FALSE(block.check_compact());

      block_info.del_size_ = 80;
      block.update(block_info);
      EXPECT_TRUE(block.check_compact());
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
