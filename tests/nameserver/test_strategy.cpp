/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_strategy.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   chuyu 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <set>
#include "strategy.h"

using namespace tfs::nameserver;
using namespace tfs::common;
class StrategyTest:public::testing::Test
{
   protected:
    static void SetUpTestCase()
    {

      TBSYS_LOGGER.setLogLevel("debug");
    }

    static void TearDownTestCase()
    {

    }
  public:
    StrategyTest()
    {
      SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 80;
      SYSPARAM_NAMESERVER.max_write_file_count_ = 5;
    }
    ~StrategyTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
  protected:
  typedef std::set<uint32_t> BLOCK_SET;
};

TEST_F(StrategyTest, check_average)
{
  // alive size = 0
  EXPECT_EQ(check_average(45, 180, 78, 160, 0), false);
  // current_load < average_load * 2
  EXPECT_EQ(check_average(45, 180, 78, 160, 3), true);
  // total_load = 0 && current_use < average_use * 2
  EXPECT_EQ(check_average(0, 0, 78, 160, 4), true);
  // total_use = 0
  EXPECT_EQ(check_average(45, 180, 0, 0, 4), true);
}

TEST_F(StrategyTest, base_strategy_check)
{
  NsGlobalInfo global_info(45, 180, 80, 0, 200, 110, 3);
  BaseStrategy base_strategy(1, global_info);

  ServerCollect server_collect;
  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));

  // current_load < average_load * 2
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  ds_stat_info.current_load_ = 45;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  EXPECT_EQ(base_strategy.check(&server_collect), true);
  // total_load = 0 && current use < average_use * 2
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  ds_stat_info.current_load_ = 60;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  EXPECT_EQ(base_strategy.check(&server_collect), true);
  // server is dead
  server_collect.dead();
  EXPECT_EQ(base_strategy.check(&server_collect), false);
}

TEST_F(StrategyTest, base_strategy_normalize)
{
  // max_block_count = 0  max_load < 10
  NsGlobalInfo global_info(45, 180, 80, 0, 7, 0, 3);
  BaseStrategy base_strategy(140, global_info);

  ServerCollect server_collect;
  DataServerStatInfo ds_stat_info;
  server_collect.register_(1001, true, true);
  server_collect.register_(1002, true, true);
  server_collect.set_elect_seq(70);

  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  ds_stat_info.current_load_ = 60;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);

  base_strategy.normalize(&server_collect);
  EXPECT_EQ(base_strategy.primary_writable_block_count_, 40);
  EXPECT_EQ(base_strategy.seqno_, 50);
  EXPECT_EQ(base_strategy.use_, 95);
  EXPECT_EQ(base_strategy.load_, 2);

  // max_block_count != 0 max_load > 10
  NsGlobalInfo global_info_other(45, 180, 80, 0, 100, 100, 3);
  BaseStrategy base_strategy_other(140, global_info_other);

  server_collect.set_elect_seq(70);

  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  ds_stat_info.current_load_ = 60;
  ds_stat_info.block_count_ = 5;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);

  base_strategy_other.normalize(&server_collect);
  EXPECT_EQ(base_strategy_other.primary_writable_block_count_, 40);
  EXPECT_EQ(base_strategy_other.seqno_, 50);
  EXPECT_EQ(base_strategy_other.use_, 5);
  EXPECT_EQ(base_strategy_other.load_, 60);
}
TEST_F(StrategyTest, write_strategy_calc)
{
  NsGlobalInfo global_info(45, 180, 80, 0, 7, 0, 3);
  WriteStrategy write_strategy(140, global_info);
  ServerCollect server_collect;
  DataServerStatInfo ds_stat_info;

  // is full
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  EXPECT_EQ(write_strategy.calc(&server_collect), 0);

  // return elect_seq
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 75;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  server_collect.set_elect_seq(70);
  EXPECT_EQ(write_strategy.calc(&server_collect), 70);

  // server dead
  server_collect.dead();
  EXPECT_EQ(write_strategy.calc(&server_collect), 0);
}

TEST_F(StrategyTest, replicate_dest_strategy_calc)
{
  NsGlobalInfo global_info(45, 180, 80, 0, 7, 0, 3);
  ReplicateDestStrategy dest_strategy(140, global_info);
  ServerCollect server_collect;
  DataServerStatInfo ds_stat_info;

  // is full
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  EXPECT_EQ(dest_strategy.calc(&server_collect), 0);

  // return elect_seq
  server_collect.register_(1001, true, true);
  server_collect.register_(1002, true, true);
  server_collect.set_elect_seq(70);

  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 75;
  ds_stat_info.current_load_ = 60;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);
  EXPECT_EQ(dest_strategy.calc(&server_collect), 5135);

  // server dead
  server_collect.dead();
  EXPECT_EQ(dest_strategy.calc(&server_collect), 0);
}

TEST_F(StrategyTest, replicate_source_strategy_calc)
{
  NsGlobalInfo global_info(45, 180, 80, 0, 100, 100, 3);
  ReplicateSourceStrategy source_strategy(140, global_info);
  ServerCollect server_collect;
  DataServerStatInfo ds_stat_info;

  server_collect.register_(1001, true, true);
  server_collect.register_(1002, true, true);
  server_collect.set_elect_seq(70);

  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 95;
  ds_stat_info.current_load_ = 60;
  ds_stat_info.block_count_ = 5;
  server_collect.update_server_information((const DataServerStatInfo)ds_stat_info);

  EXPECT_EQ(source_strategy.calc(&server_collect), 60);
}

TEST_F(LayoutManagerTest, cacl_max_capacity_ds)
{
  std::vector<ServerCollect*> ds_list;  
  std::vector<ServerCollect*> result;

  NsGlobalInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;
  bool is_new = false;
  time_t now = time(NULL);

  int32_t i = 0;

  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  ServerCollect* server1 = get_server(ds_info.id_);
  ds_info.id_++;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  ServerCollect* server2 = get_server(ds_info.id_);

  std::vector<BlockCollect*> tmp;
  for (i = 0; i < 10; ++i)
  {
    BlockCollect* block = new BlockCollect(i, time(NULL));
    tmp.push_back(block);
  }

  int count = 0;
  std::vector<BlockCollect*>::iterator iter = tmp.begin();
  for (; iter != tmp.end() && count < 4; ++iter, ++count)
  {
    server1->add((*iter), false);
  }

  iter = tmp.begin();
  for (count = 0; iter != tmp.end() && count < 8; ++iter, ++count)
  {
    server2->add((*iter), false);
  }
  ds_list.push_back(server1);
  ds_list.push_back(server2);

  EXPECT_EQ(1, cacl_max_capacity_ds(ds_list, 1, result));
  TBSYS_LOG(DEBUG, "***********max capacity ds(%u)********", result.at(0)->id());
  EXPECT_EQ(8, result.at(0)->block_count());
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
