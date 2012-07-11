/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *    daoan(daoan@taobao.com)
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "test_database_helper.h"
#include "resource_manager.h"

using namespace tfs::rcserver;

class ResourceManagerTest: public ::testing::Test
{
  public:
    ResourceManagerTest()
    {
    }
    ~ResourceManagerTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(ResourceManagerTest, login)
{
  TestResourceManager tester(new tbutil::Timer());
  tester.initialize();
  int32_t app_id = 0;
  BaseInfo base_info;
  EXPECT_EQ(TFS_SUCCESS, tester.login("appkey2", app_id, base_info));
  EXPECT_EQ(2, app_id);
  EXPECT_EQ(20, base_info.report_interval_);
  EXPECT_EQ(1, base_info.rc_server_infos_.size());
  EXPECT_EQ(2, base_info.cluster_infos_.size());

  EXPECT_EQ(1, base_info.cluster_infos_[0].need_duplicate_);
  EXPECT_EQ(1, base_info.cluster_infos_[1].need_duplicate_);

  EXPECT_EQ(1, base_info.cluster_infos_[0].cluster_data_[0].access_type_);
  EXPECT_EQ(2, base_info.cluster_infos_[1].cluster_data_[0].access_type_);

}

TEST_F(ResourceManagerTest, check_update_info)
{
  TestResourceManager tester(new tbutil::Timer());
  tester.initialize();
  bool update_flag = false;
  BaseInfo base_info;
  tester.check_update_info(1, 10, update_flag, base_info);
  EXPECT_EQ(true, update_flag);
  tester.check_update_info(1, 100, update_flag, base_info);
  EXPECT_EQ(false, update_flag);

}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
