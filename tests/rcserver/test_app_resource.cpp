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
#include "app_resource.h"

using namespace tfs::rcserver;

class AppResourceTest: public ::testing::Test
{
  public:
    AppResourceTest()
    {
    }
    ~AppResourceTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(AppResourceTest, testget)
{
  TestDatabaseHelper data_helper;
  AppResource tester(data_helper);
  EXPECT_EQ(TFS_SUCCESS, tester.load());
  AppInfo out;
  EXPECT_EQ(TFS_SUCCESS, tester.get_app_info(2, out));
  EXPECT_EQ(2, out.id_);
  EXPECT_EQ(20, out.quto_);
  EXPECT_EQ(1, out.cluster_group_id_);
  EXPECT_EQ(20, out.report_interval_);
  EXPECT_EQ(1, out.need_duplicate_);
  EXPECT_EQ(100, out.modify_time_);

  EXPECT_EQ(TFS_SUCCESS, tester.get_app_info(1, out));
  EXPECT_EQ(1, out.id_);
  EXPECT_EQ(10, out.quto_);
  EXPECT_EQ(1, out.cluster_group_id_);
  EXPECT_EQ(10, out.report_interval_);
  EXPECT_EQ(0, out.need_duplicate_);
  EXPECT_EQ(50, out.modify_time_);

}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
