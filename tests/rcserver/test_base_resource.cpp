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
#include "base_resource.h"

using namespace tfs::rcserver;

class BaseResourceTest: public ::testing::Test
{
  public:
    BaseResourceTest()
    {
    }
    ~BaseResourceTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(BaseResourceTest, get_resource_servers)
{
  TestDatabaseHelper data_helper;
  BaseResource tester(data_helper);
  EXPECT_EQ(TFS_SUCCESS, tester.load());
  std::vector<uint64_t> resource_servers;
  EXPECT_EQ(TFS_SUCCESS, tester.get_resource_servers(resource_servers));
  EXPECT_EQ(1, resource_servers.size());
  EXPECT_EQ(tbsys::CNetUtil::strToAddr("10.232.35.41:2123",0), *(resource_servers.begin()));

}
TEST_F(BaseResourceTest, get_meta_root_servers)
{
  TestDatabaseHelper data_helper;
  BaseResource tester(data_helper);
  EXPECT_EQ(TFS_SUCCESS, tester.load());
  int64_t root_server = 0;
  EXPECT_EQ(TFS_SUCCESS, tester.get_meta_root_server(5, root_server));
  EXPECT_EQ(tbsys::CNetUtil::strToAddr("10.232.35.42:2123",0), root_server);
  root_server = 0;
  EXPECT_EQ(TFS_SUCCESS, tester.get_meta_root_server(6, root_server));
  EXPECT_EQ(tbsys::CNetUtil::strToAddr("10.232.35.40:2123",0), root_server);


}

TEST_F(BaseResourceTest, get_cluster_infos)
{
  TestDatabaseHelper data_helper;
  BaseResource tester(data_helper);
  EXPECT_EQ(TFS_SUCCESS, tester.load());
  std::vector<ClusterRackData> cluster_rack_datas;
  std::vector<ClusterData> cluster_datas_for_update;
  EXPECT_EQ(TFS_SUCCESS, tester.get_cluster_infos(1, cluster_rack_datas, cluster_datas_for_update));
  EXPECT_EQ(2, cluster_rack_datas.size());

  EXPECT_STREQ("2.1.1.1:1010", cluster_rack_datas[1].dupliate_server_addr_.c_str());

  EXPECT_EQ(2, cluster_rack_datas[0].cluster_data_.size());
  EXPECT_EQ(3, cluster_rack_datas[1].cluster_data_.size());

  EXPECT_STREQ("T1M", cluster_rack_datas[0].cluster_data_[0].cluster_id_.c_str());
  EXPECT_STREQ("T5M", cluster_rack_datas[0].cluster_data_[1].cluster_id_.c_str());

  EXPECT_EQ(1, cluster_rack_datas[0].cluster_data_[0].cluster_stat_);
  EXPECT_EQ(2, cluster_rack_datas[0].cluster_data_[1].cluster_stat_);

  EXPECT_EQ(1, cluster_rack_datas[0].cluster_data_[0].access_type_);
  EXPECT_EQ(1, cluster_rack_datas[0].cluster_data_[1].access_type_);
}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
