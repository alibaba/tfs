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
#include "ip_replace_helper.h"
using namespace tfs::common;
using namespace tfs::rcserver;

class IpReplacerTest: public ::testing::Test
{
  public:
    IpReplacerTest()
    {
    }
    ~IpReplacerTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(IpReplacerTest, transfer)
{
  IpReplaceHelper::IpTransferItem item1;
  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.10.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  IpReplaceHelper::IpTransferItem item2;
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.3.5"));
  EXPECT_EQ(2, item1.transfer(item2));
  EXPECT_EQ("20.2.3.5", item2.get_dest_ip());

  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.1.3.5"));
  EXPECT_EQ(-1, item1.transfer(item2));

  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.10.3.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.3.5"));
  EXPECT_EQ(3, item1.transfer(item2));
  EXPECT_EQ("20.2.3.5", item2.get_dest_ip());

  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.10.3.5"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.3.5"));
  EXPECT_EQ(4, item1.transfer(item2));
  EXPECT_EQ("20.2.3.5", item2.get_dest_ip());

  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("*.*.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.3.5"));
  EXPECT_EQ(0, item1.transfer(item2));
  EXPECT_EQ("10.10.3.5", item2.get_dest_ip());

  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.*.5.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.5.5"));
  EXPECT_EQ(1, item1.transfer(item2));
  EXPECT_EQ("20.10.5.5", item2.get_dest_ip());

  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.*.5.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.2.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item2.set_source_ip("10.10.3.5"));
  EXPECT_EQ(-1, item1.transfer(item2));
}
TEST_F(IpReplacerTest, replace_ip)
{
  IpReplaceHelper::VIpTransferItem v;
  IpReplaceHelper::IpTransferItem item1;
  IpReplaceHelper::IpTransferItem item2;
  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.10.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("30.2.*.*"));
  v.push_back(item1);
  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("11.*.*.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("20.*.*.*"));
  v.push_back(item1);
  EXPECT_EQ(TFS_SUCCESS, item1.set_source_ip("10.5.3.*"));
  EXPECT_EQ(TFS_SUCCESS, item1.set_dest_ip("40.7.6.*"));
  v.push_back(item1);

  std::string dest;
  EXPECT_EQ(TFS_SUCCESS, IpReplaceHelper::replace_ip(v, "10.10.1.2", dest));
  EXPECT_EQ("30.2.1.2", dest);
  EXPECT_EQ(TFS_SUCCESS, IpReplaceHelper::replace_ip(v, "11.11.1.2", dest));
  EXPECT_EQ("20.11.1.2", dest);
  EXPECT_EQ(TFS_ERROR, IpReplaceHelper::replace_ip(v, "10.5.1.2", dest));
  EXPECT_EQ(TFS_SUCCESS, IpReplaceHelper::replace_ip(v, "10.5.3.2", dest));
  EXPECT_EQ("40.7.6.2", dest);

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
