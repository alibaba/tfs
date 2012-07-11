/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_path.cpp 158 2011-02-24 02:11:27Z zongdai@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "new_client/tfs_meta_client_api_impl.h"

using namespace tfs::client;
using namespace tfs::common;

class PathTest: public ::testing::Test
{
  public:

  public:
    PathTest()
    {
    }
    ~PathTest()
    {
    }
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }

};

TEST_F(PathTest, test_get_parent_dir)
{
  char dir_path[512];
  sprintf(dir_path, "/test");
  char *parent_dir_path = NULL;
  int ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/", parent_dir_path);

  sprintf(dir_path, "/test/");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/", parent_dir_path);

  sprintf(dir_path, "/test/sub");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/test/", parent_dir_path);

  sprintf(dir_path, "/test/sub/");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/test/", parent_dir_path);

  sprintf(dir_path, "/test/sub//");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/test/", parent_dir_path);

  sprintf(dir_path, "/test//sub");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ("/test//", parent_dir_path);

  sprintf(dir_path, "/");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ(NULL, parent_dir_path);

  sprintf(dir_path, "//");
  ret = NameMetaClientImpl::get_parent_dir(dir_path, parent_dir_path);
  ASSERT_EQ(TFS_SUCCESS, ret);
  ASSERT_STREQ(NULL, parent_dir_path);

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
