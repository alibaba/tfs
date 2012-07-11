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
#include "meta_cache_info.h"
#include "meta_cache_helper.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;

class MetaCacheHelperTest: public ::testing::Test
{
  public:
    MetaCacheHelperTest()
    {
    }
    ~MetaCacheHelperTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};
TEST_F(MetaCacheHelperTest, insert_dir)
{
  {
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  CacheDirMetaNode t1, t2, t3, t4;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  InfoArray<CacheDirMetaNode> t1_info;
  t1.child_dir_infos_ = &t1_info;
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t2));
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t3));
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t4));
  InfoArray<CacheDirMetaNode>* dir_infos = MetaCacheHelper::get_sub_dirs_array_info(&t1);
  EXPECT_EQ(3, dir_infos->get_size());
  }
  printf("used size %ld\n",MemHelper::get_used_size());
}
TEST_F(MetaCacheHelperTest, find_dir)
{
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  CacheDirMetaNode t1, t2, t3, t4;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  InfoArray<CacheDirMetaNode> t1_info;
  t1.child_dir_infos_ = &t1_info;
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t2));
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t3));
  EXPECT_EQ(TFS_SUCCESS, MetaCacheHelper::insert_dir(&t1, &t4));
  InfoArray<CacheDirMetaNode>* dir_infos = MetaCacheHelper::get_sub_dirs_array_info(&t1);
  EXPECT_EQ(3, dir_infos->get_size());

  CacheDirMetaNode* ret = NULL;
  MetaCacheHelper::find_dir(&t1, name2, ret);
  EXPECT_TRUE(t2 == *ret);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
