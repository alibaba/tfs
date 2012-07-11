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

class MetaCacheTest: public ::testing::Test
{
  public:
    MetaCacheTest()
    {
    }
    ~MetaCacheTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};
TEST_F(MetaCacheTest, CacheDirMetaNode_compare)
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
  EXPECT_TRUE(t1 < t2);
  EXPECT_TRUE(t1 < t3);
  EXPECT_TRUE(t2 < t3);
  EXPECT_TRUE(t4 < t3);
  EXPECT_TRUE(t4 != t3);
  t3.name_ = name4;
  EXPECT_FALSE(t4 < t3);
  EXPECT_FALSE(t3 < t4);
  EXPECT_TRUE(t4 == t3);
}
TEST_F(MetaCacheTest, CacheFileMetaNode_compare)
{
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  CacheFileMetaNode t1, t2, t3, t4;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  EXPECT_TRUE(t1 < t2);
  EXPECT_FALSE(t2 < t1);
  EXPECT_TRUE(t1 < t3);
  EXPECT_FALSE(t3 < t1);
  EXPECT_TRUE(t2 < t3);
  EXPECT_TRUE(t4 < t3);
  t3.name_ = name4;
  EXPECT_FALSE(t4 < t3);
  EXPECT_FALSE(t3 < t4);
}
TEST_F(MetaCacheTest, CacheDirMetaNode_insert_dir)
{
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  char name5[100];
  char name6[100];
  CacheDirMetaNode t1, t2, t3, t4, t5, t6;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  sprintf(name5, "%ckk", 2);
  sprintf(name6, "%cyyy", 3);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  t5.name_ = name5;
  t6.name_ = name6;
  t1.child_dir_infos_ = new InfoArray<CacheDirMetaNode>();
  InfoArray<CacheDirMetaNode>* p = ((InfoArray<CacheDirMetaNode>*)t1.child_dir_infos_);
  EXPECT_TRUE(p->insert(&t4));
  EXPECT_TRUE(p->insert(&t5));
  EXPECT_TRUE(p->insert(&t2));
  EXPECT_TRUE(p->insert(&t3));
  EXPECT_TRUE(p->insert(&t6));
  EXPECT_FALSE(p->insert(&t6));
  EXPECT_FALSE(p->insert(&t4));

  EXPECT_EQ(5, p->get_size());
  EXPECT_EQ(8, p->get_capacity());
  CacheDirMetaNode** begin = p->get_begin();
  EXPECT_TRUE(**begin == t4);
  begin++;
  EXPECT_TRUE(**begin == t2);
  begin++;
  EXPECT_TRUE(**begin == t3);
  begin++;
  EXPECT_TRUE(**begin == t5);
  begin++;
  EXPECT_TRUE(**begin == t6);
  delete p;
}
TEST_F(MetaCacheTest, CacheDirMetaNode_find_dir)
{
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  char name5[100];
  char name6[100];
  CacheDirMetaNode t1, t2, t3, t4, t5, t6;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  sprintf(name5, "%ckk", 2);
  sprintf(name6, "%cyyy", 3);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  t5.name_ = name5;
  t6.name_ = name6;
  t1.child_dir_infos_ = new InfoArray<CacheDirMetaNode>();
  InfoArray<CacheDirMetaNode>* p = ((InfoArray<CacheDirMetaNode>*)t1.child_dir_infos_);
  EXPECT_TRUE(p->insert(&t4));
  EXPECT_TRUE(p->insert(&t5));
  EXPECT_TRUE(p->insert(&t2));
  EXPECT_TRUE(p->insert(&t3));
  EXPECT_TRUE(p->insert(&t6));

  EXPECT_EQ(5, p->get_size());
  EXPECT_EQ(8, p->get_capacity());
  CacheDirMetaNode** begin = p->get_begin();
  CacheDirMetaNode** tmp;
  EXPECT_TRUE(**begin == t4);
  tmp = p->find(name4);
  EXPECT_EQ(tmp, begin);
  begin++;
  EXPECT_TRUE(**begin == t2);
  tmp = p->find(name2);
  EXPECT_EQ(tmp, begin);
  begin++;
  EXPECT_TRUE(**begin == t3);
  tmp = p->find(name3);
  EXPECT_EQ(tmp, begin);
  begin++;
  EXPECT_TRUE(**begin == t5);
  tmp = p->find(name5);
  EXPECT_EQ(tmp, begin);
  begin++;
  EXPECT_TRUE(**begin == t6);
  tmp = p->find(name6);
  EXPECT_EQ(tmp, begin);
  delete p;
}
TEST_F(MetaCacheTest, CacheDirMetaNode_remove_dir)
{
  char name1[100];
  char name2[100];
  char name3[100];
  char name4[100];
  char name5[100];
  char name6[100];
  CacheDirMetaNode t1, t2, t3, t4, t5, t6;
  sprintf(name1, "%c12345", 5);
  sprintf(name2, "%c12346", 5);
  sprintf(name3, "%caa", 2);
  sprintf(name4, "%c", 0);
  sprintf(name5, "%ckk", 2);
  sprintf(name6, "%cyyy", 3);
  t1.name_ = name1;
  t2.name_ = name2;
  t3.name_ = name3;
  t4.name_ = name4;
  t5.name_ = name5;
  t6.name_ = name6;
  t1.child_dir_infos_ = new InfoArray<CacheDirMetaNode>();
  InfoArray<CacheDirMetaNode>* p = ((InfoArray<CacheDirMetaNode>*)t1.child_dir_infos_);
  EXPECT_TRUE(p->insert(&t4));
  EXPECT_TRUE(p->insert(&t5));
  EXPECT_TRUE(p->insert(&t2));
  EXPECT_TRUE(p->insert(&t3));
  EXPECT_TRUE(p->insert(&t6));

  EXPECT_EQ(5, p->get_size());
  EXPECT_EQ(8, p->get_capacity());
  CacheDirMetaNode** begin = p->get_begin();
  CacheDirMetaNode** tmp;

  p->remove(name3);
  EXPECT_EQ(4, p->get_size());
  EXPECT_EQ(8, p->get_capacity());
  tmp = p->find(name3);
  EXPECT_TRUE(NULL == tmp);

  p->remove(name5);
  EXPECT_EQ(3, p->get_size());
  EXPECT_EQ(8, p->get_capacity());
  tmp = p->find(name5);
  EXPECT_EQ(NULL, tmp);
  delete p;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
