/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei
 *      - initial release
 *
 */
#include <set>
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include "common/error_msg.h"
#include "lru.h"

using namespace std;
using namespace tbsys;
using namespace tfs::common;
using namespace tfs::namemetaserver;

namespace tfs
{
  namespace namemetaserver
  {
    class LruTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("error");
        }
        static void TearDownTestCase(){}
        LruTest(){}
        virtual ~LruTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
        static const int32_t SERVER_COUNT;
    };

    TEST_F(LruTest, insert)
    {
      std::vector<CacheRootNode*> rs;
      Lru lru;
      AppIdUid key;
      key.app_id_ = 1;
      key.uid_ = 2;
      CacheRootNode* node = new CacheRootNode();
      node->dir_meta_= 0;
      int32_t iret = lru.insert(key, node);
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(0, node->dir_meta_);
      iret = lru.insert(key, new CacheRootNode());
      EXPECT_EQ(EXIT_LRU_VALUE_EXIST, iret);
      lru.gc(rs);
      std::vector<CacheRootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, get)
    {
      std::vector<CacheRootNode*> rs;
      Lru lru;
      AppIdUid key;
      key.app_id_ = 1;
      key.uid_ = 2;
      CacheRootNode* node = new CacheRootNode();
      node->dir_meta_ = 0;
      int32_t iret = lru.insert(key, node);
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(0, node->dir_meta_);
      CacheRootNode* inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(1, inode->ref_count_);
      lru.gc(rs);
      std::vector<CacheRootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, put)
    {
      std::vector<CacheRootNode*> rs;
      Lru lru;
      AppIdUid key;
      key.app_id_ = 1;
      key.uid_ = 2;
      CacheRootNode* node = new CacheRootNode();
      node->dir_meta_= 0;
      int32_t iret = lru.insert(key, node);
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(0, node->dir_meta_);
      CacheRootNode* inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(1, inode->ref_count_);
      lru.put(inode);
      inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(0, inode->ref_count_);
      lru.gc(rs);
      std::vector<CacheRootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, gc)
    {
      AppIdUid key;
      int32_t iret = TFS_SUCCESS;
      std::vector<CacheRootNode*> rs;
      Lru lru;
      int32_t MAX_COUNT = 100000;
      for (int32_t i = 0; i < MAX_COUNT; ++i)
      {
        key.app_id_ = i;
        key.uid_ = i + MAX_COUNT;
        CacheRootNode *rn = new CacheRootNode();
        rn->dir_meta_ = 0;
        rn->key_.app_id_ = i;
        rn->key_.uid_ = i + MAX_COUNT;
        iret = lru.insert(key, rn);
        EXPECT_EQ(TFS_SUCCESS, iret);
        if (i <= MAX_COUNT /2) {  // these are gc possible
          lru.get(key);
          lru.put(rn);
        }
        else {
          lru.get(key);
        }
      }
      double ratio = 0.2;
      uint64_t gc_count = static_cast<uint64_t>(MAX_COUNT * ratio) + 1;
      TBSYS_LOG(INFO, "gc_count %ld", gc_count);
      BaseStrategy bs(lru);
      iret = lru.gc(ratio, &bs, rs);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(gc_count, rs.size());
      EXPECT_EQ(MAX_COUNT - gc_count, lru.list_.size());
      std::vector<CacheRootNode*>::iterator iter = rs.begin();
      int32_t i = 0;
      for(; iter != rs.end();++iter)
      {
        EXPECT_EQ((*iter)->key_.app_id_, i++);  // check the gc sequence, should start from 0
        tbsys::gDelete((*iter));
      }
    }
  }/** rootserver **/
}/** tfs **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
