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
#include <stdint.h>
#include <set>
#include <Time.h>
#include <gtest/gtest.h>
#include "common/lock.h"
#include "meta_bucket_manager.h"
#include "parameter.h"

using namespace std;
using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace namemetaserver
  {
    class BucketManagerTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
           SYSPARAM_RTSERVER.mts_rts_lease_expired_time_ = 2;
        }
        static void TearDownTestCase(){}
        BucketManagerTest(){}
        virtual ~BucketManagerTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
    };

    TEST_F(BucketManagerTest, update_table)
    {
      int64_t tables[common::MAX_BUCKET_ITEM_DEFAULT];
      for (int32_t i = 1; i <= common::MAX_BUCKET_ITEM_DEFAULT; ++i)
        tables[i - 1] = i;
      BucketManager bucket;
      int32_t iret = bucket.update_table(NULL, MAX_BUCKET_DATA_LENGTH, 0, 1);
      EXPECT_EQ(TFS_ERROR, iret);
      iret = bucket.update_table((const char*)tables, 0L, 0L, 1);
      EXPECT_EQ(TFS_ERROR, iret);
      iret = bucket.update_table((const char*)tables, MAX_BUCKET_DATA_LENGTH + 1, 0, 1);
      EXPECT_EQ(TFS_ERROR, iret);
      iret = bucket.update_table((const char*)tables, MAX_BUCKET_DATA_LENGTH, INVALID_TABLE_VERSION - 1, 1);
      EXPECT_EQ(EXIT_TABLE_VERSION_ERROR, iret);
      iret = bucket.update_table((const char*)tables, MAX_BUCKET_DATA_LENGTH, 0, 1);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(0, bucket.update_table_.version_);
    }

    TEST_F(BucketManagerTest, update_new_table_status)
    {
      int64_t tables[common::MAX_BUCKET_ITEM_DEFAULT];
      for (int32_t i = 1; i <= common::MAX_BUCKET_ITEM_DEFAULT; ++i)
        tables[i - 1] = i;
      BucketManager bucket;
      int32_t iret = bucket.update_table((const char*)tables, MAX_BUCKET_DATA_LENGTH, 0, 1);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(0, bucket.update_table_.version_);
      iret = bucket.update_new_table_status(1);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_TRUE(bucket.active_table_.empty());
      Bucket::TABLES_CONST_ITERATOR iter = bucket.update_table_.tables_.begin();
      for (; iter != bucket.update_table_.tables_.end(); ++iter)
      {
        EXPECT_EQ(BUCKET_STATUS_NONE, (*iter).status_);
      }
    }

    TEST_F(BucketManagerTest, switch_table)
    {
       int64_t tables[common::MAX_BUCKET_ITEM_DEFAULT];
      for (int32_t i = 1; i <= common::MAX_BUCKET_ITEM_DEFAULT; ++i)
        tables[i - 1] = i;
      BucketManager bucket;
      int32_t iret = bucket.update_table((const char*)tables, MAX_BUCKET_DATA_LENGTH, 0, 1);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(0, bucket.update_table_.version_);
      iret = bucket.update_new_table_status(1);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_TRUE(bucket.active_table_.empty());
      Bucket::TABLES_CONST_ITERATOR iter = bucket.update_table_.tables_.begin();
      for (; iter != bucket.update_table_.tables_.end(); ++iter)
      {
        EXPECT_EQ(BUCKET_STATUS_NONE, (*iter).status_);
      }

      EXPECT_EQ(0, bucket.update_table_.version_);
      std::set<int64_t> tt;
      iret = bucket.switch_table(tt, 0, -1);
      EXPECT_EQ(EXIT_PARAMETER_ERROR, iret);
      iret = bucket.switch_table(tt, 1, -1);
      EXPECT_EQ(EXIT_TABLE_VERSION_ERROR, iret);

      iret = bucket.switch_table(tt, 1, 0);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(INVALID_TABLE_VERSION, bucket.update_table_.version_);
      EXPECT_EQ(0, bucket.active_table_.version_);
      EXPECT_TRUE(bucket.update_table_.empty());
      iter = bucket.active_table_.tables_.begin();
      for (; iter != bucket.active_table_.tables_.end(); ++iter)
      {
        if ((*iter).server_ == 1)
          EXPECT_EQ(BUCKET_STATUS_RW, (*iter).status_);
        else
          EXPECT_EQ(BUCKET_STATUS_NONE, (*iter).status_);
      }
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
