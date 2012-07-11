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
#include <Time.h>
#include <gtest/gtest.h>
#include "common/lock.h"
#include "common/rts_define.h"
#include "meta_bucket_manager.h"

using namespace std;
using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace namemetaserver 
  {
    class BucketTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase(){}
        BucketTest(){}
        virtual ~BucketTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
    };

    TEST_F(BucketTest, initialize)
    {
      int64_t tables[common::MAX_BUCKET_ITEM_DEFAULT];
      for (int32_t i = 1; i <= common::MAX_BUCKET_ITEM_DEFAULT; ++i)
        tables[i - 1] = i; 
      Bucket bucket;
      int32_t iret = bucket.initialize(NULL, MAX_BUCKET_DATA_LENGTH, 0);
      EXPECT_EQ(TFS_ERROR, iret);
      iret = bucket.initialize((const char*)tables, 0L, 0L);
      EXPECT_EQ(TFS_ERROR, iret);
      iret = bucket.initialize((const char*)tables, MAX_BUCKET_DATA_LENGTH + 1, 0);
      EXPECT_EQ(TFS_ERROR, iret);
      bucket.destroy();
      iret = bucket.initialize((const char*)tables, MAX_BUCKET_DATA_LENGTH, INVALID_TABLE_VERSION - 1);
      EXPECT_EQ(EXIT_TABLE_VERSION_ERROR, iret);
      iret = bucket.initialize((const char*)tables, MAX_BUCKET_DATA_LENGTH, 0);
      EXPECT_EQ(TFS_SUCCESS, iret);

      Bucket::TABLES_CONST_ITERATOR iter = bucket.tables_.begin();
      for (int32_t j = 0; iter != bucket.tables_.end(); ++iter, ++j)
      {
        EXPECT_EQ(j + 1, (*iter).server_);
        EXPECT_EQ(BUCKET_STATUS_NONE, (*iter).status_);
      }
    }

    TEST_F(BucketTest, query)
    {
      int64_t tables[common::MAX_BUCKET_ITEM_DEFAULT];
      for (int32_t i = 1; i <= common::MAX_BUCKET_ITEM_DEFAULT; ++i)
        tables[i - 1] = i; 
      Bucket bucket;
      int32_t iret = bucket.initialize((const char*)tables, MAX_BUCKET_DATA_LENGTH, 0);
      EXPECT_EQ(TFS_SUCCESS, iret);
      Bucket::TABLES_CONST_ITERATOR iter = bucket.tables_.begin();
      for (int32_t j = 0; iter != bucket.tables_.end(); ++iter, ++j)
      {
        EXPECT_EQ(j + 1, (*iter).server_);
        EXPECT_EQ(BUCKET_STATUS_NONE, (*iter).status_);
      }

      BucketStatus status = BUCKET_STATUS_NONE;
      iret = bucket.query(status, -1, 0, 0);
      EXPECT_EQ(EXIT_BUCKET_ID_INVLAID, iret);
      iret = bucket.query(status, MAX_BUCKET_ITEM_DEFAULT, 0, 0);
      EXPECT_EQ(EXIT_BUCKET_ID_INVLAID, iret);
      iret = bucket.query(status, 0, 1, 1);
      EXPECT_EQ(EXIT_TABLE_VERSION_ERROR, iret);
      iret = bucket.query(status, 0, 0, 0);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(BUCKET_STATUS_NONE, status);
      iret = bucket.query(status, 0, 0, 1);
      EXPECT_EQ(BUCKET_STATUS_NONE, status);
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
