/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_lru.cpp 158 2011-02-24 02:11:27Z zongdai@taobao.com $
 *
 * Authors:
 *
 */
#include <gtest/gtest.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "new_client/lru.h"
#include "new_client/tfs_session.h"

using namespace tfs::client;
#ifndef UINT32_MAX
#define UINT32_MAX 4294967295U
#endif

class LruTest: public ::testing::Test
{
  public:
    typedef lru<uint32_t, BlockCache> BLOCK_CACHE_MAP;
    typedef BLOCK_CACHE_MAP::iterator BLOCK_CACHE_MAP_ITER;

  public:
    LruTest()
    {
    }
    ~LruTest()
    {
    }
    virtual void SetUp()
    {
      srandom(time(NULL));
    }
    virtual void TearDown()
    {
    }

    void batch_create(int num)
    {
      if (num > 0)
      {
        for (int i = 0; i < num; ++i)
        {
          uint32_t blockid = 0;
          BlockCache b_cache;
          create(blockid, b_cache);
        }
      }
    }

    void create(uint32_t blockid, BlockCache& b_cache)
    {
      //rand
      if (0 == blockid)
      {
        while (blockid < RESERVE_BLOCK_ID)
        {
          blockid = random() % UINT32_MAX;
        }
        b_cache.last_time_ = time(NULL) + random() % UINT32_MAX;
        b_cache.ds_.push_back(random() % UINT32_MAX);
        b_cache.ds_.push_back(random() % UINT32_MAX);
      }

      block_cache_map_.insert(blockid, b_cache);
    }

    void test_exist(uint32_t blockid, BlockCache& b_cache)
    {
      BlockCache* b_res = block_cache_map_.find(blockid);
      ASSERT_NE(static_cast<void*>(b_res), static_cast<void*>(NULL));
      ASSERT_EQ(b_res->last_time_, b_cache.last_time_);
      ASSERT_EQ(b_res->ds_.size(), b_cache.ds_.size());
      for (int i = 0; i < static_cast<int>(b_res->ds_.size()); ++i)
      {
        ASSERT_EQ(b_res->ds_[i], b_cache.ds_[i]);
      }
    }

    void test_no_exist(uint32_t blockid)
    {
      BlockCache* b_res = block_cache_map_.find(blockid);
      ASSERT_EQ(static_cast<void*>(b_res), static_cast<void*>(NULL));
    }

    void test_equal(BLOCK_CACHE_MAP_ITER it, uint32_t blockid, BlockCache& b_cache)
    {
      ASSERT_EQ(it->first, blockid);
      ASSERT_EQ(it->second.last_time_, b_cache.last_time_);
      ASSERT_EQ(it->second.ds_.size(), b_cache.ds_.size());
      for (int i = 0; i < static_cast<int>(it->second.ds_.size()); ++i)
      {
        ASSERT_EQ(it->second.ds_[i], b_cache.ds_[i]);
      }
    }

  public:
    BLOCK_CACHE_MAP block_cache_map_;
    BLOCK_CACHE_MAP_ITER block_cache_map_iter_;
    static const int32_t DEFAULT_CACHE_SIZE = 10000;
    static const uint32_t RESERVE_BLOCK_ID = 100;

};

TEST_F(LruTest, test_insert)
{
  // test insert not exist item
  uint32_t blockid_1 = 1;
  BlockCache b_cache_1;
  b_cache_1.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_1.ds_.push_back(random() % UINT32_MAX);
  b_cache_1.ds_.push_back(random() % UINT32_MAX);

  uint32_t blockid_2 = 2;
  BlockCache b_cache_2;
  b_cache_2.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_2.ds_.push_back(random() % UINT32_MAX);
  b_cache_2.ds_.push_back(random() % UINT32_MAX);

  uint32_t blockid_3 = 3;
  BlockCache b_cache_3;
  b_cache_3.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_3.ds_.push_back(random() % UINT32_MAX);
  b_cache_3.ds_.push_back(random() % UINT32_MAX);

  create(blockid_1, b_cache_1);
  create(blockid_2, b_cache_2);
  create(blockid_3, b_cache_3);

  test_exist(blockid_1, b_cache_1);
  test_exist(blockid_2, b_cache_2);
  test_exist(blockid_3, b_cache_3);

  // items size in lru is large than default
  // test resize
  block_cache_map_.resize(DEFAULT_CACHE_SIZE);
  // blockid_1 will be extruded
  batch_create(DEFAULT_CACHE_SIZE - 2);

  test_no_exist(blockid_1);
  test_exist(blockid_2, b_cache_2);

  uint32_t blockid_4 = 4;
  BlockCache b_cache_4;
  b_cache_4.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_4.ds_.push_back(random() % UINT32_MAX);
  b_cache_4.ds_.push_back(random() % UINT32_MAX);

  create(blockid_4, b_cache_4);
  test_exist(blockid_4, b_cache_4);
  test_no_exist(blockid_3);

  //now list seq is id4,id2,******
  BLOCK_CACHE_MAP_ITER iter = block_cache_map_.begin();
  test_equal(iter, blockid_4, b_cache_4);

  // test insert exist item
  BlockCache b_cache_2_1;
  b_cache_2_1.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_2_1.ds_.push_back(random() % UINT32_MAX);
  b_cache_2_1.ds_.push_back(random() % UINT32_MAX);
  create(blockid_2, b_cache_2_1);

  iter = block_cache_map_.begin();
  test_equal(iter, blockid_2, b_cache_2_1);

  test_exist(blockid_4, b_cache_4);
  iter = block_cache_map_.begin();
  test_equal(iter, blockid_4, b_cache_4);
}

TEST_F(LruTest, test_remove)
{
  block_cache_map_.resize(DEFAULT_CACHE_SIZE);
  batch_create(DEFAULT_CACHE_SIZE / 2);

  uint32_t blockid_1 = 1;
  BlockCache b_cache_1;
  b_cache_1.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_1.ds_.push_back(random() % UINT32_MAX);
  b_cache_1.ds_.push_back(random() % UINT32_MAX);

  uint32_t blockid_2 = 2;
  BlockCache b_cache_2;
  b_cache_2.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_2.ds_.push_back(random() % UINT32_MAX);
  b_cache_2.ds_.push_back(random() % UINT32_MAX);

  uint32_t blockid_3 = 3;
  BlockCache b_cache_3;
  b_cache_3.last_time_ = time(NULL) + random() % UINT32_MAX;
  b_cache_3.ds_.push_back(random() % UINT32_MAX);
  b_cache_3.ds_.push_back(random() % UINT32_MAX);

  create(blockid_1, b_cache_1);
  create(blockid_2, b_cache_2);
  create(blockid_3, b_cache_3);

  test_exist(blockid_1, b_cache_1);
  test_exist(blockid_2, b_cache_2);
  test_exist(blockid_3, b_cache_3);

  ASSERT_EQ(block_cache_map_.size(), DEFAULT_CACHE_SIZE / 2 + 3);

  //now list seq is id1,id2,id3, ***
  uint32_t blockid_not_exist = 10;
  block_cache_map_.remove(blockid_not_exist);
  ASSERT_EQ(block_cache_map_.size(), DEFAULT_CACHE_SIZE / 2 + 3);
  test_exist(blockid_1, b_cache_1);
  test_exist(blockid_2, b_cache_2);
  test_exist(blockid_3, b_cache_3);

  block_cache_map_.remove(blockid_2);
  ASSERT_EQ(block_cache_map_.size(), DEFAULT_CACHE_SIZE / 2 + 2);
  test_exist(blockid_1, b_cache_1);
  test_no_exist(blockid_2);
  test_exist(blockid_3, b_cache_3);

  batch_create(DEFAULT_CACHE_SIZE / 2);
  uint32_t default_size = DEFAULT_CACHE_SIZE;
  ASSERT_EQ(static_cast<uint32_t>(block_cache_map_.size()), default_size);

  block_cache_map_.remove(blockid_1);
  ASSERT_EQ(static_cast<uint32_t>(block_cache_map_.size()), default_size - 1);

  test_no_exist(blockid_1);
  test_no_exist(blockid_2);
  test_exist(blockid_3, b_cache_3);

  //remove not exist blockid
  block_cache_map_.remove(blockid_1);
  ASSERT_EQ(static_cast<uint32_t>(block_cache_map_.size()), default_size - 1);
  test_no_exist(blockid_1);
  test_no_exist(blockid_2);
  test_exist(blockid_3, b_cache_3);

  //now list seq is id3, ***
  block_cache_map_.remove(blockid_3);
  test_no_exist(blockid_1);
  test_no_exist(blockid_2);
  test_no_exist(blockid_3);
  ASSERT_EQ(static_cast<uint32_t>(block_cache_map_.size()), default_size - 2);
}

TEST_F(LruTest, test_find)
{
  //tested in insert and remove
}

TEST_F(LruTest, test_begin)
{
  //tested in insert
}

TEST_F(LruTest, test_size)
{
  //tested in remove
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
