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
 *   chuyu 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "block_chunk.h"
#include <Memory.hpp>
#include "block_collect.h"
#include "server_collect.h"

using namespace tfs::nameserver;
using namespace tfs::common;

class BlockChunkTest:public::testing::Test
{
public:
  static void SetUpTestCase()
  {
    TBSYS_LOGGER.setLogLevel("debug");
  }
  
  static void TearDownTestCase()
  {
  }
  BlockChunkTest()
  {
  }
  ~BlockChunkTest()
  {
  }
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }
};

TEST_F(BlockChunkTest, add_by_id)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  int32_t block_id = 100;
  EXPECT_EQ(true, ptr->add(block_id, now) != NULL);
  BlockCollect* block_collect = ptr->find(block_id);
  EXPECT_EQ(false, ptr->add(block_id, now) == block_collect);
}

/*TEST_F(BlockChunkTest, add_by_collect)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  bool overwrite = false;
  BlockCollect block(100, now);
  EXPECT_EQ(true, ptr->add(&block, now, overwrite) == &block);

  BlockCollect* new_block = new BlockCollect(101, now);
  
  EXPECT_EQ(true, ptr->add(new_block, now, overwrite) == new_block);

  overwrite = true;
  BlockCollect new_block_ext(101, now);
  EXPECT_EQ(true, ptr->add(&new_block_ext, now, overwrite) == &new_block_ext);
}*/

TEST_F(BlockChunkTest, connect)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  uint32_t block_id = 100;

  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  bool force = false;
  bool master = false;
  ServerCollect server(info, now);
  BlockCollect block(block_id, now);
  EXPECT_EQ(true, ptr->connect(&block, &server, now, force, master));
}

TEST_F(BlockChunkTest, remove)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  int32_t block_id = 100;
  EXPECT_EQ(true, ptr->add(block_id, now) != NULL);
  BlockCollect* block_collect = ptr->find(block_id);
  EXPECT_EQ(false, ptr->add(block_id, now) == block_collect);

  EXPECT_EQ(true, ptr->remove(block_id));
  EXPECT_EQ(true, ptr->find(100) == NULL);
}

TEST_F(BlockChunkTest, find)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  uint32_t block_id = 100;
  EXPECT_EQ(true, (ptr->add(block_id, now))->id() == block_id);
  EXPECT_EQ(true, (ptr->find(block_id))->id() == block_id);
  EXPECT_EQ(true, ptr->find(101) == NULL);
}

TEST_F(BlockChunkTest, exist)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  uint32_t block_id = 100;
  EXPECT_EQ(true, (ptr->add(block_id, now))->id() == block_id);

  EXPECT_EQ(true, ptr->exist(block_id));
  EXPECT_EQ(false, ptr->exist(101));
}

TEST_F(BlockChunkTest, calc_max_block_id)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  int32_t block_id = 100;
  EXPECT_EQ(true, ptr->add(block_id, now) != NULL);
  EXPECT_EQ(true, ptr->add(1200, now) != NULL);
  EXPECT_EQ(true, ptr->add(1000, now) != NULL);
  EXPECT_EQ(true, ptr->add(10000, now) != NULL);
  EXPECT_EQ(10000U, ptr->calc_max_block_id());
}

TEST_F(BlockChunkTest, calc_all_block_bytes)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();

  ptr->add(100, now);
  BlockCollect* block = ptr->find(100);
  block->info_.size_ = 1000;

  ptr->add(102, now);
  BlockCollect* block2 = ptr->find(102);
  block2->info_.size_ = 2000;

  EXPECT_EQ(ptr->calc_all_block_bytes(), 3000);
}

TEST_F(BlockChunkTest, calc_size)
{
  time_t now = time(NULL);
  BlockChunkPtr ptr = new BlockChunk();
  uint32_t block_id = 100;
  EXPECT_EQ(true, (ptr->add(100, now))->id() == block_id);
  EXPECT_EQ(true, ptr->calc_size() == 1);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
