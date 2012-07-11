/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_superblock_impl.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "superblock_impl.h"

const char * FILE_NAME = "file_superblock_impl.test";
using namespace tfs::dataserver;
using namespace std;

class SuperBlockImplTest: public ::testing::Test
{
  public:
    SuperBlockImplTest()
    {
    }
    ~SuperBlockImplTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(SuperBlockImplTest, testWriteRead)
{
  uint32_t super_start_offset = 0;
  tfs::common::MMapOption mmap_option;
  mmap_option.max_mmap_size_ = 256;
  mmap_option.first_mmap_size_ = 128;
  mmap_option.per_mmap_size_ = 4;
  char* block_tag = "taobao";

  int32_t fd = open(FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  tfs::common::SuperBlock super_block;
  tfs::common::SuperBlock read_super_block;

  for (super_start_offset = 0; super_start_offset < 256; ++super_start_offset)
  {
    SuperBlockImpl *super_block_impl = new SuperBlockImpl(FILE_NAME, super_start_offset, false);
    super_block.mmap_option_ = mmap_option;

    memcpy(super_block.mount_tag_, "taobao", strlen("taobao")+1);
    memcpy(super_block.mount_point_, "computer", strlen("computer")+1);
    super_block.mount_point_use_space_ = 32;

    super_block.superblock_reserve_offset_ = 8;
    super_block.bitmap_start_offset_ = 16;
    super_block.avg_segment_size_ = 32;

    super_block.block_type_ratio_ = 0.5;
    super_block.main_block_count_ = 90;
    super_block.main_block_size_ = 1000;
  
    super_block.extend_block_count_ = 10;
    super_block.extend_block_size_ = 100;

    super_block.used_block_count_ = 90;
    super_block.used_extend_block_count_ = 10;

    super_block.hash_slot_ratio_ = 0.3;
    super_block.hash_slot_size_ = 20;
    super_block.version_ = 1;

    EXPECT_EQ(super_block_impl->write_super_blk(super_block), 0);
    EXPECT_EQ(super_block_impl->read_super_blk(read_super_block), 0);

    EXPECT_EQ(super_block.avg_segment_size_, read_super_block.avg_segment_size_);
    EXPECT_STREQ(super_block.mount_tag_, read_super_block.mount_tag_);
    EXPECT_STREQ(super_block.mount_point_, read_super_block.mount_point_);
  
    EXPECT_EQ(super_block.mount_point_use_space_, read_super_block.mount_point_use_space_);
    EXPECT_EQ(super_block.superblock_reserve_offset_, read_super_block.superblock_reserve_offset_);
    EXPECT_EQ(super_block.bitmap_start_offset_, read_super_block.bitmap_start_offset_);
    EXPECT_EQ(super_block.avg_segment_size_, read_super_block.avg_segment_size_);

    EXPECT_EQ(super_block.block_type_ratio_, read_super_block.block_type_ratio_);
    EXPECT_EQ(super_block.main_block_count_, read_super_block.main_block_count_);
    EXPECT_EQ(super_block.main_block_size_, read_super_block.main_block_size_);

    EXPECT_EQ(super_block.extend_block_count_, read_super_block.extend_block_count_);
    EXPECT_EQ(super_block.extend_block_size_, read_super_block.extend_block_size_);

    EXPECT_EQ(super_block.used_block_count_, read_super_block.used_block_count_);
    EXPECT_EQ(super_block.used_extend_block_count_, read_super_block.used_extend_block_count_);

    EXPECT_EQ(super_block.hash_slot_ratio_, read_super_block.hash_slot_ratio_);
    EXPECT_EQ(super_block.hash_slot_size_, read_super_block.hash_slot_size_);
    EXPECT_EQ(super_block.version_, read_super_block.version_);
  
    EXPECT_EQ(super_block.mmap_option_.max_mmap_size_, read_super_block.mmap_option_.max_mmap_size_);
    EXPECT_EQ(super_block.mmap_option_.first_mmap_size_, read_super_block.mmap_option_.first_mmap_size_);
    EXPECT_EQ(super_block.mmap_option_.per_mmap_size_, read_super_block.mmap_option_.per_mmap_size_);
    
    // test check_status
    EXPECT_TRUE(super_block_impl->check_status(block_tag, super_block));  

    delete super_block_impl;
    super_block_impl = NULL;
  }

  close(fd);
  unlink(FILE_NAME);
}

TEST_F(SuperBlockImplTest, testWriteReadBitmap)
{
  uint32_t super_start_offset = 0;
  int32_t item_count = 200;
  int32_t fd = open(FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
 
  BitMap normal_bitmap(item_count, 0);
  for (int32_t i = 0; i < item_count; )
  {
    normal_bitmap.set(i);
    i = i + 3;
  }

  BitMap error_bitmap(item_count, 0);
  for (int32_t i = 0; i < item_count; )
  {
    error_bitmap.set(i);
    i = i+2;
  }
  
  uint32_t bitmap_size = normal_bitmap.get_slot_count();
  char* read_bitmap_buf = new char[4 * bitmap_size + 1];
  char* tmp_bitmap_buf = new char[4 * bitmap_size + 1];

  read_bitmap_buf[4 * bitmap_size] = '\0';
  tmp_bitmap_buf[4 * bitmap_size] = '\0';

  memcpy(tmp_bitmap_buf, normal_bitmap.get_data(), bitmap_size);
  memcpy(tmp_bitmap_buf + bitmap_size, normal_bitmap.get_data(), bitmap_size);
  memcpy(tmp_bitmap_buf + 2 * bitmap_size, error_bitmap.get_data(), bitmap_size);
  memcpy(tmp_bitmap_buf + 3 * bitmap_size, error_bitmap.get_data(), bitmap_size);

  for (super_start_offset = 0; super_start_offset < 256; ++super_start_offset)
  {
    SuperBlockImpl *super_block_impl = new SuperBlockImpl(FILE_NAME, super_start_offset, false);
    EXPECT_EQ(super_block_impl->write_bit_map(&normal_bitmap, &error_bitmap), 0);
    EXPECT_EQ(super_block_impl->read_bit_map(read_bitmap_buf, 4 * bitmap_size), 0); 
    EXPECT_STREQ(read_bitmap_buf, tmp_bitmap_buf);

    delete super_block_impl;
    super_block_impl = NULL;
  }

  close(fd);
  unlink(FILE_NAME);

  delete []read_bitmap_buf;
  read_bitmap_buf = NULL;
  delete []tmp_bitmap_buf;
  tmp_bitmap_buf = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
