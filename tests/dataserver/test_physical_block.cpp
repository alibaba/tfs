/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_physical_block.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "physical_block.h"
#include "common/error_msg.h"
#include <tbsys.h>

using namespace tfs::dataserver;
using namespace tfs::common;
const std::string MOUNT_PATH = "./mount";

class PhysicalBlockTest: public ::testing::Test
{
  protected:
    static void SetUpTestCase()
    {
      TBSYS_LOGGER.setLogLevel("error");
    }

    static void TearDownTestCase()
    {
      TBSYS_LOGGER.setLogLevel("debug");
    }
 
  public:
    PhysicalBlockTest()
    {
    }
    ~PhysicalBlockTest()
    {
    }
    virtual void SetUp()
    {
      strcpy(path, MOUNT_PATH.c_str());
      mkdir(path, 0775);
    }
    virtual void TearDown()
    {
      rmdir(path);
    }
  protected:
    char path[100];
};

TEST_F(PhysicalBlockTest, testWriteReadOffset)
{
  uint32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];
  int64_t offset = 0;
  
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);
  
  char* buf = "hello world!";
  char* read_buf = new char[strlen(buf) + 1];

  for (offset = 0; offset < 1024; ++offset)
  {
    int64_t len = static_cast<int64_t>(physical_block->get_total_data_len());
    if (offset + static_cast<int64_t>(strlen(buf) + 1) > len)
    {
      EXPECT_EQ(physical_block->pwrite_data(buf, strlen(buf) + 1, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
      EXPECT_EQ(physical_block->pread_data(read_buf, strlen(buf) + 1, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
    }
    else
    {
      EXPECT_EQ(physical_block->pwrite_data(buf, strlen(buf) + 1, offset), 0);
      EXPECT_EQ(physical_block->pread_data(read_buf, strlen(buf) + 1, offset), 0);
      EXPECT_STREQ(buf, read_buf);
    }
  }

  chdir(path);
  unlink(file_name);
  chdir("..");

  close(fd);
  buf = NULL;
  delete []read_buf;
  read_buf = NULL;
  delete physical_block;
  physical_block = NULL;
}

TEST_F(PhysicalBlockTest, testGetDataLen)
{
  uint32_t physical_block_id = 1;
  int32_t block_length = 0;
  char file_name[100];
  int32_t len = 0;

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  BlockType block_type = C_MAIN_BLOCK;

  for (block_length = BLOCK_RESERVER_LENGTH + 1; block_length < 1000 ; ++block_length)
  {
    PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);
    len = block_length - BLOCK_RESERVER_LENGTH;
    EXPECT_EQ(len, physical_block->get_total_data_len());
    delete physical_block;
    physical_block = NULL;
  }
  
  chdir(path);
  unlink(file_name);
  chdir("..");
  close(fd);
}

TEST_F(PhysicalBlockTest, testGetBlockId)
{
  uint32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  BlockType block_type = C_MAIN_BLOCK;

  for (physical_block_id = 1; physical_block_id < 1000 ; ++physical_block_id)
  {
    PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);
    EXPECT_EQ(physical_block_id, static_cast<uint32_t>(physical_block->get_physic_block_id()));
    delete physical_block;
    physical_block = NULL;
  }

  chdir(path);
  unlink(file_name);
  chdir("..");
  close(fd);
}

TEST_F(PhysicalBlockTest, testBlockPrefix)
{
  uint32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];
  BlockPrefix block_prefix;

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  BlockType block_type = C_MAIN_BLOCK;
  
  PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type); 
  physical_block->set_block_prefix(1, 2, 3);
  EXPECT_EQ(physical_block->dump_block_prefix(), 0);
  EXPECT_EQ(physical_block->load_block_prefix(), 0);
  physical_block->get_block_prefix(block_prefix);

  EXPECT_EQ(static_cast<int32_t>(block_prefix.logic_blockid_), 1);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.prev_physic_blockid_), 2);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.next_physic_blockid_), 3);

  EXPECT_EQ(physical_block->clear_block_prefix(), 0);
  physical_block->get_block_prefix(block_prefix);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.logic_blockid_), 0);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.prev_physic_blockid_), 0);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.next_physic_blockid_), 0);

  delete physical_block;
  physical_block = NULL;

  chdir(path);
  unlink(file_name);
  chdir("..");
  close(fd);
}

TEST_F(PhysicalBlockTest, testBlockPrefix1)
{
  uint32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];
  BlockPrefix block_prefix;

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  BlockType block_type = C_MAIN_BLOCK;
  
  PhysicalBlock* physical_block = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type); 
  
  EXPECT_EQ(physical_block->clear_block_prefix(), 0);
  physical_block->get_block_prefix(block_prefix);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.logic_blockid_), 0);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.prev_physic_blockid_), 0);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.next_physic_blockid_), 0);
 
  physical_block->set_block_prefix(5, 10, 13);
  EXPECT_EQ(physical_block->dump_block_prefix(), 0);
  EXPECT_EQ(physical_block->load_block_prefix(), 0);
  physical_block->get_block_prefix(block_prefix);

  EXPECT_EQ(static_cast<int32_t>(block_prefix.logic_blockid_), 5);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.prev_physic_blockid_), 10);
  EXPECT_EQ(static_cast<int32_t>(block_prefix.next_physic_blockid_), 13);

  physical_block->set_next_block(105);
  physical_block->get_block_prefix(block_prefix);

  EXPECT_EQ(static_cast<int32_t>(block_prefix.next_physic_blockid_), 105);
  
  delete physical_block;
  physical_block = NULL;

  chdir(path);
  unlink(file_name);
  chdir("..");
  close(fd);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
