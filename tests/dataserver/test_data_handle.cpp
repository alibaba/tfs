/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_data_handle.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#ifndef __DS_UNIT_TEST__
#define __DS_UNIT_TEST__

#include <gtest/gtest.h>
#include "logic_block.h"
#include "common/error_msg.h"
using namespace tfs::dataserver;
using namespace tfs::common;

const std::string MOUNT_PATH = "./mount";

class DataHandleTest: public ::testing::Test
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
    DataHandleTest()
    {
    }
    ~DataHandleTest()
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

TEST_F(DataHandleTest, testReadWrite)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];
  int32_t data_len = 26;
  int32_t offset = 0;

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  
  LogicBlock* logic_block = new LogicBlock(1);
  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);
  
  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type); 
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2); 

  char* buf = new char[data_len];
  for (int32_t i = 0; i < data_len - 1; ++i)
  {
    buf[i] = 'a' + (i % 26);
  }
  buf[data_len] = '\0';
  char* read_buf = new char[strlen(buf) + 1];

  DataHandle* m_data_handle = new DataHandle(logic_block);

  for (offset = 0; offset < 2000; ++offset)
  {
    if (offset + data_len <= 1024)
    {
      EXPECT_EQ(m_data_handle->write_segment_data(buf, strlen(buf) + 1, offset), 0);
      EXPECT_EQ(m_data_handle->read_segment_data(read_buf, strlen(buf) + 1, offset), 0);
      EXPECT_STREQ(read_buf, buf);
    }
    else
    {
      EXPECT_EQ(m_data_handle->write_segment_data(buf, strlen(buf) + 1, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
      EXPECT_EQ(m_data_handle->read_segment_data(read_buf, strlen(buf) + 1, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
    }
  }

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");
 
  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete m_data_handle;
  m_data_handle = NULL;
  delete logic_block;
  logic_block = NULL;
  delete []read_buf;
  read_buf = NULL;
}

TEST_F(DataHandleTest, testReadWriteInfo)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];
  int32_t offset = 0;

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");
  
  LogicBlock* logic_block = new LogicBlock(1);
  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);
  
  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type); 
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2); 

  DataHandle* m_data_handle = new DataHandle(logic_block);

  FileInfo* m_file_info = NULL;
  FileInfo* read_file_info = NULL;

  for (offset = 0; offset < 2000; ++offset)
  {
    if (m_file_info == NULL)
    {
      EXPECT_EQ(m_data_handle->write_segment_info(m_file_info,  offset), EXIT_POINTER_NULL);
      EXPECT_EQ(m_data_handle->read_segment_info(read_file_info, offset), EXIT_POINTER_NULL);
    }
  }

  m_file_info = new FileInfo;
  read_file_info = new FileInfo;

  m_file_info->id_ = 1;
  m_file_info->offset_ = 0;
  m_file_info->size_ = 10;
  m_file_info->usize_ = 15;
  m_file_info->modify_time_ = 3;
  m_file_info->create_time_ = 8;
  m_file_info->flag_ = 0;
  m_file_info->crc_ = 10;
  
  for (offset = 0; offset < 2000; ++offset)
  {
    if (offset + FILEINFO_SIZE <= 1024)
    {
      EXPECT_EQ(m_data_handle->write_segment_info(m_file_info, offset), 0);
      EXPECT_EQ(m_data_handle->read_segment_info(read_file_info, offset), 0);

      EXPECT_EQ(m_file_info->id_, read_file_info->id_);
      EXPECT_EQ(m_file_info->offset_, read_file_info->offset_);
      EXPECT_EQ(m_file_info->size_, read_file_info->size_);
      EXPECT_EQ(m_file_info->usize_, read_file_info->usize_);
      EXPECT_EQ(m_file_info->modify_time_, read_file_info->modify_time_);
      EXPECT_EQ(m_file_info->create_time_, read_file_info->create_time_);
      EXPECT_EQ(m_file_info->flag_, read_file_info->flag_);
      EXPECT_EQ(m_file_info->crc_, read_file_info->crc_);
    }
    else
    {
      EXPECT_EQ(m_data_handle->write_segment_info(m_file_info, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
      EXPECT_EQ(m_data_handle->read_segment_info(read_file_info, offset), EXIT_PHYSIC_BLOCK_OFFSET_ERROR);
    }
  }

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");
 
  close(fd1);
  close(fd2);
  
  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete m_data_handle;
  m_data_handle = NULL;
  delete logic_block;
  logic_block = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif
