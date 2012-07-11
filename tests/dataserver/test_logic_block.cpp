/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_logic_block.cpp 32 2011-07-26 08:02:25Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "logic_block.h"
#include "dataserver_define.h"

const std::string BASE_PATH = ".";
const std::string MOUNT_PATH = "./mount";
const std::string MOUNT_SUB_PATH = "./mount/tmp";
const std::string INDEX_PATH = "./index";
const std::string TMP_PATH = "./tmp";

using namespace tfs::dataserver;
using namespace tfs::common;

class LogicBlockTest:public::testing::Test
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
    LogicBlockTest() {
      mmap_option.max_mmap_size_ = 256;
      mmap_option.first_mmap_size_ = 128;
      mmap_option.per_mmap_size_ = 4;

      logic_block_id = 0;
      main_blk_key = 2;

      bucket_size = 16;
      block_type = C_MAIN_BLOCK;
    }
    ~LogicBlockTest() {
    }
    virtual void SetUp()
    {
      strcpy(path, MOUNT_PATH.c_str());
      mkdir(path, 0775);
      mkdir(MOUNT_SUB_PATH.c_str(), 0775);
      mkdir(INDEX_PATH.c_str(), 0775);
      mkdir(TMP_PATH.c_str(), 0775);
    }
    virtual void TearDown()
    {
      rmdir(MOUNT_SUB_PATH.c_str());
      rmdir(path);
      rmdir(INDEX_PATH.c_str());
      rmdir(TMP_PATH.c_str());
    }
  protected:
    MMapOption mmap_option;
    uint32_t logic_block_id;
    uint32_t  main_blk_key;
    char file_name[100];
    char path[100];
    int32_t bucket_size;
    BlockType block_type;
};

TEST_F(LogicBlockTest, testInitBlockFile)
{
  // illegal: logic_block_id = 0
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), EXIT_BLOCKID_ZERO_ERROR);

  delete logic_block;
  logic_block = NULL;

  logic_block_id = 1;
  // success: logic_block_id = 1
  logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);

  delete logic_block;
  logic_block = NULL;

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);
}

TEST_F(LogicBlockTest, testLoadBlockFile)
{
  // illegal: logic_block_id = 0
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), EXIT_BLOCKID_ZERO_ERROR);

  delete logic_block;
  logic_block = NULL;

  logic_block_id = 1;

  // illegal: empty file
  logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), EXIT_INDEX_CORRUPT_ERROR);

  // illegal: already loaded
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), EXIT_INDEX_ALREADY_LOADED_ERROR);

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  delete logic_block;
  logic_block = NULL;

  // sucess:not empty file
  logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  sprintf(file_name, "./index/%d", main_blk_key);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  IndexHeader i_header;
  i_header.block_info_.block_id_ = logic_block_id;
  i_header.block_info_.seq_no_ = 1;
  i_header.index_file_size_ =
    sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
  i_header.bucket_size_ = bucket_size;
  i_header.flag_ = C_DATA_CLEAN;

  // index header + total buckets
  char* init_data = new char[i_header.index_file_size_];
  memcpy(init_data, &i_header, sizeof(IndexHeader));
  memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));

  write(fd, init_data, i_header.index_file_size_);
  close(fd);

  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), 0);

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);
}

TEST_F(LogicBlockTest, testOpenWriteFile)
{
  logic_block_id = 1;

  // success: logic_block_id = 1
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);

  // must be initialized
  uint64_t inner_file_id = 0;
  EXPECT_EQ(logic_block->open_write_file(inner_file_id), 0);
  EXPECT_EQ(inner_file_id, (uint64_t) 1);

  for(int32_t i = 2; i < 100; ++i)
  {
    inner_file_id = 0;
    EXPECT_EQ(logic_block->open_write_file(inner_file_id), 0);
    EXPECT_EQ(inner_file_id, (uint32_t) i);
  }

  int32_t i = 2;
  for(inner_file_id = 2; inner_file_id < 100; ++inner_file_id)
  {
    EXPECT_EQ(logic_block->open_write_file(inner_file_id), 0);
    EXPECT_EQ(inner_file_id, (uint32_t) i);
    ++i;
  }

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testCheckBlockVersion)
{
  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  sprintf(file_name, "./index/%d", main_blk_key);

  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  IndexHeader i_header;
  i_header.block_info_.block_id_ = logic_block_id;
  i_header.block_info_.seq_no_ = 1;
  i_header.block_info_.version_ = 5;
  i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
  i_header.bucket_size_ = bucket_size;
  i_header.flag_ = C_DATA_CLEAN;

  char* init_data = new char[i_header.index_file_size_];
  memcpy(init_data, &i_header, sizeof(IndexHeader));
  memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));

  write(fd, init_data, i_header.index_file_size_);
  close(fd);

  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), 0);

  int32_t remote_version = 5;
  UpdateBlockType repair = UPDATE_BLOCK_NORMAL;

  // same
  EXPECT_EQ(logic_block->check_block_version(remote_version, repair), 0);

  // illegal
  remote_version = 8;
  EXPECT_LT(logic_block->check_block_version(remote_version, repair), 0);
  remote_version = 2;
  EXPECT_LT(logic_block->check_block_version(remote_version, repair), 0);

  // local version modify
  remote_version = 6;
  EXPECT_EQ(logic_block->check_block_version(remote_version, repair), 0);

  // remote version modify
  remote_version = 4;
  EXPECT_EQ(logic_block->check_block_version(remote_version, repair), 0);
  EXPECT_EQ(6, remote_version);

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);
}

TEST_F(LogicBlockTest, testCloseWriteFile)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  block_length = 2 * 1024 * 1024;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  DataFile data_file(1);

  char* data = "hangzhou";
  data_file.set_data(data, strlen(data) + 1, 0);

  uint64_t inner_file_id = 1;
  uint32_t crc = 0;

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);

  int32_t len = strlen(data) + 1 + sizeof(FileInfo);
  char* tmp_data = new char[len];
  EXPECT_EQ(logic_block->read_file(inner_file_id, tmp_data, len, 0, READ_DATA_OPTION_FLAG_NORMAL), 0);

  char read_data[sizeof(FileInfo)];
  memcpy(read_data, tmp_data + sizeof(FileInfo), strlen(data) + 1);
  EXPECT_STREQ(data, read_data);

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
  delete[]tmp_data;
  tmp_data = NULL;
}

TEST_F(LogicBlockTest, testCloseWriteDatFile)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  block_length = 2 * 1024 * 1024;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  int64_t data_len = 3 * 1024 * 1024;
  char* write_data = new char[data_len];

  for(int32_t i = 0; i < data_len; ++i)
  {
    write_data[i] = 'a' + (i % 26);
  }
  write_data[data_len] = '\0';

  uint64_t file_number = 1;
  DataFile data_file(file_number, "./mount");
  data_file.set_data(write_data, data_len, 0);

  uint64_t inner_file_id = 1;
  uint32_t crc = 0;
  int32_t read_data_len = data_len + sizeof(FileInfo);
  char* read_file_data = new char[read_data_len];

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);
  EXPECT_EQ(logic_block->read_file(inner_file_id, read_file_data, read_data_len, 0, READ_DATA_OPTION_FLAG_NORMAL), 0);
  read_file_data[sizeof(FileInfo) + data_len] = '\0';
  EXPECT_STREQ(read_file_data + sizeof(FileInfo), write_data);

  delete write_data;
  write_data = NULL;
  delete read_file_data;
  read_file_data = NULL;

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testReadFileInfo)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  DataFile data_file(1);

  char data[] = "hangzhou";
  data_file.set_data(data, strlen(data) + 1, 0);

  uint64_t inner_file_id = 1;
  uint32_t crc = 0;

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);

  FileInfo file_info;
  EXPECT_EQ(logic_block->read_file_info(inner_file_id, file_info), 0);
  EXPECT_EQ(file_info.id_, (uint64_t) 1);
  EXPECT_EQ(file_info.offset_, 0);
  EXPECT_EQ(file_info.size_, static_cast<int32_t> (sizeof(FileInfo) + strlen(data) + 1));
  EXPECT_EQ(file_info.usize_, static_cast<int32_t> (sizeof(FileInfo) + strlen(data) + 1));

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testReNameFile)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  DataFile data_file(1);

  char data[] = "hangzhou";
  data_file.set_data(data, strlen(data) + 1, 0);

  uint64_t inner_file_id = 1;
  uint64_t new_file_id = 2;
  uint32_t crc = 0;

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);
  EXPECT_EQ(logic_block->rename_file(inner_file_id, new_file_id), 0);

  FileInfo file_info;
  EXPECT_EQ(logic_block->read_file_info(new_file_id, file_info), 0);
  EXPECT_EQ(file_info.id_, (uint64_t) 2);
  EXPECT_EQ(file_info.offset_, 0);
  EXPECT_EQ(file_info.size_, static_cast<int32_t> (sizeof(FileInfo) + strlen(data) + 1));
  EXPECT_EQ(file_info.usize_, static_cast<int32_t> (sizeof(FileInfo) + strlen(data) + 1));

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testResetBlockVersion)
{
  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  sprintf(file_name, "./index/%d", main_blk_key);
  int32_t fd = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  IndexHeader i_header;
  i_header.block_info_.block_id_ = logic_block_id;
  i_header.block_info_.seq_no_ = 1;
  i_header.block_info_.version_ = 5;
  i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
  i_header.bucket_size_ = bucket_size;
  i_header.flag_ = C_DATA_CLEAN;

  char* init_data = new char[i_header.index_file_size_];
  memcpy(init_data, &i_header, sizeof(IndexHeader));
  memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));

  write(fd, init_data, i_header.index_file_size_);
  close(fd);

  EXPECT_EQ(logic_block->load_block_file(bucket_size, mmap_option), 0);

  int32_t remote_version = 5;
  int32_t reset_version = 1;
  UpdateBlockType repair = UPDATE_BLOCK_NORMAL;

  EXPECT_EQ(logic_block->check_block_version(remote_version, repair), 0);
  EXPECT_EQ(logic_block->reset_block_version(), 0);
  EXPECT_EQ(logic_block->check_block_version(reset_version, repair), 0);

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);
}

TEST_F(LogicBlockTest, testUnlinkFile)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  DataFile data_file(1);

  char data[] = "hangzhou";
  data_file.set_data(data, strlen(data) + 1, 0);

  int32_t crc = 0;
  for(uint64_t inner_file_id = 1; inner_file_id < 5; ++inner_file_id)
  {
    EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);
  }

  BlockInfo* m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->file_count_, 4);
  EXPECT_EQ(m_block_info->version_, 4);
  EXPECT_EQ(m_block_info->del_file_count_, 0);

  FileInfo file_info;
  int64_t file_size;

  logic_block->unlink_file(1, DELETE, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 1);
  EXPECT_EQ(logic_block->read_file_info(1, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_DELETED, 1);

  logic_block->unlink_file(2, DELETE, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 2);
  EXPECT_EQ(logic_block->read_file_info(2, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_DELETED, 1);

  logic_block->unlink_file(1, UNDELETE, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 1);
  EXPECT_EQ(logic_block->read_file_info(1, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_DELETED, 0);

  logic_block->unlink_file(2, UNDELETE, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 0);
  EXPECT_EQ(logic_block->read_file_info(1, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_DELETED, 0);

  logic_block->unlink_file(1, CONCEAL, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 0);
  EXPECT_EQ(logic_block->read_file_info(1, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_CONCEAL, 4);

  logic_block->unlink_file(1, REVEAL, file_size);
  m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->del_file_count_, 0);
  EXPECT_EQ(logic_block->read_file_info(1, file_info), 0);
  EXPECT_EQ(file_info.flag_ & FI_CONCEAL, 0);

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testBatchReadData)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  DataFile data_file(1);
  char* data = "hangzhou";
  data_file.set_data(data, strlen(data) + 1, 0);

  uint64_t inner_file_id = 1;
  uint32_t crc = 0;

  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);

  inner_file_id = 2;
  char* data1 = "wuhan";
  data_file.set_data(data1, strlen(data) + 1, 0);
  EXPECT_EQ(logic_block->close_write_file(inner_file_id, &data_file, crc), 0);

  int32_t data_len = 2 * sizeof(FileInfo) + strlen(data) + strlen(data1) + 2;
  char* read_data = new char[data_len];
  logic_block->read_raw_data(read_data, data_len, 0);

  EXPECT_STREQ(data, read_data + sizeof(FileInfo));
  EXPECT_STREQ(data1, read_data + 2 * sizeof(FileInfo) + strlen(data) + 1);

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testBatchWriteData)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  char* write_buf = "world";
  char* read_buf = new char[strlen(write_buf) + 1];
  int32_t data_len = strlen(write_buf) + 1;

  for(int32_t offset = 0; offset < 100; ++offset)
  {
    EXPECT_EQ(logic_block->write_raw_data(write_buf, (int64_t) strlen(write_buf) + 1, offset), 0);
    EXPECT_EQ(logic_block->read_raw_data(read_buf, data_len, offset), 0);
    EXPECT_STREQ(write_buf, read_buf);
  }

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testBatchWriteMetaVec)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  uint64_t file_id = 1;
  uint64_t in_offset = 0;
  uint64_t file_size = 10;
  RawMeta meta_info1(file_id, in_offset, file_size);

  file_id = 2;
  in_offset = 10;
  file_size = 20;
  RawMeta meta_info2(file_id, in_offset, file_size);

  RawMetaVec meta_info_vec;
  meta_info_vec.push_back(meta_info1);
  meta_info_vec.push_back(meta_info2);

  BlockInfo blk_info;
  blk_info.block_id_ = 1;
  blk_info.version_ = 1;
  blk_info.file_count_ = 2;
  blk_info.size_ = 30;
  blk_info.del_file_count_ = 0;
  blk_info.del_size_ = 10;
  blk_info.seq_no_ = 1;

  EXPECT_EQ(logic_block->batch_write_meta(&blk_info, &meta_info_vec), 0);

  BlockInfo* m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->block_id_, (uint32_t) 1);
  EXPECT_EQ(m_block_info->version_, 1);
  EXPECT_EQ(m_block_info->file_count_, 2);
  EXPECT_EQ(m_block_info->size_, 30);
  EXPECT_EQ(m_block_info->del_file_count_, 0);
  EXPECT_EQ(m_block_info->del_size_, 10);
  EXPECT_EQ(m_block_info->seq_no_, (uint32_t) 1);

  RawMetaVec get_meta_info_vec;
  EXPECT_EQ(logic_block->get_meta_infos(get_meta_info_vec), 0);
  EXPECT_EQ(get_meta_info_vec[0].get_file_id(), (uint32_t) 1);
  EXPECT_EQ(get_meta_info_vec[0].get_offset(), 0);
  EXPECT_EQ(get_meta_info_vec[0].get_size(), 10);

  EXPECT_EQ(get_meta_info_vec[1].get_file_id(), (uint32_t) 2);
  EXPECT_EQ(get_meta_info_vec[1].get_offset(), 10);
  EXPECT_EQ(get_meta_info_vec[1].get_size(), 20);

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

TEST_F(LogicBlockTest, testBatchWriteRawMeta)
{
  int32_t physical_block_id = 1;
  int32_t block_length = 1024;
  char file_name[100];

  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd1 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  BlockType block_type = C_MAIN_BLOCK;
  PhysicalBlock* physical_block1 =
    new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  physical_block_id = 2;
  chdir(path);
  sprintf(file_name, "%d", physical_block_id);
  int32_t fd2 = open(file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  chdir("..");

  PhysicalBlock* physical_block2 = new PhysicalBlock(physical_block_id, MOUNT_PATH, block_length, block_type);

  logic_block_id = 1;
  LogicBlock* logic_block = new LogicBlock(logic_block_id, main_blk_key, BASE_PATH);
  EXPECT_EQ(logic_block->init_block_file(bucket_size, mmap_option, block_type), 0);
  logic_block->add_physic_block(physical_block1);
  logic_block->add_physic_block(physical_block2);

  uint64_t file_id = 1;
  uint64_t in_offset = 0;
  uint64_t file_size = 10;
  RawMeta raw_meta1(file_id, in_offset, file_size);

  file_id = 2;
  in_offset = 10;
  file_size = 20;
  RawMeta raw_meta2(file_id, in_offset, file_size);

  RawMetaVec raw_meta_vec;
  raw_meta_vec.push_back(raw_meta1);
  raw_meta_vec.push_back(raw_meta2);

  BlockInfo blk_info;
  blk_info.block_id_ = 1;
  blk_info.version_ = 1;
  blk_info.file_count_ = 2;
  blk_info.size_ = 30;
  blk_info.del_file_count_ = 0;
  blk_info.del_size_ = 10;
  blk_info.seq_no_ = 1;

  EXPECT_EQ(logic_block->batch_write_meta(&blk_info, &raw_meta_vec), 0);

  BlockInfo* m_block_info = logic_block->get_block_info();
  EXPECT_EQ(m_block_info->block_id_, (uint32_t) 1);
  EXPECT_EQ(m_block_info->version_, 1);
  EXPECT_EQ(m_block_info->file_count_, 2);
  EXPECT_EQ(m_block_info->size_, 30);
  EXPECT_EQ(m_block_info->del_file_count_, 0);
  EXPECT_EQ(m_block_info->del_size_, 10);
  EXPECT_EQ(m_block_info->seq_no_, (uint32_t) 1);

  RawMetaVec raw_meta_vecs;
  EXPECT_EQ(logic_block->get_meta_infos(raw_meta_vecs), 0);
  EXPECT_EQ(raw_meta_vecs[0].get_file_id(), (uint32_t) 1);
  EXPECT_EQ(raw_meta_vecs[0].get_offset(), 0);
  EXPECT_EQ(raw_meta_vecs[0].get_size(), 10);

  EXPECT_EQ(raw_meta_vecs[1].get_file_id(), (uint32_t) 2);
  EXPECT_EQ(raw_meta_vecs[1].get_offset(), 10);
  EXPECT_EQ(raw_meta_vecs[1].get_size(), 20);

  chdir(path);
  sprintf(file_name, "%d", physical_block1->get_physic_block_id());
  unlink(file_name);
  sprintf(file_name, "%d", physical_block2->get_physic_block_id());
  unlink(file_name);
  chdir("..");

  sprintf(file_name, "./index/%d", main_blk_key);
  unlink(file_name);

  close(fd1);
  close(fd2);

  delete physical_block1;
  physical_block1 = NULL;
  delete physical_block2;
  physical_block2 = NULL;
  delete logic_block;
  logic_block = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
