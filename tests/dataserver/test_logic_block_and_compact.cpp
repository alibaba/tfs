/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_logic_block_and_compact.cpp 155 2011-02-21 07:33:27Z zongdai@taobao.com $
 *
 * Authors:
 *   yangye
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "logic_block.h"
#include "data_file.h"
#include "compact_block.h"
#include "physical_block.h"


#define main_id 100
#define ext_id 200
#define block_path "."
#define data_length 4096
#define main_block_length (41943040 * 2)   
#define ext_block_length 4096      
#define write_buf_length 100 
#define logical_block_id 110
#define BLOCK_RESERVER_LENGTH 512

static const int32_t INSERT_FILE_NUM = 1000;
using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;

// need one more test for some exception, with TEST instead
class TestLogicBlock: public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    TBSYS_LOGGER.setLogLevel("error");
    mkdir("index", 10705);
    mkdir("extend", 10705);
    mkdir("tmp", 10705);
  }

  static void TearDownTestCase()
  {
    TBSYS_LOGGER.setLogLevel("debug");
    rmdir("index");
    rmdir("extend");
    rmdir("tmp");
  }

public:
  virtual void SetUp()
  {
    index_handle = fopen("./index/100", "w+");
    fclose(index_handle);
    EXPECT_EQ(access("./index/100", 0), 0);

    index_handle = fopen("./index/101", "w+");
    fclose(index_handle);

    //init for datahandle
    main_block_file = fopen("./100", "w+");
    fclose(main_block_file);

    main_block_file = fopen("./101", "w+");
    fclose(main_block_file);

    EXPECT_EQ(access("./100", 0), 0) << "cannot create file, might because the directory extend is not created";
    ext_file1 = fopen("./extend/200", "w+");
    fclose(ext_file1);
    EXPECT_EQ(access("./extend/200", 0), 0);
    ext_file2 = fopen("./extend/201", "w+");
    fclose(ext_file2);
    EXPECT_EQ(access("./extend/201", 0), 0);
    ext_file3 = fopen("./extend/202", "w+");
    fclose(ext_file3);
    EXPECT_EQ(access("./extend/202", 0), 0);
    logic_block = new LogicBlock(logical_block_id, main_id, ".");
    dest_block = new LogicBlock(logical_block_id + 1, 101, ".");
    main_block = new PhysicalBlock(main_id, block_path, main_block_length, C_MAIN_BLOCK);
    m2 = new PhysicalBlock(101, block_path, main_block_length, C_MAIN_BLOCK);
    ext_block = new PhysicalBlock(ext_id, block_path, ext_block_length, C_EXT_BLOCK);
    ext_block2 = new PhysicalBlock(ext_id + 1, block_path, ext_block_length, C_EXT_BLOCK);
    ext_block3 = new PhysicalBlock(ext_id + 2, block_path, ext_block_length, C_EXT_BLOCK);
    logic_block->get_physic_block_list()->clear();
    dest_block->get_physic_block_list()->clear();

    mmap_option.max_mmap_size_ = 1024;
    mmap_option.first_mmap_size_ = 1024;
    mmap_option.per_mmap_size_ = 1024;
  }

  virtual void TearDown()
  {
    //indexhandle
    unlink("./index/100");
    EXPECT_NE(access("./index/100", 0), 0);

    unlink("./index/101");
    EXPECT_NE(access("./index/101", 0), 0);
    //datahandle
    delete (logic_block);
    unlink("./100");
    EXPECT_NE(access("./100", 0), 0);
    unlink("./101");
    EXPECT_NE(access("./101", 0), 0);
    unlink("./extend/200");
    EXPECT_NE(access("./extend/200", 0), 0);
    unlink("./extend/201");
    EXPECT_NE(access("./extend/201", 0), 0);
    unlink("./extend/202");
    EXPECT_NE(access("./extend/202", 0), 0);
  }

public:
  LogicBlock* logic_block;
  LogicBlock* dest_block;
  FILE* main_block_file;
  FILE* ext_file1;
  FILE* ext_file2;
  FILE* ext_file3;
  PhysicalBlock* main_block;
  PhysicalBlock* m2;
  PhysicalBlock* ext_block;
  PhysicalBlock* ext_block2;
  PhysicalBlock* ext_block3;
  FILE* index_handle;
  MMapOption mmap_option;
};

TEST_F(TestLogicBlock, testCreate)
{
  EXPECT_EQ(logic_block->get_visit_count(), 0);
  EXPECT_TRUE(logic_block->get_physic_block_list()->empty());
  EXPECT_LE(logic_block->get_last_update(), time(NULL));
}

// init load delete block file
TEST_F(TestLogicBlock, testBlockFileOP)
{
  // init_block_file
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  FILE* temp = fopen("./index/100", "r");
  IndexHeader index_header;
  fread((char*)&index_header, sizeof(IndexHeader), 1, temp);
  EXPECT_EQ(index_header.flag_, C_DATA_CLEAN);
  EXPECT_EQ(index_header.bucket_size_, 16);
  EXPECT_EQ(index_header.data_file_offset_, 0);
  EXPECT_EQ(index_header.index_file_size_, static_cast<int32_t>(sizeof(IndexHeader) + 16 * sizeof(int32_t)));
  EXPECT_EQ(index_header.free_head_offset_, 0);
  EXPECT_EQ(110, static_cast<int32_t>(index_header.block_info_.block_id_));
  EXPECT_EQ(0, index_header.block_info_.version_);
  EXPECT_EQ(0, index_header.block_info_.file_count_);
  EXPECT_EQ(0, index_header.block_info_.size_);
  EXPECT_EQ(0, index_header.block_info_.del_file_count_);
  EXPECT_EQ(0, index_header.block_info_.del_size_);
  EXPECT_EQ(1, static_cast<int32_t>(index_header.block_info_.seq_no_));
  fclose(temp);
  // did not test
  logic_block->load_block_file(bucket_size, mmap_option);
  logic_block->delete_block_file();
}

TEST_F(TestLogicBlock, testPhysicBlock)
{
  logic_block->add_physic_block(main_block);
  logic_block->add_physic_block(ext_block);
  logic_block->add_physic_block(ext_block2);
  logic_block->add_physic_block(ext_block3);
  list<PhysicalBlock*>::iterator temp = logic_block->get_physic_block_list()->begin();
  EXPECT_TRUE(*temp == main_block);
  temp++;
  EXPECT_TRUE(*temp == ext_block);
  temp++;
  EXPECT_TRUE(*temp == ext_block2);
  temp++;
  EXPECT_TRUE(*temp == ext_block3);
  temp++;
  EXPECT_TRUE(temp == logic_block->get_physic_block_list()->end());
}

//open_write_file close_write_file
TEST_F(TestLogicBlock, testWriteFile) 
{
  //open_write_file is used to get a available file id_
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);

  FILE* temp = fopen("./index/100", "r");
  IndexHeader index_header;
  fread(reinterpret_cast<char*>(&index_header), sizeof(IndexHeader), 1, temp);
  uint64_t file_name = 0;
  logic_block->open_write_file(file_name);
  EXPECT_EQ(file_name, index_header.block_info_.seq_no_);
  fclose(temp);
  // close_write_file is used to write data
  DataFile* data_file = new DataFile(file_name);
  int32_t* data = new int32_t[data_length];
  int32_t offset = 0;
  for(int32_t i = 0; i < data_length; i++)
  {
    data[i] = i;
  }
  data_file->set_data(reinterpret_cast<char*>(data), data_length * sizeof(int32_t), offset);
  uint32_t crc = data_file->get_crc();
  main_block->clear_block_prefix();
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);

  // write the data_file to filename
  logic_block->close_write_file(file_name, data_file, crc);
  temp = fopen("./100", "r");

  // index_header bucket meta_info
  fseek(temp, BLOCK_RESERVER_LENGTH, 0);
  FileInfo file_info;
  fread(reinterpret_cast<char*>(&file_info), sizeof(FileInfo), 1, temp);
  logic_block->open_write_file(file_name);
  EXPECT_EQ(file_info.id_, file_name-1); //file info in the physic block
  EXPECT_EQ(file_info.offset_, 0);
  EXPECT_EQ(file_info.size_, static_cast<int32_t>(sizeof(FileInfo) + data_length * sizeof(int32_t)));
  EXPECT_EQ(file_info.usize_, static_cast<int32_t>(sizeof(FileInfo) + data_length * sizeof(int32_t)));
  EXPECT_EQ(file_info.flag_, 0);
  EXPECT_EQ(file_info.crc_, crc);
  int32_t* buf = new int32_t[data_length];
  fread(reinterpret_cast<char*>(buf), data_length * sizeof(int32_t), 1, temp);
  for (int32_t i = 0; i < data_length; i++)
  {
    EXPECT_EQ(buf[i], i);
  }
  fclose(temp);
  delete(data);
  delete(buf);
  delete(data_file);
  unlink("./index/100");
}

TEST_F(TestLogicBlock, testFileOP)
{
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  uint64_t file_id[10];
  // write file
  memset(file_id, 0, sizeof(file_id));
  // writedata
  int32_t* data = new int32_t[data_length];
  int32_t offset = 0;
  DataFile* data_file;
  int32_t data_size = data_length * sizeof(int32_t) + sizeof(FileInfo);
  int64_t file_size;
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);
  for(int32_t i = 0; i < 10; i++)
  {
    // if want to get a new file_id, assign 0 first
    logic_block->open_write_file(file_id[i]);
    for(int32_t j = 0; j < data_length; j++)
    {
      data[j] = j + i * j;
    }
    data_file = new DataFile(file_id[i]);
    data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete(data_file);
  }
  delete(data);
  int32_t* buf = new int32_t[data_length + sizeof(FileInfo) / sizeof(int32_t)];
  for(int32_t i = 0; i < 10; i++)
  {
    offset = 0;
    logic_block->read_file(file_id[i], reinterpret_cast<char*>(buf), data_size, offset, READ_DATA_OPTION_FLAG_NORMAL);
    FileInfo* raad_info = (FileInfo*)buf;
    EXPECT_EQ(file_id[i], raad_info->id_);
    EXPECT_EQ(raad_info->offset_, static_cast<int32_t>((file_id[i] - 1) * data_size));
    EXPECT_EQ(raad_info->size_, data_size);
    int32_t* databuf = reinterpret_cast<int32_t*>(reinterpret_cast<char*>(buf) + sizeof(FileInfo));
    EXPECT_EQ(static_cast<int32_t>(file_id[i]), i + 1);
    for(int32_t j = 0; j < data_length; j++)
    {
      EXPECT_EQ(databuf[j], j + i * j);
    }
    FileInfo temp;
    logic_block->read_file_info(file_id[i], temp);
    EXPECT_EQ(file_id[i], temp.id_);
    EXPECT_EQ(temp.offset_, static_cast<int32_t>((file_id[i] - 1) * data_size));
    EXPECT_EQ(temp.size_, data_size);
    EXPECT_EQ(temp.crc_, raad_info->crc_);
  }
  delete(buf);
  // rename 
  uint64_t old_id = file_id[0];
  uint64_t new_id = 0;
  logic_block->open_write_file(new_id);
  // if the file exist, read_file_info will set the value of file info
  FileInfo exist, temp_file;
  memset(&temp_file, 0, sizeof(FileInfo));
  logic_block->read_file_info(old_id, temp_file);
  EXPECT_NE(0, static_cast<int32_t>(temp_file.id_));
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_EQ(0, static_cast<int32_t>(exist.id_));
  // old_id file exist, new_id file is not.
  logic_block->rename_file(old_id, new_id);
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(old_id, exist);
  EXPECT_EQ(0, static_cast<int32_t>(exist.id_));
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_NE(0, static_cast<int32_t>(exist.id_));
  EXPECT_EQ(temp_file.offset_, exist.offset_);
  EXPECT_EQ(temp_file.size_, exist.size_);
  EXPECT_EQ(temp_file.usize_, exist.usize_);
  EXPECT_EQ(temp_file.crc_, exist.crc_);
  EXPECT_EQ(temp_file.modify_time_, exist.modify_time_);
  EXPECT_EQ(temp_file.create_time_, exist.create_time_);
  EXPECT_EQ(temp_file.flag_, exist.flag_);

  // unlink
  // delete
  EXPECT_EQ(logic_block->unlink_file(old_id, DELETE, file_size), EXIT_META_NOT_FOUND_ERROR);
  logic_block->unlink_file(new_id, DELETE, file_size);
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_EQ(exist.flag_&FI_DELETED, FI_DELETED);
  //undelete
  EXPECT_EQ(logic_block->unlink_file(file_id[1], UNDELETE, file_size), EXIT_FILE_STATUS_ERROR);
  logic_block->unlink_file(new_id, UNDELETE, file_size);
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_EQ(exist.flag_&FI_DELETED, 0);
  // conceal
  logic_block->unlink_file(file_id[1], DELETE, file_size);
  EXPECT_EQ(logic_block->unlink_file(file_id[1], CONCEAL, file_size), EXIT_FILE_STATUS_ERROR);
  logic_block->unlink_file(file_id[1], UNDELETE, file_size);
  logic_block->unlink_file(file_id[1], CONCEAL, file_size);
  EXPECT_EQ(logic_block->unlink_file(file_id[1], CONCEAL, file_size), EXIT_FILE_STATUS_ERROR);
  logic_block->unlink_file(new_id, CONCEAL, file_size);
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_EQ(exist.flag_&FI_CONCEAL, FI_CONCEAL);
  // unconceal
  EXPECT_EQ(logic_block->unlink_file(file_id[2], REVEAL, file_size), EXIT_FILE_STATUS_ERROR);
  logic_block->unlink_file(new_id, REVEAL, file_size);
  memset(&exist, 0, sizeof(exist));
  logic_block->read_file_info(new_id, exist);
  EXPECT_EQ(exist.flag_&FI_CONCEAL, 0);
}

// batch_write_data
TEST_F(TestLogicBlock, testRawDataOP)
{
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  logic_block->add_physic_block(main_block);
  logic_block->add_physic_block(ext_block);
  logic_block->add_physic_block(ext_block2);
  logic_block->add_physic_block(ext_block3);

  // 4mb, so 250bits in ext_block
  int32_t length = main_block_length;
  char* buf = new char[length];
  int32_t offset = 0;
  memset(buf, -1, length);
  logic_block->write_raw_data(buf, length, offset);
  // test
  char* read_buf = new char[length];
  logic_block->read_raw_data(read_buf, length, offset);
  // test
  for(int32_t i = 0; i < length; i++)
  {
    EXPECT_EQ(read_buf[i], (char) - 1);
  }
  delete(buf);
  delete(read_buf);
}

// batch_write_meta getmetainfo(rawinfo)
TEST_F(TestLogicBlock, testBatchRAWOP)
{
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  logic_block->add_physic_block(main_block);
  logic_block->add_physic_block(ext_block);
  logic_block->add_physic_block(ext_block2);
  logic_block->add_physic_block(ext_block3);

  // make a list
  uint64_t key = 0;
  logic_block->open_write_file(key); 
  RawMetaVec meta_list;
  meta_list.clear();
  BlockInfo block_info;
  block_info.block_id_ = logical_block_id;
  for(int32_t i = 0; i < 200; i++, key++)
  {
    RawMeta meta;
    meta.set_key(key);
    meta.set_offset(static_cast<int32_t>(key) * sizeof(MetaInfo));
    meta.set_size(16);
    meta_list.push_back(meta);
    block_info.version_++;
    block_info.size_+=16;
    block_info.file_count_++;
    block_info.seq_no_++;
  }
  EXPECT_EQ(logic_block->batch_write_meta(&block_info, &meta_list), 0);
  FILE* read_meta = fopen("index/100", "r");
  fseek(read_meta, sizeof(IndexHeader) + bucket_size * sizeof(int32_t), 0);
  MetaInfo temp;
  for(int32_t i = 0; i < 20; i++)//read file

  {
    memset(&temp, 0, sizeof(MetaInfo));
    fread(reinterpret_cast<char*>(&temp), sizeof(MetaInfo), 1, read_meta);
    EXPECT_EQ(temp.get_file_id(), static_cast<uint64_t>(i + 1));
    EXPECT_EQ(temp.get_offset(), static_cast<int32_t>((i + 1) * sizeof(MetaInfo)));
    EXPECT_EQ(temp.get_size(), 16);
  }
  //read
  RawMetaVec raw_list;
  logic_block->get_meta_infos(raw_list);
  for (RawMetaVecIter mit = raw_list.begin(); mit != raw_list.end(); ++mit)
  {
    EXPECT_EQ(mit->get_offset(), static_cast<int32_t>(mit->get_key() * sizeof(MetaInfo)));
    EXPECT_EQ(mit->get_size(), 16);
  }
}

//batch_write_meta get_meta_info(metainfo) get_sorted_info
TEST_F(TestLogicBlock, testBatchMetaOP) 
{
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  logic_block->add_physic_block(main_block);
  logic_block->add_physic_block(ext_block);
  logic_block->add_physic_block(ext_block2);
  logic_block->add_physic_block(ext_block3);
  // make a list
  uint64_t key = 0;
  logic_block->open_write_file(key);
  RawMetaVec meta_list;
  meta_list.clear();
  BlockInfo block_info;
  block_info.block_id_ = logical_block_id;
  // RawMeta* meta;
  for(int32_t i = 0; i < 200; i++, key++)
  {
    RawMeta meta;
    meta.set_key(key);
    meta.set_offset(static_cast<int32_t>(key) * sizeof(MetaInfo));
    meta.set_size(16);
    meta_list.push_back(meta);
    block_info.version_++;
    block_info.size_+=16;
    block_info.file_count_++;
    block_info.seq_no_++;
  }
  logic_block->batch_write_meta(&block_info, &meta_list);
  FILE* read_meta = fopen("index/100", "r");
  fseek(read_meta, sizeof(IndexHeader) + bucket_size * sizeof(int32_t), 0);
  MetaInfo temp;
  // read file
  for(int32_t i = 0;i < 200; i++)
  {
    memset(&temp, 0, sizeof(MetaInfo));
    fread(reinterpret_cast<char*>(&temp), sizeof(MetaInfo), 1, read_meta);
    EXPECT_EQ(temp.get_file_id(), static_cast<uint64_t>(i + 1));
    EXPECT_EQ(temp.get_offset(), static_cast<int32_t>((i + 1) * sizeof(MetaInfo)));
    EXPECT_EQ(temp.get_size(), 16);
  }
  // get_meta_infos
  meta_list.clear();
  logic_block->get_meta_infos(meta_list);
  for (RawMetaVecIter mit = meta_list.begin(); mit != meta_list.end(); ++mit)
  {
    EXPECT_EQ(mit->get_offset(), static_cast<int32_t>(mit->get_key() * sizeof(MetaInfo)));
    EXPECT_EQ(mit->get_size(), 16);
  }
  // get_sorted_meta_infos
  logic_block->get_sorted_meta_infos(meta_list);
  int32_t j = 0;
  for (RawMetaVecIter mit = meta_list.begin(); mit != meta_list.end(); ++mit)
  {
    j++;
    EXPECT_EQ(mit->get_key(), static_cast<uint64_t>(j));
    EXPECT_EQ(mit->get_offset(), static_cast<int32_t>(j * sizeof(MetaInfo)));
    EXPECT_EQ(mit->get_size(), 16);
  }
  EXPECT_EQ(j, 200);
}


TEST_F(TestLogicBlock, testGetFileInfos)
{
  // insert data files
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  uint64_t file_id[INSERT_FILE_NUM];

  // write file
  memset(file_id, 0, sizeof(file_id));
  int32_t* data = new int32_t[data_length];
  int64_t offset = 0;
  DataFile* data_file = NULL;
  int64_t data_size = data_length * sizeof(int32_t) + sizeof(FileInfo);
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);
  char* tmp_data = new char[MAX_COMPACT_READ_SIZE + 1];
  for(int32_t i = 0; i < INSERT_FILE_NUM; i++)
  {
    // if want to get a new file_id, assign 0 first
    logic_block->open_write_file(file_id[i]);
    // insert 3 big files
    if (5 == i || 200 == i || 500 == i)
    {
      data_file = new DataFile(file_id[i]);
      data_file->set_data(tmp_data, MAX_COMPACT_READ_SIZE + 1, 0);
    }
    else
    {
      for(int32_t j = 0; j < data_length; j++)
      {
        data[j] = j + i * j;
      }
      data_file = new DataFile(file_id[i]);
      data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    }

    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete(data_file);
  }
  delete(tmp_data);
  delete(data);

  // check
  std::vector<FileInfo> file_infos;
  EXPECT_EQ(0, logic_block->get_file_infos(file_infos));
  EXPECT_EQ(INSERT_FILE_NUM, static_cast<int32_t>(file_infos.size()));
  for (int32_t i = 0; i < static_cast<int32_t>(file_infos.size()); i++)
  {
    int32_t j = 0;
    for (j = 0; j < INSERT_FILE_NUM; j++)
    if (file_id[j] == file_infos[i].id_)
    {
      break;
    }
    if (j >= INSERT_FILE_NUM)
    ADD_FAILURE() << "not found file id_:" << file_infos[i].id_ << "(" << i <<")";
    if (5 == i || 200 == i || 500 == i)
    {
      EXPECT_EQ(MAX_COMPACT_READ_SIZE + 1, file_infos[i].size_);
    }
    else
    {
      EXPECT_EQ(data_size - sizeof(FileInfo), static_cast<uint32_t>(file_infos[i].size_));
    }
  }
}

TEST_F(TestLogicBlock, testFileIterator)
{
  // insert data files
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  uint64_t file_id[INSERT_FILE_NUM];
  
  // write file
  memset(file_id, 0, sizeof(file_id));
  int32_t* data = new int32_t[data_length]; 
  int64_t offset = 0;
  DataFile* data_file = NULL;
  int64_t data_size = data_length * sizeof(int32_t) + sizeof(FileInfo);
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);
  char* tmp_data = new char[MAX_COMPACT_READ_SIZE + 1];
  for(int32_t i = 0; i < INSERT_FILE_NUM; i++)
  {
    // if want to get a new file_id, assign 0 first
    logic_block->open_write_file(file_id[i]); 
    // insert 3  big files
    if (5 == i || 200 == i || 500 == i)
    {
      data_file = new DataFile(file_id[i]);
      data_file->set_data(tmp_data, MAX_COMPACT_READ_SIZE + 1, 0);
    }
    else
    {
      for(int32_t j = 0; j < data_length; j++)
      {
        data[j] = j + file_id[i] * j;
      }
      data_file = new DataFile(file_id[i]);
      data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    }

    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete(data_file);
  }
  delete(tmp_data);
  
  // check
  FileIterator* fit = new FileIterator(logic_block);
  for (int32_t i = 0;i < INSERT_FILE_NUM; i++)
  {
    EXPECT_TRUE(fit->has_next());
    EXPECT_EQ(0, fit->next());
    if (5 == i || 200 == i || 500 == i)
    {
      EXPECT_TRUE(fit->is_big_file());
      EXPECT_EQ(file_id[i], fit->current_file_info()->id_);
      EXPECT_EQ(MAX_COMPACT_READ_SIZE + 1, fit->current_file_info()->size_);
      continue;
    }
    // check FileInfo
    const FileInfo* file_info = fit->current_file_info();
    EXPECT_EQ(file_info->size_, static_cast<int32_t>(data_size - sizeof(FileInfo)));
    EXPECT_EQ(file_info->id_, file_id[i]);
    EXPECT_GT(file_info->create_time_, 0);
    // check content
    int32_t read_len = data_size - sizeof(FileInfo);
    EXPECT_EQ(0, fit->read_buffer(reinterpret_cast<char*>(data), read_len));
    for (int32_t j = 0;j < data_length; j++)
    {
      EXPECT_EQ(j + file_info->id_ * j, static_cast<uint32_t>(data[j]));
    }
  }
  EXPECT_FALSE(fit->has_next());

  delete fit;
  delete []data;
}

int write_big_file(LogicBlock* src, LogicBlock* dest, const FileInfo& src_info, const FileInfo& dest_info,
    int64_t woffset)
{
  int32_t src_size = src_info.size_;
  int32_t src_offset = src_info.offset_ + sizeof(FileInfo);
  char* buf = new char[MAX_COMPACT_READ_SIZE];
  int32_t read_len = 0;
  int32_t ret = 0;

  memcpy(buf, &dest_info, sizeof(FileInfo));
  int32_t data_len = sizeof(FileInfo);
  while (read_len < src_size)
  {
    int32_t cur_read = MAX_COMPACT_READ_SIZE - data_len;
    if (cur_read > src_size - read_len)
    {
      cur_read = src_size - read_len;
    }
    ret = src->read_raw_data(buf + data_len, cur_read, src_offset);
    if (ret)
    {
      break;
    }
    data_len += cur_read;
    read_len += cur_read;
    src_offset += cur_read;

    ret = dest->write_raw_data(buf, data_len, woffset);
    if (ret)
    {
      break;
    }
    woffset += data_len;
    data_len = 0;
  }
  delete[] buf;
  if (ret)
  {
    TBSYS_LOG(ERROR, "write big file error, blockid: %u, ret: %u ", dest->get_logic_block_id(), ret);
  }
  return ret;
}

int real_compact(LogicBlock* src, LogicBlock* dest)
{
  assert(src != NULL && dest != NULL);

  BlockInfo dest_blk;
  BlockInfo* src_blk = src->get_block_info();
  dest_blk.block_id_ = src_blk->block_id_;
  dest_blk.seq_no_ = src_blk->seq_no_;
  dest_blk.version_ = src_blk->version_;
  dest_blk.file_count_ = 0;
  dest_blk.size_ = 0;
  dest_blk.del_file_count_ = 0;
  dest_blk.del_size_ = 0;

  dest->set_last_update(time(NULL));
  TBSYS_LOG(DEBUG, "compact block set_last_update. blockid: %u\n", dest->get_logic_block_id());

  char* dest_buf = new char[MAX_COMPACT_READ_SIZE];
  int64_t write_offset = 0, data_len = 0;
  int64_t w_file_offset = 0;
  RawMetaVec dest_metas;
  FileIterator* fit = new FileIterator(src);
  int32_t ret = 0;

  while (fit->has_next())
  {
    ret = fit->next();
    if (ret)
    {
      delete[] dest_buf;
      delete fit;
      return ret;
    }

    const FileInfo* file_info = fit->current_file_info();
    if (file_info->flag_ & (FI_DELETED | FI_INVALID))
    {
      continue;
    }

    FileInfo dfinfo = *file_info;
    dfinfo.offset_ = w_file_offset;
    dfinfo.size_ = file_info->size_ + sizeof(FileInfo);
    dfinfo.usize_ = file_info->size_ + sizeof(FileInfo);
    w_file_offset += dfinfo.size_;

    dest_blk.file_count_++;
    dest_blk.size_ += dfinfo.size_;

    RawMeta tmp_meta;
    tmp_meta.set_file_id(dfinfo.id_);
    tmp_meta.set_size(dfinfo.size_);
    tmp_meta.set_offset(dfinfo.offset_);
    dest_metas.push_back(tmp_meta);

    // need flush write buffer
    if ((data_len != 0) && (fit->is_big_file() || data_len + dfinfo.size_ > MAX_COMPACT_READ_SIZE))
    {
      TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %" PRI64_PREFIX "d\n", dest->get_logic_block_id(), write_offset);
      ret = dest->write_raw_data(dest_buf, data_len, write_offset);
      if (ret)
      {
        TBSYS_LOG(ERROR, "batch write data fail, blockid: %u, offset %" PRI64_PREFIX "d, readinglen: %" PRI64_PREFIX "d, ret :%d", dest->get_logic_block_id(), write_offset, data_len, ret);
        delete[] dest_buf;
        delete fit;
        return ret;
      }
      write_offset += data_len;
      data_len = 0;
    }

    if (fit->is_big_file())
    {
      ret = write_big_file(src, dest, *file_info, dfinfo, write_offset);
      write_offset += dfinfo.size_;
    }
    else
    {
      memcpy(dest_buf + data_len, &dfinfo, sizeof(FileInfo));
      int32_t left_len = MAX_COMPACT_READ_SIZE - data_len - sizeof(FileInfo);
      ret = fit->read_buffer(dest_buf + data_len + sizeof(FileInfo), left_len);
      data_len += dfinfo.size_;
    }
    if (ret)
    {
      delete[] dest_buf;
      delete fit;
      return ret;
    }
  } 
  
  // flush the last buffer
  if (data_len != 0) 
  {
    TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %" PRI64_PREFIX "d\n", dest->get_logic_block_id(), write_offset);
    ret = dest->write_raw_data(dest_buf, data_len, write_offset);
    if (ret)
    {
      TBSYS_LOG(ERROR, "batch write data fail, blockid: %u, offset %" PRI64_PREFIX "d, readinglen: %" PRI64_PREFIX "d, ret :%d", dest->get_logic_block_id(), write_offset, data_len, ret);
      delete[] dest_buf;
      delete fit;
      return ret;
    }
  }

  delete[] dest_buf;
  delete fit;
  TBSYS_LOG(DEBUG, "compact write complete. blockid: %u\n", dest->get_logic_block_id());

  ret = dest->batch_write_meta(&dest_blk, &dest_metas);
  if (ret)
  {
    TBSYS_LOG(ERROR, "compact write segment meta failed. blockid: %u, meta size %d\n", dest->get_logic_block_id(),
        dest_metas.size());
    return ret;
  }

  TBSYS_LOG(DEBUG, "compact set dirty flag. blockid: %u\n", dest->get_logic_block_id());
  ret = dest->set_block_dirty_type(C_DATA_CLEAN);
  if (ret)
  {
    TBSYS_LOG(ERROR, "compact blockid: %u set dirty flag fail. ret: %d\n", dest->get_logic_block_id(), ret);
    return ret;
  }
  return 0;
}

TEST_F(TestLogicBlock,testRealCompact)
{
  // insert data files
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  uint64_t file_id[INSERT_FILE_NUM];

  // write file
  memset(file_id, 0, sizeof(file_id));
  int32_t* data = new int32_t[data_length];
  int64_t offset = 0;
  DataFile* data_file = NULL;
  int64_t data_size = data_length * sizeof(int32_t) + sizeof(FileInfo);
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);
  char* tmp_data = new char[MAX_COMPACT_READ_SIZE + 1];
  for(int32_t i = 0; i < INSERT_FILE_NUM; i++)
  {
    //if want to get a new file_id, assign 0 first
    logic_block->open_write_file(file_id[i]); 
    if (i == 5 || i == 200 || i == 500) // insert 3  big files
    {
      data_file = new DataFile(file_id[i]);
      data_file->set_data(tmp_data, MAX_COMPACT_READ_SIZE+1, 0);
    }
    else
    {
      for(int32_t j = 0; j < data_length; j++)
      {
        data[j] = j + file_id[i] * j;
      }
      data_file = new DataFile(file_id[i]);
      data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    }

    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete(data_file);
  }

  delete(tmp_data);

  dest_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  m2->set_block_prefix(logical_block_id + 1, 0, 0);
  dest_block->add_physic_block(m2);

  // delete even ones
  int64_t file_size;
  for (int32_t i = 0; i < INSERT_FILE_NUM; i += 2)
  {
    EXPECT_EQ(0, logic_block->unlink_file(file_id[i], DELETE, file_size));
  }
  // compact
  CompactBlock compact;
  EXPECT_EQ(0, compact.real_compact(logic_block, dest_block));

  // check
  FileIterator* fit = new FileIterator(dest_block);
  for (int32_t i = 1; i < INSERT_FILE_NUM; i += 2)
  {
    EXPECT_TRUE(fit->has_next());
    EXPECT_EQ(0, fit -> next()) << "i:" << i;
    if (i == 5 || i == 200 || i == 500)
    {
      EXPECT_TRUE(fit->is_big_file());
      EXPECT_EQ(file_id[i], fit->current_file_info()->id_);
      EXPECT_EQ(MAX_COMPACT_READ_SIZE + 1, fit->current_file_info()->size_);
      continue;
    }
    // check FileInfo
    const FileInfo* file_info = fit->current_file_info();
    EXPECT_EQ(file_info->size_, static_cast<int32_t>(data_size - sizeof(FileInfo))); 
    EXPECT_EQ(file_info->id_, file_id[i]);
    EXPECT_GT(file_info->create_time_, 0);
    // check content
    int32_t read_len = data_size - sizeof(FileInfo);
    EXPECT_EQ(0, fit->read_buffer(reinterpret_cast<char*>(data), read_len));
    for (int32_t j=0;j < data_length;j++)
    {
      EXPECT_EQ(j + file_info->id_ * j, static_cast<uint32_t>(data[j]));
    }
  }
  EXPECT_FALSE(fit->has_next());
  delete fit;
  delete []data;
}

TEST_F(TestLogicBlock, testComplexCompact)
{
  // insert data files
  int32_t bucket_size = 16;
  logic_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  uint64_t file_id[INSERT_FILE_NUM];
  //write file
  memset(file_id, 0, sizeof(file_id));
  int32_t* data = new int32_t[data_length]; //writedata
  int64_t offset = 0;
  DataFile* data_file;
  int64_t data_size = data_length * sizeof(int32_t) + sizeof(FileInfo);
  main_block->set_block_prefix(logical_block_id, 0, 0);
  logic_block->add_physic_block(main_block);
  char* tmp_data = new char[MAX_COMPACT_READ_SIZE + 1];
  // 1) insert 1000 files
  for(int32_t i = 0; i < INSERT_FILE_NUM; i++)
  {
    //if want to get a new file_id, assign 0 first
    logic_block->open_write_file(file_id[i]); 

      // insert 3  big files
      if (5 == i || 200 == i || 500 == i)
    {
      data_file = new DataFile(file_id[i]);
      data_file->set_data(tmp_data, MAX_COMPACT_READ_SIZE + 1, 0);
    }
    else
    {
      for(int32_t j = 0; j < data_length; j++)
      {
        data[j] = j + file_id[i] * j;
      }
      data_file = new DataFile(file_id[i]);
      data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    }

    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete(data_file);
  }

  // 2) delete even(500) ones
  int64_t file_size;
  for (int32_t i = 0; i < INSERT_FILE_NUM; i += 2)
  {
    EXPECT_EQ(0, logic_block->unlink_file(file_id[i], DELETE, file_size));
  }
  for (int32_t i = 1; i < INSERT_FILE_NUM; i += 2)
  {
    file_id[i / 2] = file_id[i];
  }
  // 3) insert another 500 files
  for (int32_t i = INSERT_FILE_NUM / 2; i < INSERT_FILE_NUM; i++)
  {
    file_id[i] = 0;
    EXPECT_EQ(0, logic_block->open_write_file(file_id[i]));
    for (int32_t j = 0; j < data_length; j++)
    {
      data[j] = j + file_id[i] * j;
    }
    data_file = new DataFile(file_id[i]);
    data_file->set_data(reinterpret_cast<char*>(data), data_size - sizeof(FileInfo), offset);
    uint32_t crc = data_file->get_crc();
    EXPECT_EQ(0, logic_block->close_write_file(file_id[i], data_file, crc));
    delete data_file;
  }
  // 4) delete 3x ones
  for (int32_t i = 0; i < INSERT_FILE_NUM; i += 3)
  {
    EXPECT_EQ(0, logic_block->unlink_file(file_id[i], DELETE, file_size));
  }
  
  delete(tmp_data);
  dest_block->init_block_file(bucket_size, mmap_option, C_MAIN_BLOCK);
  m2->set_block_prefix(logical_block_id + 1, 0, 0);
  dest_block->add_physic_block(m2);

  // compact
  CompactBlock compact;
  EXPECT_EQ(0, compact.real_compact(logic_block, dest_block));

  // check
  FileIterator* fit = new FileIterator(dest_block);
  for (int32_t i = 0; i < INSERT_FILE_NUM; i++)
  {
    if (0 == i % 3)
    {
      continue;
    }

    EXPECT_TRUE(fit->has_next());
    EXPECT_EQ(0, fit->next()) << "i:" << i;
    if (2 == i)
    {
      EXPECT_TRUE(fit->is_big_file());
      EXPECT_EQ(file_id[i], fit->current_file_info()->id_);
      EXPECT_EQ(MAX_COMPACT_READ_SIZE + 1, fit->current_file_info()->size_);
      continue;
    }
    // check FileInfo
    const FileInfo* file_info = fit->current_file_info();
    EXPECT_EQ(file_info->size_, static_cast<int32_t>(data_size - sizeof(FileInfo))) << "i:" << i;
    EXPECT_EQ(file_info->id_, file_id[i]);
    EXPECT_GT(file_info->create_time_, 0);
    // check content
    int32_t read_len = data_size - sizeof(FileInfo);
    EXPECT_EQ(0, fit->read_buffer(reinterpret_cast<char*>(data), read_len));
    for (int32_t j = 0;j < data_length; j++)
    {
      EXPECT_EQ(j + file_info->id_ * j, static_cast<uint32_t>(data[j]));
    }
  }
  EXPECT_FALSE(fit->has_next());
  delete fit;

  delete []data;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
