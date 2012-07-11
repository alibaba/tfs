/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_index_handle.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <tbsys.h>
#include "index_handle.h"
#include "common/error_msg.h"

using namespace tfs::dataserver;
using namespace tfs::common;
    
const char* TEST_FILE_NAME = "test";
const char* TEST_NONEMPTY = "test1";
const char* TEST_BAD_FILE = "bad_test";

class TestIndexHandle : public ::testing::Test
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
    virtual void SetUp()
    {
      FILE* fp;
      if ((fp=fopen(TEST_FILE_NAME, "w+"))==NULL)
      {
        exit(0);
      }
      fclose(fp);

      op.max_mmap_size_ = 1024;
      op.first_mmap_size_ = 1024;
      op.per_mmap_size_ = 1024;

      // create a mock block file(having one meta info node)
      memset(handle_buf, 0, sizeof(handle_buf));
      IndexHeader* index_handle = (IndexHeader*)handle_buf;
      index_handle->block_info_.block_id_ = 1;
      index_handle->block_info_.seq_no_ = 2;
      index_handle->block_info_.version_ = 5;
      index_handle->index_file_size_ = sizeof(IndexHeader) + 16 * sizeof(int32_t);
      index_handle->bucket_size_ = 16;
      index_handle->flag_ = C_DATA_CLEAN;

      int32_t tmp_off = sizeof(IndexHeader) + 16 * sizeof(int32_t);
      MetaInfo* pmi = (MetaInfo*)(handle_buf + tmp_off);
      pmi->set_file_id(1);
      pmi->set_offset(10);
      pmi->set_size(10);

      int32_t* bucket = reinterpret_cast<int32_t*>(handle_buf + sizeof(IndexHeader));
      // according to fileid, the bucket should be 1
      bucket[1] = tmp_off;

      if ((fp=fopen(TEST_NONEMPTY, "w+"))==NULL)
      {
        exit(0);
      }
      fwrite(handle_buf, 4096, 1, fp);
      fclose(fp);

      empty = new IndexHandle(new MMapFileOperation(TEST_FILE_NAME));
      handle = new IndexHandle(new MMapFileOperation(TEST_NONEMPTY));

    }

    virtual void TearDown()
    {
      unlink(TEST_FILE_NAME);
      unlink(TEST_NONEMPTY);
      delete empty;
      delete handle;
    }

  protected:
    IndexHandle *empty, *handle;
    char empty_buf[4096];
    char handle_buf[4096];
    MMapOption op;
};

TEST_F(TestIndexHandle, testCreate)
{
  EXPECT_EQ(0, empty->create(1, 16, op, C_DATA_CLEAN));

  FILE* fp = fopen(TEST_FILE_NAME, "r");
  fread(empty_buf, 4096, 1, fp);
  fclose(fp);

  IndexHeader* index_handle = (IndexHeader*)empty_buf;
  EXPECT_EQ(1, static_cast<int32_t>(index_handle->block_info_.block_id_));
  EXPECT_EQ(1, static_cast<int32_t>(index_handle->block_info_.seq_no_));
  EXPECT_EQ(static_cast<int32_t>(sizeof(IndexHeader) + 16 * sizeof(int32_t)), index_handle->index_file_size_);
  EXPECT_EQ(16, index_handle->bucket_size_);
  EXPECT_EQ(C_DATA_CLEAN, index_handle->flag_);

  int32_t* bucket = reinterpret_cast<int32_t*>(empty_buf + sizeof(IndexHeader));
  for (int32_t i=0;i<16;i++)
  {
    EXPECT_EQ(0, bucket[i]);
  }

  // create nonexists file
  IndexHandle badfile(new MMapFileOperation(TEST_BAD_FILE));
  EXPECT_NE(badfile.create(1, 16, op, C_DATA_CLEAN), 0);

  // create nonempty file
  IndexHandle noempty(new MMapFileOperation(TEST_NONEMPTY));
  EXPECT_LT(noempty.create(1, 26, op, C_DATA_CLEAN), 0);
}

TEST_F(TestIndexHandle, testLoad)
{
  EXPECT_EQ(0, handle->load(1, 16, op));

  FILE* fp = fopen(TEST_NONEMPTY, "r");
  fread(handle_buf, 4096, 1, fp);
  fclose(fp);

  IndexHeader* index_handle = (IndexHeader*)handle_buf;
  EXPECT_EQ(1, static_cast<int32_t>(index_handle->block_info_.block_id_));
  EXPECT_EQ(2, static_cast<int32_t>(index_handle->block_info_.seq_no_));
  EXPECT_EQ(static_cast<int32_t>(sizeof(IndexHeader) + 16 * sizeof(int32_t)), index_handle->index_file_size_);
  EXPECT_EQ(16, index_handle->bucket_size_);
  EXPECT_EQ(C_DATA_CLEAN, index_handle->flag_);

  int32_t* bucket = reinterpret_cast<int32_t*>(handle_buf + sizeof(IndexHeader));
  EXPECT_GT(bucket[1], 0);
  EXPECT_EQ(0, bucket[0]);
  for (int32_t i = 2; i < 16; i++)
  {
    EXPECT_EQ(0, bucket[i]);
  }

  // load file with wrong properties
  IndexHandle wpro(new MMapFileOperation(TEST_FILE_NAME));
  EXPECT_LT(wpro.load(0, 16, op), 0);
  EXPECT_LT(wpro.load(1, 16, op), 0);

  // load bad file
  IndexHandle bfile(new MMapFileOperation(TEST_BAD_FILE));
  EXPECT_LT(bfile.load(1, 16, op), 0);
}

TEST_F(TestIndexHandle, testFindAvailKey)
{
  // test empty file
  EXPECT_EQ(0, empty->create(1, 16, op, C_DATA_CLEAN));
  uint64_t key = 0; // must be initialized
  EXPECT_EQ(0, empty->find_avail_key(key));
  EXPECT_EQ((uint64_t)1, key);
  for (int32_t i = 2; i < 100; i++)
  {
    key = 0;
    EXPECT_EQ(0, empty->find_avail_key(key));
    EXPECT_EQ((uint64_t)i, key);
  }
  key = 200;
  EXPECT_EQ(0, empty->find_avail_key(key));
  EXPECT_EQ((uint64_t)200, key);

  // test nonempty file
  EXPECT_EQ(0, handle->load(1, 16, op));
  key = 0;
  EXPECT_EQ(0, handle->find_avail_key(key));
  EXPECT_EQ((uint64_t)2, key);
  key = 200;
  EXPECT_EQ(0, handle->find_avail_key(key));
  EXPECT_EQ((uint64_t)200, key);
}

TEST_F(TestIndexHandle, testCheckBlockVersion)
{
  EXPECT_EQ(0, handle->load(1, 16, op));
  EXPECT_EQ(5, handle->index_header()->block_info_.version_); // init version is 5

  int32_t remote_version = 5;
  // same
  EXPECT_EQ(0, handle->check_block_version(remote_version));

  // illegal
  remote_version = 8;
  EXPECT_LT(handle->check_block_version(remote_version), 0);
  remote_version = 2;
  EXPECT_LT(handle->check_block_version(remote_version), 0);

  // local version modify
  remote_version = 6;
  EXPECT_EQ(0, handle->check_block_version(remote_version));
  EXPECT_EQ(static_cast<int32_t>(6), handle->index_header()->block_info_.version_);

  // remote version modify
  remote_version = 4; // current local version is 6
  EXPECT_EQ(0, handle->check_block_version(remote_version));
  EXPECT_EQ(static_cast<int32_t>(6), remote_version);
}

TEST_F(TestIndexHandle, testSimpleReadWrite)
{
  // load and read
  EXPECT_EQ(0, handle->load(1, 16, op));
  uint64_t key;
  RawMeta raw_meta;
  // read nonexists
  key = 2;
  EXPECT_LT(handle->read_segment_meta(key, raw_meta), 0);
  // read exists
  key = 1;
  EXPECT_EQ(0, handle->read_segment_meta(key, raw_meta));
  EXPECT_EQ((uint64_t)1, raw_meta.get_file_id());
  EXPECT_EQ(10, raw_meta.get_offset());
  EXPECT_EQ(10, raw_meta.get_size());
  // write exists
  key = 1;
  EXPECT_LT(handle->write_segment_meta(key, raw_meta), 0);
  // write and read
  EXPECT_EQ(0, handle->find_avail_key(key));
  raw_meta.set_file_id(key);
  raw_meta.set_offset(20);
  raw_meta.set_size(20);
  //raw_meta.set_next_meta_offset(0);
  EXPECT_EQ(0, handle->write_segment_meta(key, raw_meta));
  RawMeta tmp_meta;
  EXPECT_EQ(0, handle->read_segment_meta(key, tmp_meta));
  EXPECT_EQ(20, tmp_meta.get_offset());
  EXPECT_EQ(20, tmp_meta.get_size());
}

TEST_F(TestIndexHandle, testOverrideSegmentMeta)
{
  RawMeta override, newin;
  uint32_t key;
  EXPECT_EQ(0, handle->load(1, 16, op));
  // insert
  key = 2;
  newin.set_file_id(2);
  newin.set_offset(20);
  newin.set_size(20);
  EXPECT_EQ(0, handle->override_segment_meta(key, newin));
  // update
  key = 1;
  override.set_file_id(1);
  override.set_offset(30);
  override.set_size(30);
  EXPECT_EQ(0, handle->override_segment_meta(key, override));
  // check
  RawMeta tmp_meta;
  EXPECT_EQ(0, handle->read_segment_meta(1, tmp_meta));
  EXPECT_EQ(30, tmp_meta.get_offset());
  EXPECT_EQ(30, tmp_meta.get_size());
  EXPECT_EQ(0, handle->read_segment_meta(2, tmp_meta));
  EXPECT_EQ(20, tmp_meta.get_offset());
  EXPECT_EQ(20, tmp_meta.get_size());

  // insert 100 data
  for (int32_t i = 3; i <= 100; i++)
  {
    tmp_meta.set_file_id(i);
    tmp_meta.set_offset(i * 10);
    tmp_meta.set_size(i * 10);
    EXPECT_EQ(0, handle->write_segment_meta(i, tmp_meta));
  }
  // override 50(51-100) and insert 50(100-150) nodes
  for (int32_t i = 51; i <= 150; i++)
  {
    tmp_meta.set_file_id(i);
    tmp_meta.set_offset(i * 100); // change the value(*100)
    tmp_meta.set_size(i * 100);
    EXPECT_EQ(0, handle->override_segment_meta(i, tmp_meta));
  }
  // check
  for (int32_t i = 51; i <= 150; i++)
  {
    EXPECT_EQ(0, handle->read_segment_meta(i, tmp_meta));
    EXPECT_EQ(i * 100, tmp_meta.get_offset());
  }
}

TEST_F(TestIndexHandle, testBatchOverrideSegmentMeta)
{
  EXPECT_EQ(0, handle->load(1, 16, op));
  // insert 100 data
  int32_t item_number = 100;
  RawMetaVec meta_infos;
  for (int32_t i = 0; i <= item_number; ++i)
  {
    RawMeta meta_info;
    meta_info.set_file_id(i);
    meta_info.set_offset(i * 10);
    meta_info.set_size(i * 100);
    meta_infos.push_back(meta_info);
  }
  EXPECT_EQ(0, handle->batch_override_segment_meta(meta_infos));

  RawMetaVec modify_meta_infos;
  // override 50(51-100) and insert 50(100-150) nodes
  for (int32_t i = 51; i <= 150; ++i)
  {
    RawMeta meta_info;
    meta_info.set_file_id(i);
    meta_info.set_offset(i * 100); // change the value(*100)
    meta_info.set_size(i * 10);
    modify_meta_infos.push_back(meta_info);
  }
  EXPECT_EQ(0, handle->batch_override_segment_meta(modify_meta_infos));

  // check
  for (int32_t i = 0; i <= 50; ++i)
  {
    RawMeta meta_info;
    EXPECT_EQ(0, handle->read_segment_meta(i, meta_info));
    EXPECT_EQ(i * 10, meta_info.get_offset());
    EXPECT_EQ(i * 100, meta_info.get_size());
  }
  for (int32_t i = 51; i <= 150; i++)
  {
    RawMeta meta_info;
    EXPECT_EQ(0, handle->read_segment_meta(i, meta_info));
    EXPECT_EQ(i * 100, meta_info.get_offset());
    EXPECT_EQ(i * 10, meta_info.get_size());
  }
}

TEST_F(TestIndexHandle, testBatchOverrideSegmentRawMeta)
{
  EXPECT_EQ(0, handle->load(1, 16, op));

  {
    //test batch modify single
    RawMetaVec meta_infos;
    for (int32_t i = 1; i < 2; ++i)
    {
      RawMeta meta_info;
      meta_info.set_file_id(i);
      meta_info.set_offset(i * 10);
      meta_info.set_size(i * 100);
      meta_infos.push_back(meta_info);
    }
    EXPECT_EQ(0, handle->batch_override_segment_meta(meta_infos));

    for (int32_t i = 1; i < 2; ++i)
    {
      RawMeta meta_info;
      EXPECT_EQ(0, handle->read_segment_meta(i, meta_info));
      EXPECT_EQ(i * 10, meta_info.get_offset());
      EXPECT_EQ(i * 100, meta_info.get_size());
    }
  }
  // insert 100 data
  int32_t item_number = 100;
  RawMetaVec meta_infos;
  for (int32_t i = 0; i <= item_number; ++i)
  {
    RawMeta meta_info;
    meta_info.set_file_id(i);
    meta_info.set_offset(i * 10);
    meta_info.set_size(i * 100);
    meta_infos.push_back(meta_info);
  }
  EXPECT_EQ(0, handle->batch_override_segment_meta(meta_infos));

  RawMetaVec modify_meta_infos;
  // override 50(51-100) and insert 50(100-150) nodes
  for (int32_t i = 51; i <= 150; ++i)
  {
    RawMeta meta_info;
    meta_info.set_file_id(i);
    meta_info.set_offset(i * 100); // change the value(*100)
    meta_info.set_size(i * 10);
    modify_meta_infos.push_back(meta_info);
  }
  EXPECT_EQ(0, handle->batch_override_segment_meta(modify_meta_infos));

  // check
  for (int32_t i = 0; i <= 50; ++i)
  {
    RawMeta meta_info;
    EXPECT_EQ(0, handle->read_segment_meta(i, meta_info));
    EXPECT_EQ(i * 10, meta_info.get_offset());
    EXPECT_EQ(i * 100, meta_info.get_size());
  }
  for (int32_t i = 51; i <= 150; i++)
  {
    RawMeta meta_info;
    EXPECT_EQ(0, handle->read_segment_meta(i, meta_info));
    EXPECT_EQ(i * 100, meta_info.get_offset());
    EXPECT_EQ(i * 10, meta_info.get_size());
  }
}

TEST_F(TestIndexHandle, testUpdateSegmentMeta)
{
  EXPECT_EQ(0, handle->load(1, 16, op));
  RawMeta tmp_meta;
  EXPECT_EQ(0, handle->read_segment_meta(1, tmp_meta));
  EXPECT_EQ(10, tmp_meta.get_offset());
  // update
  tmp_meta.set_offset(20);
  tmp_meta.set_size(20);
  EXPECT_EQ(0, handle->update_segment_meta(1, tmp_meta));
  // check
  RawMeta tmpm1;
  EXPECT_EQ(0, handle->read_segment_meta(1, tmpm1));
  EXPECT_EQ(20, tmpm1.get_offset());

  // update nonexists
  EXPECT_LT(handle->update_segment_meta(5, tmp_meta), 0);

  // insert 100 data
  for (int32_t i = 2; i <= 101; i++)
  {
    tmp_meta.set_file_id(i);
    tmp_meta.set_offset(i * 10);
    tmp_meta.set_size(i * 10);
    //tmp_meta.set_next_meta_offset(0);
    EXPECT_EQ(0, handle->write_segment_meta(i, tmp_meta));
  }
  // update them
  for (int32_t i = 2; i < 101; i++)
  {
    tmp_meta.set_file_id(i);
    tmp_meta.set_offset(i * 100);
    EXPECT_EQ(0, handle->update_segment_meta(i, tmp_meta));
  }
  // check
  for (int32_t i = 2; i < 101; i++)
  {
    EXPECT_EQ(0, handle->read_segment_meta(i, tmpm1));
    EXPECT_EQ(i * 100, tmpm1.get_offset());
  }
  // last one(101) is not updated
  EXPECT_EQ(0, handle->read_segment_meta(101, tmpm1));
  EXPECT_EQ(1010, tmpm1.get_offset());
}

TEST_F(TestIndexHandle, testDeleteSegmentMeta)
{
  // load and delete
  EXPECT_EQ(0, handle->load(1, 16, op));
  EXPECT_EQ(0, handle->delete_segment_meta(1));
  RawMeta tmp_meta;
  EXPECT_LT(handle->read_segment_meta(1, tmp_meta), 0);
  int32_t* buckets = handle->bucket_slot();
  // bucket slot is cleared
  EXPECT_EQ(0, buckets[0]);
  // delete nonexists
  EXPECT_LT(handle->delete_segment_meta(2), 0);

  // insert 100 data
  for (int32_t i = 1; i <= 100; i++)
  {
    tmp_meta.set_file_id(i);
    tmp_meta.set_offset(i * 10);
    tmp_meta.set_size(i * 10);
    EXPECT_EQ(0, handle->write_segment_meta(i, tmp_meta));
  }
  // delete 2*i elements
  for (int32_t i = 2; i <= 100; i += 2)
  {
    EXPECT_EQ(0, handle->delete_segment_meta(i));
  }
  // check
  for (int32_t i = 2; i <= 100; i += 2)
  {
    EXPECT_LT(handle->read_segment_meta(i, tmp_meta), 0);
  }
  // check free list, read direct from file
  FILE* fp;
  if ((fp=fopen(TEST_NONEMPTY, "r"))==NULL)
  {
    ADD_FAILURE();
  }
  IndexHeader header;
  fread(&header, sizeof(IndexHeader), 1, fp);
  int32_t free_list = header.free_head_offset_;
  int32_t i = 0;
  MetaInfo free_meta;
  while (free_list != 0)
  {
    // read free list node
    fseek(fp, free_list, SEEK_SET);
    fread(&free_meta, sizeof(MetaInfo), 1, fp);
    free_list = free_meta.get_next_meta_offset();
    i++;
  }
  EXPECT_EQ(50, i); // 50 elems in free list
}

TEST_F(TestIndexHandle, testMetaRW)
{
  uint32_t logic_block_id =101;
  int32_t bucket_size=15;
  MMapOption map_option;
  map_option.max_mmap_size_ = 1024;
  map_option.first_mmap_size_ = 1024;
  map_option.per_mmap_size_ = 1024;
  empty->create(logic_block_id, bucket_size, map_option, C_DATA_CLEAN);
  uint64_t keylist[1000];     //the array of the key,cannot be 0
  memset(keylist, 0, sizeof(keylist));
  // write 1000 data continuely
  for (int32_t i = 0; i < 1000; i++)
  {
    RawMeta raw_meta;
    empty->find_avail_key(keylist[i]);
    raw_meta.set_key(keylist[i]);
    empty->write_segment_meta(keylist[i], raw_meta);
  }
  // read them 
  for(int32_t i = 0; i < 1000; i++)
  {
    RawMeta raw_meta;
    empty->read_segment_meta(keylist[i], raw_meta);
    EXPECT_EQ(raw_meta.get_key(), keylist[i]); 
  }
  //del random metainfo
  uint64_t del_elem[1000];
  int32_t deled = 0;
  int32_t p = 0;
  for(int32_t i = 0; i < 100; i++)
  {
    p=(p+random()) % 1000;
    if(keylist[p] != 0)
    {
      empty->delete_segment_meta(keylist[p]);

      RawMeta raw_meta;
      EXPECT_EQ(empty->read_segment_meta(keylist[p], raw_meta), EXIT_META_NOT_FOUND_ERROR);    //read one that is deleted
      del_elem[deled++] = keylist[p];
      keylist[p] = 0;
    }
  }
  // read left element
  for (int32_t i = 0; i < 1000; i++)
  {
    if(keylist[i]!=0)
    {
      RawMeta raw_meta;
      empty->read_segment_meta(keylist[i], raw_meta);
      EXPECT_EQ(raw_meta.get_key(), keylist[i]);
    }
  }  
  // reinsert deled key
  for (int32_t i = 0; i < deled; i++)
  {
    uint64_t key;
    RawMeta tmp_meta;
    key = del_elem[i];
    EXPECT_EQ(0, empty->find_avail_key(key));
    EXPECT_EQ(del_elem[i], key);
    tmp_meta.set_file_id(key);
    tmp_meta.set_offset(key * 100);
    tmp_meta.set_size(key * 100);
    //tmp_meta.set_next_meta_offset(0);
    EXPECT_EQ(0, empty->write_segment_meta(key, tmp_meta));
  }
  // check reinsert elems
  for (int32_t i = 0; i < deled; i++)
  {
    RawMeta tmp_meta;
    EXPECT_EQ(0, empty->read_segment_meta(del_elem[i], tmp_meta));
    EXPECT_EQ(static_cast<int32_t>(del_elem[i] * 100), tmp_meta.get_size());
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
