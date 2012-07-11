/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_ns_cache.cpp 158 2011-02-24 02:11:27Z zongdai@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "new_client/tfs_session.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"
#include "new_client/tair_cache_helper.h"

using namespace tfs::client;
using namespace tfs::common;

TfsClientImpl *g_tfsClientImpl = TfsClientImpl::Instance();
static int BUF_LEN = 1024;

class NsCacheTest: public ::testing::Test
{
  public:

  public:
    NsCacheTest()
    {
    }
    ~NsCacheTest()
    {
    }
    virtual void SetUp()
    {
      strcpy(ns_addr, "10.232.35.41:3100");
      test_clear_local(ns_addr);
      test_clear_remote(ns_addr);

      int ret = g_tfsClientImpl->initialize(ns_addr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, true);
      ASSERT_EQ(TFS_SUCCESS, ret);

      g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_LOCAL | USE_CACHE_FLAG_REMOTE);
    }

    virtual void TearDown()
    {
    }

    void test_local_exist(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      BlockCache* b_res = block_cache_map->find(blockid);
      ASSERT_NE(static_cast<void*>(b_res), static_cast<void*>(NULL));
    }

    void test_local_exist(const char* ns_addr, uint32_t blockid, VUINT64 ds)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      BlockCache* b_res = block_cache_map->find(blockid);
      ASSERT_NE(static_cast<void*>(b_res), static_cast<void*>(NULL));
      size_t cache_ds_count = b_res->ds_.size();
      ASSERT_EQ(cache_ds_count, ds.size());
      for (size_t i = 0; i < cache_ds_count; i++)
      {
        ASSERT_EQ(b_res->ds_[i], ds[i]);
      }
    }

    void test_local_not_exist(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      BlockCache* b_res = block_cache_map->find(blockid);
      ASSERT_EQ(static_cast<void*>(b_res), static_cast<void*>(NULL));
    }

    void test_insert_local(const char* ns_addr, uint32_t blockid, VUINT64& ds)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      BlockCache block_cache;
      block_cache.last_time_ = time(NULL);
      block_cache.ds_ = ds;
      int32_t old_size = block_cache_map->size();
      block_cache_map->insert(blockid, block_cache);
      ASSERT_EQ(block_cache_map->size(), old_size + 1);
    }

    void test_remove_local(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      block_cache_map->remove(blockid);
    }

    void test_clear_local(const char* ns_addr)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TfsSession::BLOCK_CACHE_MAP* block_cache_map = tfs_session->get_block_cache_map();
      ASSERT_NE(static_cast<void*>(block_cache_map), static_cast<void*>(NULL));
      block_cache_map->clear();
    }

    void test_remote_not_init(const char* ns_addr)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_EQ(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
    }

    void test_remote_exist(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_NE(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
      std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
      BlockCacheKey key;
      key.set_key(tfs_session->ns_addr_, blockid);
      std::map<BlockCacheKey, BlockCacheValue>::iterator iter = remote_block_cache.find(key);
      bool b_res = remote_block_cache.end() != iter ? true : false;
      ASSERT_EQ(b_res, true);
    }

    void test_remote_exist(const char* ns_addr, uint32_t blockid, VUINT64 ds)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_NE(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
      std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
      BlockCacheKey key;
      key.set_key(tfs_session->ns_addr_, blockid);
      std::map<BlockCacheKey, BlockCacheValue>::iterator iter = remote_block_cache.find(key);
      bool b_res = remote_block_cache.end() != iter ? true : false;
      ASSERT_EQ(b_res, true);
      size_t cache_ds_count = iter->second.ds_.size();
      ASSERT_EQ(cache_ds_count, ds.size());
      for (size_t i = 0; i < cache_ds_count; i++)
      {
        ASSERT_EQ(iter->second.ds_[i], ds[i]);
      }
    }

    void test_remote_not_exist(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_NE(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
      std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
      BlockCacheKey key;
      key.set_key(tfs_session->ns_addr_, blockid);
      std::map<BlockCacheKey, BlockCacheValue>::iterator iter = remote_block_cache.find(key);
      bool b_res = remote_block_cache.end() != iter ? true : false;
      ASSERT_EQ(b_res, 0);
    }

    void test_insert_remote(const char* ns_addr, uint32_t blockid, VUINT64& ds)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_NE(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
      std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
      BlockCacheKey key;
      BlockCacheValue value;
      key.set_key(tfs_session->ns_addr_, blockid);
      value.ds_ = ds;
      uint32_t old_size = remote_block_cache.size();
      remote_block_cache.insert(std::map<BlockCacheKey, BlockCacheValue>::value_type(key, value));
      ASSERT_EQ(remote_block_cache.size(), old_size + 1);
    }

    void test_remove_remote(const char* ns_addr, uint32_t blockid)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      ASSERT_NE(static_cast<void*>(remote_cache_helper), static_cast<void*>(NULL));
      std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
      BlockCacheKey key;
      key.set_key(tfs_session->ns_addr_, blockid);
      remote_block_cache.erase(key);
    }

    void test_clear_remote(const char* ns_addr)
    {
      TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
      TairCacheHelper* remote_cache_helper = tfs_session->get_remote_cache_helper();
      if (NULL != remote_cache_helper)
      {
        std::map<BlockCacheKey, BlockCacheValue>& remote_block_cache = remote_cache_helper->remote_block_cache_;
        remote_block_cache.clear();
      }
    }

  public:
    char ns_addr[30];
};

TEST_F(NsCacheTest, test_write)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 101;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // write
  int fd = g_tfsClientImpl->open(NULL, NULL, NULL, T_WRITE);
  ASSERT_GT(fd, 0);
  char *buf = "hello world";
  int64_t count = strlen(buf);
  ret = g_tfsClientImpl->write(fd, buf, count);
  ASSERT_EQ(count, ret);
  char tfs_name[TFS_FILE_LEN];
  ret = g_tfsClientImpl->close(fd, tfs_name, TFS_FILE_LEN);
  ASSERT_EQ(TFS_SUCCESS, ret);
  TBSYS_LOG(DEBUG, "write file: (%s) success!", tfs_name);

  FSName fsname;
  fsname.set_name(tfs_name, NULL);
  ASSERT_EQ(block_id, fsname.get_block_id());
  test_local_not_exist(ns_addr, block_id);
  test_remote_not_init(ns_addr);
}

TEST_F(NsCacheTest, test_write_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 102;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // write
  int fd = g_tfsClientImpl->open(NULL, NULL, NULL, T_WRITE);
  ASSERT_GT(fd, 0);
  char *str = "hello world";
  int64_t count = strlen(str);
  ret = g_tfsClientImpl->write(fd, str, count);
  ASSERT_EQ(count, ret);
  char tfs_name[TFS_FILE_LEN];
  ret = g_tfsClientImpl->close(fd, tfs_name, TFS_FILE_LEN);
  ASSERT_EQ(TFS_SUCCESS, ret);
  TBSYS_LOG(DEBUG, "write file: (%s) success!", tfs_name);

  FSName fsname;
  fsname.set_name(tfs_name, NULL);
  block_id = fsname.get_block_id();

  test_local_not_exist(ns_addr, block_id);
  test_remote_not_init(ns_addr);

  // read
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_both_miss_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 103;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 103;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_local_hit_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 104;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 104;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_remote_hit_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 105;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 105;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert remote cache
  test_insert_remote(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_local_hit_obsolete_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 106;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 106;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // make local cache obsolete
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.find(block_id);
  bool exist = block_ds_map.end() != iter ? true : false;
  ASSERT_EQ(true, exist);
  ds.clear();
  ds.push_back(23456789);
  ds.push_back(98765432);
  iter->second = ds;

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id, ds);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_remote_hit_obsolete_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 107;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 107;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert remote cache
  test_insert_remote(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make remote cache obsolete
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.find(block_id);
  bool exist = block_ds_map.end() != iter ? true : false;
  ASSERT_EQ(true, exist);
  ds.clear();
  ds.push_back(23456789);
  ds.push_back(98765432);
  iter->second = ds;

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id, ds);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_both_obsolete_read)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 108;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 108;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert local & remote cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);
  test_insert_remote(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make both cache obsolete
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.find(block_id);
  bool exist = block_ds_map.end() != iter ? true : false;
  ASSERT_EQ(true, exist);
  ds.clear();
  ds.push_back(23456789);
  ds.push_back(98765432);
  iter->second = ds;

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id, ds);

  g_tfsClientImpl->close(fd);
}

// large file
TEST_F(NsCacheTest, test_both_miss_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 109;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 109;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_local_hit_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 110;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // insert local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert local cache
    test_insert_local(ns_addr, iter->first, iter->second);
    test_local_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 110;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_not_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_remote_hit_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 111;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // insert remote cache
  test_insert_remote(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id);

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert remote cache
    test_insert_remote(ns_addr, iter->first, iter->second);
    test_remote_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 111;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_hit_partial_local_partial_remote_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 112;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
    else
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 112;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (i > 0)
      test_remote_exist(ns_addr, block_ids[i]);
    else
      test_remote_not_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_local_hit_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 113;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 113;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (0 == i)
    {
      test_remote_not_exist(ns_addr, block_ids[i]);
    }
    else
    {
      test_remote_exist(ns_addr, block_ids[i]);
    }
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_remote_hit_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 114;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (i > 0)
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 114;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_local_partial_remote_hit_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 115;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
    if (1 == i)
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 115;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (0 == i)
    {
      test_remote_not_exist(ns_addr, block_ids[i]);
    }
    else
    {
      test_remote_exist(ns_addr, block_ids[i]);
    }
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_local_hit_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 116;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert local cache
    test_insert_local(ns_addr, iter->first, iter->second);
    test_local_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 116;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make local cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    iter->second.clear();
    iter->second.push_back(23456789);
    iter->second.push_back(98765432);
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_remote_hit_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 117;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert remote cache
    test_insert_remote(ns_addr, iter->first, iter->second);
    test_remote_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 117;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make remote cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    iter->second.clear();
    iter->second.push_back(12345678);
    iter->second.push_back(87654321);
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_hit_partial_local_partial_remote_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 118;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
    else
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 118;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make all cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    iter->second.clear();
    iter->second.push_back(23456789);
    iter->second.push_back(98765432);
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_local_hit_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 119;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 119;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make local cache obsolete
  iter = block_ds_map.begin();
  iter->second.clear();
  iter->second.push_back(12345678);
  iter->second.push_back(87654321);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_local_hit_parial_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 127;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (i < 2)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 127;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make partial local cache obsolete
  iter = block_ds_map.begin();
  iter->second.clear();
  iter->second.push_back(34567890);
  iter->second.push_back(76543210);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (1 == i)
    {
      test_remote_not_exist(ns_addr, block_ids[i]);
    }
    else
    {
      test_remote_exist(ns_addr, block_ids[i]);
    }
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_partial_remote_hit_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 120;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (i > 0)
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 120;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make remote cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    if (i > 0)
    {
      iter->second.clear();
      iter->second.push_back(12345678);
      iter->second.push_back(87654321);
    }
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_local_hit_partial_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 121;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert local cache
    test_insert_local(ns_addr, iter->first, iter->second);
    test_local_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 121;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make partial local cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    if (i > 0)
    {
      iter->second.clear();
      iter->second.push_back(23456789);
      iter->second.push_back(98765432);
    }
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (0 == i)
      test_remote_not_exist(ns_addr, block_ids[i]);
    else
      test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_remote_hit_partial_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 122;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert remote cache
    test_insert_remote(ns_addr, iter->first, iter->second);
    test_remote_exist(ns_addr, iter->first);
  }

  srand(time(NULL) + rand());
  block_id = 122;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make partial remote cache obsolete
  iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    if (i == 0)
    {
      iter->second.clear();
      iter->second.push_back(23456789);
      iter->second.push_back(98765432);
    }
  }

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_all_hit_partial_local_partial_remote_partial_obsolete_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 123;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    if (0 == i)
    {
      // insert local cache
      test_insert_local(ns_addr, iter->first, iter->second);
      test_local_exist(ns_addr, iter->first);
    }
    else
    {
      // insert remote cache
      test_insert_remote(ns_addr, iter->first, iter->second);
      test_remote_exist(ns_addr, iter->first);
    }
  }

  srand(time(NULL) + rand());
  block_id = 123;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // make partial remote cache obsolete
  iter = block_ds_map.begin();
  iter++;
  iter->second.clear();
  iter->second.push_back(12345678);
  iter->second.push_back(87654321);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    if (0 == i)
      test_remote_not_exist(ns_addr, block_ids[i]);
    else
      test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_meta_seg_hit_other_all_miss_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 124;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
  }

  // insert meta seg's block into local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  srand(time(NULL) + rand());
  block_id = 124;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_meta_seg_hit_obsolete_other_all_miss_read_large)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 124;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
  }

  // insert meta seg's block into local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  srand(time(NULL) + rand());
  block_id = 124;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // make local cache obsolete
  iter = block_ds_map.find(block_id);
  bool exist = block_ds_map.end() != iter ? true : false;
  ASSERT_EQ(true, exist);
  ds.clear();
  ds.push_back(23456789);
  ds.push_back(98765432);
  iter->second = ds;

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_local_hit_obsolete_stat)
{
  int ret = TFS_ERROR;

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 125;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 125;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // insert local cache
  test_insert_local(ns_addr, block_id, ds);
  test_local_exist(ns_addr, block_id);

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // make local cache obsolete
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.find(block_id);
  bool exist = block_ds_map.end() != iter ? true : false;
  ASSERT_EQ(true, exist);
  ds.clear();
  ds.push_back(23456789);
  ds.push_back(98765432);
  iter->second = ds;

  // stat
  TfsFileStat statBuf;
  ret = g_tfsClientImpl->fstat(fd, &statBuf);
  ASSERT_EQ(ret, TFS_SUCCESS);
  TBSYS_LOG(DEBUG, "stat file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id, ds);
  test_remote_exist(ns_addr, block_id, ds);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_cache_switch_local_cache_only)
{
  int ret = TFS_ERROR;

  // switch remote cache off
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_LOCAL);

  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 126;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 126;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  // stat
  TfsFileStat statBuf;
  ret = g_tfsClientImpl->fstat(fd, &statBuf);
  ASSERT_EQ(ret, TFS_SUCCESS);
  TBSYS_LOG(DEBUG, "stat file: (%s) success!", tfs_name);

  test_local_exist(ns_addr, block_id, ds);
  test_remote_not_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_cache_switch_remote_cache_only)
{
  int ret = TFS_ERROR;

  // switch local cache off
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_REMOTE);

  // small file stat
  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 127;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 127;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // stat
  TfsFileStat statBuf;
  ret = g_tfsClientImpl->fstat(fd, &statBuf);
  ASSERT_EQ(ret, TFS_SUCCESS);
  TBSYS_LOG(DEBUG, "stat file: (%s) success!", tfs_name);

  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

  // large file read
  // get non-meta segments blockids (first 3 block in block_ds_map)
  VUINT32 block_ids;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.begin();
  for (int i = 0; block_ds_map.end() != iter && i < 3; i++, iter++)
  {
    block_ids.push_back(iter->first);
    // insert remote cache
    test_insert_remote(ns_addr, iter->first, iter->second);
    test_remote_exist(ns_addr, iter->first);
  }

  // open
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_READ|T_LARGE);
  ASSERT_GT(fd, 0);
  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  // read
  char buf[BUF_LEN];
  ret = g_tfsClientImpl->read(fd, buf, BUF_LEN);
  ASSERT_GT(ret, 0);
  TBSYS_LOG(DEBUG, "read file: (%s) success!", tfs_name);

  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    test_local_not_exist(ns_addr, block_ids[i]);
    test_remote_exist(ns_addr, block_ids[i]);
  }

  g_tfsClientImpl->close(fd);
}

TEST_F(NsCacheTest, test_cache_switch_off_local_cache)
{
  // add writable block
  TfsSession* tfs_session = g_tfsClientImpl->get_tfs_session(ns_addr);
  std::map<uint32_t, VUINT64>& block_ds_map = tfs_session->block_ds_map_;
  uint32_t block_id = 128;
  VUINT64 ds;
  ds.push_back(12345678);
  ds.push_back(87654321);
  block_ds_map.insert(std::map<uint32_t, VUINT64>::value_type(block_id, ds));

  srand(time(NULL) + rand());
  block_id = 128;
  uint64_t file_id = 1 + rand() % 32768;
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);
  const char* tfs_name = fsname.get_name();

  // open
  int fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

  // switch local cache off
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_REMOTE);

  block_id = 127;
  FSName fsname2;
  fsname2.set_block_id(block_id);
  fsname2.set_file_id(file_id);
  tfs_name = fsname2.get_name();

  // open
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

  // switch remote cache off
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_NO);

  block_id = 126;
  FSName fsname3;
  fsname3.set_block_id(block_id);
  fsname3.set_file_id(file_id);
  tfs_name = fsname3.get_name();

  // open
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_not_exist(ns_addr, block_id);
  test_remote_not_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

  // switch remote cache on
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_REMOTE);

  // open
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_not_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

  // switch local cache on
  g_tfsClientImpl->set_use_cache(USE_CACHE_FLAG_LOCAL|USE_CACHE_FLAG_REMOTE);

  // open
  fd = g_tfsClientImpl->open(tfs_name, NULL, NULL, T_STAT);
  ASSERT_GT(fd, 0);
  test_local_exist(ns_addr, block_id);
  test_remote_exist(ns_addr, block_id);

  g_tfsClientImpl->close(fd);

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
