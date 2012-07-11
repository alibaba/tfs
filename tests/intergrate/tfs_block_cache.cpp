/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_block_cache.cpp 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "common/func.h"
#include "common/define.h"
#include "tfs_block_cache.h"
#include "tfs_ioapi_util.h"
#include <stdlib.h>
#include <tbsys.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <functional>

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;
using namespace tbsys;

string TfsBlockCacheTest::ns_ip_;
vector<FileRecord> TfsBlockCacheTest::tfs_files_;
TfsSession::BLOCK_CACHE_MAP TfsBlockCacheTest::bak_cache_;

void TfsBlockCacheTest::SetUpTestCase()
{
}

void TfsBlockCacheTest::TearDownTestCase()
{
}

void TfsBlockCacheTest::set_ns_ip(const char* nsip)
{
  ns_ip_ = nsip;
}

void TfsBlockCacheTest::write_files(const int count)
{
  char tfs_name[TFS_FILE_LEN];
  bool large_flag = false;
  const char* suffix = ".jpg";
  for (int i = 0; i < count; ++i)
  {
    memset(tfs_name, 0, TFS_FILE_LEN);
    uint32_t write_crc = 0;
    int64_t length = 0;
    int64_t base = 10240; // 10K
    if (i % 2 == 0)
    {
      large_flag = true;
      base = 20480; // 20K
    }
    else
    {
      large_flag = false;
    }
    TfsIoApiUtil::generate_length(length, base);
    ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
    cout << "generate length: " << length << " tfsname: " << tfs_name << " write crc: "
         << write_crc << "large flag: " << large_flag << endl;
    FileRecord file_record;
    file_record.file_name_ = tfs_name;
    file_record.write_crc_ = write_crc;
    file_record.length_ = length;
    file_record.large_flag_ = large_flag;
    tfs_files_.push_back(file_record);
  }
  random_shuffle(tfs_files_.begin(), tfs_files_.end());
  cout << "write file finish. file count " << tfs_files_.size() << endl;
}

void TfsBlockCacheTest::backup_cache()
{
  TfsSession::BLOCK_CACHE_MAP* lru_cache = TfsClientImpl::Instance()->get_tfs_session(ns_ip_.c_str())->get_block_cache_map();
  TfsSession::BLOCK_CACHE_MAP_ITER lit = lru_cache->begin();
  for ( ; lit != lru_cache->end(); ++lit)
  {
    bak_cache_.insert(lit->first, lit->second);
  }
}

void TfsBlockCacheTest::modify_cache(const int32_t step)
{
  cout << "begin modify cache. step " << step << endl;
  TfsSession::BLOCK_CACHE_MAP* lru_cache = TfsClientImpl::Instance()->get_tfs_session(ns_ip_.c_str())->get_block_cache_map();
  TfsSession::BLOCK_CACHE_MAP_ITER lit = lru_cache->begin();
  int i = 1;
  for ( ; lit != lru_cache->end(); ++lit, ++i)
  {
    if (0 == (i % step))
    {
      BlockCache& blk_cache = lit->second;
      int seed = rand();
      if (1 == (seed % 2))
      {
        if (blk_cache.ds_.size() > 0)
        {
          blk_cache.ds_[0] = rand();
        }
      }
      else if (0 == (seed % 2))
      {
        int index = 0;
        if (blk_cache.ds_.size() >= 0)
        {
          if (blk_cache.ds_.size() >= 2)
          {
            index = 1;
          }
          blk_cache.ds_[index] = rand();
        }
      }
      else if (0 == (seed % 3))
      {
        for (int i = 0; i < static_cast<int>(blk_cache.ds_.size()); ++i)
        {
          blk_cache.ds_[i] = rand();
        }
      }
    }
  }
  cout << "finish modify cache. step " << step << endl;
}

void TfsBlockCacheTest::read_files(const int32_t count)
{
  int size = std::min(count, static_cast<const int32_t>(tfs_files_.size()));
  for (int i = 0; i < size; ++i)
  {
    FileRecord& fr = tfs_files_[i];
    uint32_t read_crc;
    TfsFileStat file_info;
    cout << "read file tfsname: " << fr.file_name_.c_str() << " length: " << fr.length_ << " write crc: " << fr.write_crc_ << endl;
    if (0 == (i % 2))
    {
      ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(fr.large_flag_, const_cast<char*>(fr.file_name_.c_str()), file_info));
    }
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(fr.large_flag_, fr.file_name_.c_str(), ".jpg", read_crc));
    ASSERT_EQ(read_crc, fr.write_crc_);
    if (1 == (i % 2))
    {
      ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(fr.large_flag_, const_cast<char*>(fr.file_name_.c_str()), file_info));
    }
  }
}

void TfsBlockCacheTest::check_cache()
{
  TfsSession::BLOCK_CACHE_MAP* now_cache = TfsClientImpl::Instance()->get_tfs_session(ns_ip_.c_str())->get_block_cache_map();
  TfsSession::BLOCK_CACHE_MAP_ITER sit = now_cache->begin();
  for ( ; sit != now_cache->end(); ++sit)
  {
    BlockCache* b_cache = bak_cache_.find(sit->first);
    std::sort(b_cache->ds_.begin(), b_cache->ds_.end(), DsSort());
    std::sort(sit->second.ds_.begin(), sit->second.ds_.end(), DsSort());

    ASSERT_EQ(b_cache->ds_.size(), sit->second.ds_.size());
    for (int i = 0; i < static_cast<int>(b_cache->ds_.size()); ++i)
    {
      ASSERT_EQ(b_cache->ds_[i], sit->second.ds_[i]);
    }
  }
}

void TfsBlockCacheTest::shuffle_files()
{
  cout << "start shuffle" << endl;
  random_shuffle(tfs_files_.begin(), tfs_files_.end());
  cout << "end shuffle" << endl;
}

TEST_F(TfsBlockCacheTest, test_block_cache)
{
  int count = 200000;
  TfsBlockCacheTest::write_files(count);
  TfsBlockCacheTest::backup_cache();
  TfsBlockCacheTest::read_files(count);

  int step = 2;
  TfsBlockCacheTest::modify_cache(step);
  TfsBlockCacheTest::shuffle_files();
  TfsBlockCacheTest::read_files(count);
}

void usage(char* name)
{
  printf("Usage: %s -s nsip:port\n", name);
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char nsip[256];
  int i = 0;

  memset(nsip, 0, 256);
  while ((i = getopt(argc, argv, "s:i")) != EOF)
  {
    switch (i)
    {
      case 's':
        strcpy(nsip, optarg);
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if ('\0' == nsip[0])
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  TfsBlockCacheTest::ns_ip_ = nsip;
#ifdef USE_CPP_CLIENT
  TfsIoApiUtil::tfs_client_ = TfsClient::Instance();
	int ret = TfsIoApiUtil::tfs_client_->initialize(TfsBlockCacheTest::ns_ip_.c_str(), 10000, 1000000);
#else
  int ret = t_initialize(TfsBlockCacheTest::ns_ip_.c_str(), 10000, 1000000);
#endif
	if (ret != TFS_SUCCESS)
	{
    cout << "tfs client init fail" << endl;
		return TFS_ERROR;
	}

  return TFS_SUCCESS;
}

void destroy()
{
#ifdef USE_CPP_CLIENT
  TfsIoApiUtil::tfs_client_ = TfsClient::Instance();
	TfsIoApiUtil::tfs_client_->destroy();
#else
  t_destroy();
#endif
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (ret == TFS_ERROR)
  {
    printf("input argument error...\n");
    return 1;
  }
  ret = RUN_ALL_TESTS();
  destroy();
  return ret;
}
