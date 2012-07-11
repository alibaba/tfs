/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_block_cache.h 166 2011-03-15 07:35:48Z zongdai@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_TEST_BLOCK_CACHE_H_
#define TFS_TEST_BLOCK_CACHE_H_
#include <tbsys.h>
#include <gtest/gtest.h>
#include "message/message_factory.h"
#include "common/define.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/tfs_session.h"
#include <vector>
#include <string>

struct FileRecord
{
  std::string file_name_;
  uint32_t write_crc_;
  int64_t length_;
  bool large_flag_;
};

class TfsBlockCacheTest : public testing::Test
{
  public:
    static void SetUpTestCase();
    static void TearDownTestCase();

  public:
    static void set_ns_ip(const char* nsip);
    static void write_files(const int32_t count);
    static void backup_cache();
    static void modify_cache(const int32_t step);
    static void read_files(const int32_t count);
    static void check_cache();
    static void shuffle_files();

  public:
    static std::vector<FileRecord> tfs_files_;
    static tfs::client::TfsSession::BLOCK_CACHE_MAP bak_cache_;
    static std::string ns_ip_;
};

struct DsSort
{   
  bool operator()(const uint64_t left, const uint64_t right) const
  {   
    return (left < right);
  }   
}; 
#endif
