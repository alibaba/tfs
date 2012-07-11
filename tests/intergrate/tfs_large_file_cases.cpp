/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_large_file_cases.cpp 432 2011-06-08 07:06:11Z nayan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "common/func.h"
#include "common/define.h"
#include "tfs_large_file_cases.h"
#include "tfs_ioapi_util.h"
#include <stdlib.h>
#include <tbsys.h>
#include <iostream>
#include <vector>

using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace std;
using namespace tbsys;

void TfsLargeFileTest::SetUpTestCase()
{
}

void TfsLargeFileTest::TearDownTestCase()
{
}

void TfsLargeFileTest::test_read(const bool large_flag, int64_t base, const char* suffix)
{
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  uint32_t write_crc = 0;
  uint32_t read_crc = 0;
  int64_t length = 0;
  TfsIoApiUtil::generate_length(length, base);
  cout << "generate length: " << length << endl;
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
  ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
  cout << "write crc: " << write_crc << " read crc: " << read_crc << endl;
  ASSERT_EQ(write_crc, read_crc);
}

void TfsLargeFileTest::unlink_process(const bool large_flag)
{
  vector<const char*> suffixs;
  const char* a = NULL;
  const char* b = ".jpg";
  const char* c = ".bmp";
  suffixs.push_back(a);
  suffixs.push_back(b);
  suffixs.push_back(c);

  const int32_t INVALID_FILE_SIZE = -1;

  for (size_t i = 0; i < suffixs.size(); ++i)
  {
    int64_t length = 0;
    if (large_flag)
    {
      // 20M
      length = rand() % 40960000;
    }
    else
    {
      length = rand() % 3072000;
    }
    // write
    uint32_t crc = 0;
    char tfs_name[TFS_FILE_LEN];
    memset(tfs_name, 0, TFS_FILE_LEN);

    ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, crc, tfs_name, suffixs[i], TFS_FILE_LEN));

    // stat normal file
    TfsFileStat finfo;
    memset(&finfo, 0, sizeof(TfsFileStat));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
    ASSERT_EQ(0, finfo.flag_);
    if (large_flag)
    {
      ASSERT_EQ(length, finfo.size_);
      ASSERT_TRUE(length < finfo.usize_);
    }
    else
      ASSERT_EQ(length , finfo.size_);

    // do conceal
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, CONCEAL));
    usleep(100 * 1000);
    cout << "conceal success" << endl;

    // stat
    memset(&finfo, 0, sizeof(TfsFileStat));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
    ASSERT_EQ(4, finfo.flag_);
    if (large_flag)
    {
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.size_);
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.usize_);
    }
    else
      ASSERT_EQ(length , finfo.size_);

    // do conceal again, return false
    ASSERT_NE(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, CONCEAL));
    usleep(100 * 1000);
    cout << "conceal again success" << endl;

    // reveal
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, REVEAL));
    usleep(100 * 1000);
    cout << "reveal success" << endl;

    // stat
    memset(&finfo, 0, sizeof(TfsFileStat));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
    ASSERT_EQ(0, finfo.flag_);
    if (large_flag)
    {
      ASSERT_EQ(length, finfo.size_);
      ASSERT_TRUE(length < finfo.usize_);
    }
    else
      ASSERT_EQ(length , finfo.size_);

    // reveal again, return false
    ASSERT_NE(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, REVEAL));
    usleep(100 * 1000);
    cout << "reveal again success" << endl;

    // do unlink
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name));
    usleep(100 * 1000);
    cout << "delete success" << endl;

    // stat deleted file
    memset(&finfo, 0, sizeof(TfsFileStat));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
    ASSERT_EQ(1, finfo.flag_);
    if (large_flag)
    {
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.size_);
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.usize_);
    }
    else
      ASSERT_EQ(length , finfo.size_);

    // do unlink again, fail
    ASSERT_NE(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name));
    usleep(100 * 1000);
    cout << "delete again success" << endl;

    // stat deleted file again
    memset(&finfo, 0, sizeof(TfsFileStat));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
    ASSERT_EQ(1, finfo.flag_);
    if (large_flag)
    {
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.size_);
      ASSERT_EQ(INVALID_FILE_SIZE, finfo.usize_);
    }
    else
      ASSERT_EQ(length , finfo.size_);

    if (!large_flag)
    {
      // do undelete
      ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, UNDELETE));
      usleep(100 * 1000);
      cout << "undelete success" << endl;

      // stat
      memset(&finfo, 0, sizeof(TfsFileStat));
      ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
      ASSERT_EQ(0, finfo.flag_);
      ASSERT_EQ(length , finfo.size_);

      // do undelete again
      ASSERT_NE(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, UNDELETE));
      usleep(100 * 1000);
      cout << "undelete again success" << endl;

      // stat
      memset(&finfo, 0, sizeof(TfsFileStat));
      ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(large_flag, tfs_name, finfo));
      ASSERT_EQ(0, finfo.flag_);
      ASSERT_EQ(length , finfo.size_);
    }
    else
    {
      // do undelete
      ASSERT_NE(TFS_SUCCESS, TfsIoApiUtil::unlink_file(tfs_name, NULL, UNDELETE));
      usleep(100 * 1000);
      cout << "undelete success" << endl;
    }
  }
}

void TfsLargeFileTest::test_update(const bool large_flag, int64_t base, const char* suffix)
{
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  int64_t length = 0;
  TfsIoApiUtil::generate_length(length, base);
  cout << "generate length: " << length << endl;

  uint32_t write_crc = 0;
  uint32_t read_crc = 0;
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
  ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
  cout << "write crc: " << write_crc << " read crc: " << read_crc << endl;
  ASSERT_EQ(write_crc, read_crc);

  if (!large_flag)
  {
    // update: file is same length
    write_crc = 0;
    read_crc = 0;
    ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
    cout << "write crc: " << write_crc << " read crc: " << read_crc << endl;
    ASSERT_EQ(write_crc, read_crc);

    // update: re generate length
    TfsIoApiUtil::generate_length(length, base);
    cout << "generate length: " << length << endl;
    write_crc = 0;
    read_crc = 0;
    ASSERT_EQ(length, TfsIoApiUtil::write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
    cout << "write crc: " << write_crc << " read crc: " << read_crc << endl;
    ASSERT_EQ(write_crc, read_crc);
  }
  else
  {
    // update is not support now
    read_crc = 0;
    cout << "tfs name: " << tfs_name << endl;
    uint32_t tmp_write_crc = 0;

    ASSERT_NE(length, TfsIoApiUtil::write_new_file(large_flag, length, tmp_write_crc, tfs_name, suffix, TFS_FILE_LEN));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
    ASSERT_EQ(write_crc, read_crc);

    // update: re generate length
    TfsIoApiUtil::generate_length(length, base);
    cout << "generate length: " << length << endl;
    read_crc = 0;
    tmp_write_crc = 0;
    ASSERT_NE(length, TfsIoApiUtil::write_new_file(large_flag, length, tmp_write_crc, tfs_name, suffix, TFS_FILE_LEN));
    ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::read_exist_file(large_flag, tfs_name, suffix, read_crc));
    ASSERT_EQ(write_crc, read_crc);
  }
}

TEST_F(TfsLargeFileTest, write_small_file)
{
  int64_t length = 0;
  uint32_t crc = 0;
  srand((unsigned)time(NULL));
  // 512K ~ 1M
  TfsIoApiUtil::generate_length(length, 1024 * 512);
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(false, length, crc));
  // 1~2M
  crc = 0;
  TfsIoApiUtil::generate_length(length, 1024 * 1024);
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(false, length, crc));
  // 2~4M
  crc = 0;
  TfsIoApiUtil::generate_length(length, 2 * 1024 * 1024);
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(false, length, crc));
}

TEST_F(TfsLargeFileTest, update_small_file)
{
  const int max_loop = 2;
  // 512K ~ 1M
  for (int i = 0; i < max_loop; ++i)
    test_update(false, 1024 * 512);
  // 1~2M
  for (int i = 0; i < max_loop; ++i)
    test_update(false, 1024 * 1024, ".jpg");
  // 2~4M
  for (int i = 0; i < max_loop; ++i)
    test_update(false, 1024 * 1024, ".jpg");
}

TEST_F(TfsLargeFileTest, read_small_file)
{
  const int max_loop = 20;
  //const int max_loop = 1;
  // 512K ~ 1M
  for (int i = 0; i < max_loop; ++i)
    test_read(false, 1024 * 512);
  // 1~2M
  for (int i = 0; i < max_loop; ++i)
    test_read(false, 1024 * 1024, ".jpg");
  // 2~4M
  for (int i = 0; i < max_loop; ++i)
    test_read(false, 1024 * 1024, ".jpg");
}

TEST_F(TfsLargeFileTest, stat_exist_small_file)
{
  int64_t length = 607200;
  uint32_t crc = 0;
  char tfs_name[TFS_FILE_LEN];
  TfsFileStat finfo;

  memset(tfs_name, 0, TFS_FILE_LEN);
  memset(&finfo, 0 ,sizeof(TfsFileStat));
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(false, length, crc, tfs_name, NULL, TFS_FILE_LEN));
  ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(false, tfs_name, finfo));

  crc = 0;
  length = 1024343;
  memset(tfs_name, 0, TFS_FILE_LEN);
  memset(&finfo, 0, sizeof(TfsFileStat));
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(false, length, crc, tfs_name, ".jpg" , TFS_FILE_LEN));
  ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(false, tfs_name, finfo));
}

TEST_F(TfsLargeFileTest, unlink_small_file)
{
  unlink_process(false);
}

//TEST_F(TfsLargeFileTest, write_large_file)
//{
//  int64_t length = 0;
//  uint32_t crc = 0;
//  srand((unsigned)time(NULL));
//  // 512K ~ 1M
//  TfsIoApiUtil::generate_length(length, 1024 * 512);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 1~2M
//  crc = 0;
//  TfsIoApiUtil::generate_length(length, 1024 * 1024);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 2~4M
//  crc = 0;
//  TfsIoApiUtil::generate_length(length, 2 * 1024 * 1024);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 5~10M
//  crc = 0;
//  TfsIoApiUtil::generate_length(length, 5 * 1024 * 1024);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 40~80M
//  crc = 0;
//  TfsIoApiUtil::generate_length(length, 40 * 1024 * 1024);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 1024M ~ 2048M
//  crc = 0;
//  TfsIoApiUtil::generate_length(length, 1024 * 1024 * 1024);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//  // 3G ~ 6G
//  crc = 0;
//  int64_t base = 3 * 1024 * 1024 * 1024L;
//  TfsIoApiUtil::generate_length(length, base);
//  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc));
//}

TEST_F(TfsLargeFileTest, read_large_file)
{
  const int max_loop = 1;
  // 512K ~ 1M
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 1024 * 512);
  // 1~2M
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 1024 * 1024, ".jpg");
  // 2~4M
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 2 * 1024 * 1024);
  // 5~10M
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 5 * 1024 * 1024, ".gif");
  // 40~80M
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 40 * 1024 * 1024);
  // 1G ~ 2G
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 1024 * 1024 * 1024, ".bmp");
  // 3G ~ 6G
  for (int i = 0; i < max_loop; ++i)
    test_read(true, 3 * 1024 * 1024 * 1024L);
}

TEST_F(TfsLargeFileTest, update_large_file)
{
  const int max_loop = 3;
  // 512K ~ 1M
  for (int i = 0; i < max_loop; ++i)
    test_update(true, 1024 * 512);
  // 10~20M
  for (int i = 0; i < max_loop; ++i)
    test_update(true, 10 * 1024 * 1024, ".jpg");
  // 200~400M
  for (int i = 0; i < max_loop; ++i)
    test_update(true, 200 * 1024 * 1024, ".jpg");
}

TEST_F(TfsLargeFileTest, stat_exist_large_file)
{
  int64_t length = 60720000;
  uint32_t crc = 0;
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  ASSERT_EQ(length, TfsIoApiUtil::write_new_file(true, length, crc, tfs_name, NULL, TFS_FILE_LEN));

  TfsFileStat finfo;
  ASSERT_EQ(TFS_SUCCESS, TfsIoApiUtil::stat_exist_file(true, tfs_name, finfo));
}

TEST_F(TfsLargeFileTest, unlink_large_file)
{
  unlink_process(true);
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

#ifdef USE_CPP_CLIENT
  TfsIoApiUtil::tfs_client_ = TfsClient::Instance();
	int ret = TfsIoApiUtil::tfs_client_->initialize(nsip);
#else
  int ret = t_initialize(nsip, 10, 10000);
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
  TfsIoApiUtil::tfs_client_ = tfs::client::TfsClient::Instance();
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
