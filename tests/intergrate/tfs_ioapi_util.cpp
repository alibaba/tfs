/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_ioapi_util.cpp 166 2011-03-15 07:35:48Z zongdai@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "common/func.h"
#include "common/define.h"
#include "tfs_ioapi_util.h"
#include <stdlib.h>
#include <tbsys.h>
#include <iostream>
#include <vector>

using namespace tfs::common;
using namespace tfs::message;
using namespace std;
using namespace tbsys;

#ifdef USE_CPP_CLIENT
using namespace tfs::client;
TfsClient* TfsIoApiUtil::tfs_client_;
#endif

#ifdef USE_CPP_CLIENT
#define tfs_open tfs_client_->open
#define tfs_read tfs_client_->read
#define tfs_write tfs_client_->write
#define tfs_close tfs_client_->close
#define tfs_fstat tfs_client_->fstat
#define tfs_unlink tfs_client_->unlink
#else
#define tfs_open t_open
#define tfs_read t_read
#define tfs_write t_write
#define tfs_close t_close
#define tfs_fstat t_fstat
#define tfs_unlink t_unlink
#endif

static const int32_t MAX_LOOP = 20;
TfsIoApiUtil::TfsIoApiUtil()
{
  srand(time(NULL) + rand() + pthread_self());
}

TfsIoApiUtil::~TfsIoApiUtil()
{
}

int32_t TfsIoApiUtil::generate_data(char* buffer, const int32_t length)
{
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int TfsIoApiUtil::generate_length(int64_t& length, int64_t base)
{
  length = static_cast<int64_t>(rand()) * static_cast<int64_t>(rand()) % base + base;
  return 0;
}

int64_t TfsIoApiUtil::write_new_file(const bool large_flag, const int64_t length, uint32_t& crc,
                          char* tfsname, const char* suffix, const int32_t name_len)
{
  int64_t cost_time = 0, start_time = 0, end_time = 0;
  int fd = 0;
  if (large_flag) //large file
  {
    const char* key = "test_large";
    int local_fd = open(key, O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (local_fd < 0)
    {
      return local_fd;
    }
    close(local_fd);
    start_time = CTimeUtil::getTime();
    // do not support update now
    fd = tfs_open(tfsname, suffix, NULL, T_WRITE | T_LARGE, key);
    end_time = CTimeUtil::getTime();
  }
  else
  {
    start_time = CTimeUtil::getTime();
#ifdef USE_CPP_CLIENT
    fd = tfs_open(tfsname, suffix, NULL, T_WRITE);
#else
    fd = tfs_open(tfsname, suffix, NULL, T_WRITE, NULL);
#endif
    end_time = CTimeUtil::getTime();
  }
  cost_time += (end_time - start_time);
  if (fd <= 0)
  {
    return fd;
  }
  cout << "tfs open successful. large file flag: " << large_flag << " cost time " << cost_time << endl;

  int64_t ret = TFS_SUCCESS;
  char* buffer = new char[PER_SIZE];
  int64_t remain_size = length;
  int64_t cur_size = 0;
  while (remain_size > 0)
  {
    cur_size = (remain_size >= PER_SIZE) ? PER_SIZE : remain_size;
    generate_data(buffer, cur_size);
    start_time = CTimeUtil::getTime();
    ret = tfs_write(fd, buffer, cur_size);
    end_time = CTimeUtil::getTime();
    if (ret != cur_size)
    {
      cout << "tfs write fail. cur_size: " << cur_size << endl;
      delete []buffer;
      return ret;
    }
    crc = Func::crc(crc, buffer, cur_size);
    uint32_t single_crc = 0;
    single_crc = Func::crc(single_crc, buffer, cur_size);
    cout << "write offset " << length - remain_size << " size " << cur_size << " write crc " << crc 
         << " single crc " << single_crc << " cost time " << end_time - start_time << endl;
    remain_size -= cur_size;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  cout << "write data successful. write length: " << length << " crc: " <<  crc << endl;

  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  start_time = CTimeUtil::getTime();
  ret = tfs_close(fd, tfs_name, TFS_FILE_LEN);
  end_time = CTimeUtil::getTime();
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }
  cout << "tfs close tfsname:" << tfs_name << " cost time " << end_time - start_time << " total cost time " << cost_time << endl;
  cost_time += (end_time - start_time);
  if (NULL != tfsname)
  {
    strncpy(tfsname, tfs_name, name_len);
  }

  return length;
}

int TfsIoApiUtil::read_exist_file(const bool large_flag, const char* tfs_name, const char* suffix, uint32_t& read_crc)
{
  int64_t cost_time = 0, start_time = 0, end_time = 0;
  int fd = 0;
  start_time = CTimeUtil::getTime();
  if (large_flag)
  {
#ifdef USE_CPP_CLIENT
    fd = tfs_open(tfs_name, suffix, NULL, T_READ | T_LARGE);
#else
    fd = tfs_open(tfs_name, suffix, NULL, T_READ | T_LARGE, NULL);
#endif
  }
  else
  {
#ifdef USE_CPP_CLIENT
    fd = tfs_open(tfs_name, suffix, NULL, T_READ);
#else
    fd = tfs_open(tfs_name, suffix, NULL, T_READ, NULL);
#endif
  }
  end_time = CTimeUtil::getTime();
  if (fd <= 0)
  {
    return fd;
  }
  cost_time += (end_time - start_time);
  cout << "tfs open successful. cost time " << cost_time << endl;

  //TfsFileStat finfo;
  //int ret = TFS_HANDLE->fstat(fd, &finfo, 1);
  //if (ret != TFS_SUCCESS)
  //{
  //  return ret;
  //}

  read_crc = 0;
  int64_t max_read_size = 0;
  if (large_flag)
  {
    max_read_size = PER_SIZE;
  }
  else
  {
    max_read_size = SEG_SIZE;
  }
  char* buffer = new char[PER_SIZE];
  int64_t cur_size = 0;
  int64_t offset = 0;
  int ret = 0;
  bool not_end = true;
  while (not_end)
  {
    //cur_size = PER_SIZE;
    cur_size = max_read_size;
    start_time = CTimeUtil::getTime();
    ret = tfs_read(fd, buffer, cur_size);
    end_time = CTimeUtil::getTime();
    if (ret < 0)
    {
      cout << "tfs read fail. cur_size: " << cur_size << endl;
      delete []buffer;
      return ret;
    }
    else if (ret < cur_size)
    {
      cout << "tfs read reach end. cur_size: " << cur_size << " read size: " << ret << endl;
      not_end = false;
    }

    read_crc = Func::crc(read_crc, buffer, ret);
    offset += ret;
    uint32_t single_crc = 0;
    single_crc = Func::crc(single_crc, buffer, ret);
    cout << "read offset " << offset << " size " << ret << " read crc " << read_crc
         << " single crc " << single_crc << " cost time " << end_time - start_time << endl;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  start_time = CTimeUtil::getTime();
  ret = tfs_close(fd, NULL, 0);
  end_time = CTimeUtil::getTime();
  if (TFS_SUCCESS != ret)
  {
    return ret;
  }
  cost_time += (end_time - start_time);
  cout << "read length: " << offset << " read crc " << read_crc << " cost time " << cost_time << endl;

  return TFS_SUCCESS;
}

int TfsIoApiUtil::stat_exist_file(const bool large_flag, char* tfsname, TfsFileStat& file_stat)
{
  int fd = 0;
  if (large_flag)
  {
#ifdef USE_CPP_CLIENT
    fd = tfs_open(tfsname, NULL, NULL, T_STAT | T_LARGE);
#else
    fd = tfs_open(tfsname, NULL, NULL, T_STAT | T_LARGE, NULL);
#endif
  }
  else
  {
#ifdef USE_CPP_CLIENT
    fd = tfs_open(tfsname, NULL, NULL, T_STAT);
#else
    fd = tfs_open(tfsname, NULL, NULL, T_STAT, NULL);
#endif
  }
  if (fd <= 0)
  {
    return fd;
  }

  int ret = tfs_fstat(fd, &file_stat, FORCE_STAT);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  cout << "tfsname: " << tfsname << " id: " << file_stat.file_id_ << " size: " << file_stat.size_  << " usize: " << file_stat.usize_
       << " offset: " << file_stat.offset_ << " flag: " << file_stat.flag_  << " crc: " << file_stat.crc_
       << " create time: " << file_stat.create_time_ << " modify time: " << file_stat.modify_time_ << endl;
  ret = tfs_close(fd, NULL, 0);
  return ret;
}

int TfsIoApiUtil::unlink_file(const char* tfsname, const char* suffix, const TfsUnlinkType action)
{
  return tfs_unlink(tfsname, suffix, action);
}
