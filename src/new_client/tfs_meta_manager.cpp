/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_manager.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "tfs_meta_manager.h"
#include "fsname.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

int TfsMetaManager::initialize()
{
  int ret = TFS_SUCCESS;
  if ((ret = TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, 1000, false)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize tfs client failed, ret: %d", ret);
  }
  return ret;
}

int TfsMetaManager::destroy()
{
  return TfsClientImpl::Instance()->destroy();
}

int32_t TfsMetaManager::get_cluster_id(const char* ns_addr)
{
  return TfsClientImpl::Instance()->get_cluster_id(ns_addr);
}

int64_t TfsMetaManager::read_data(const char* ns_addr, const uint32_t block_id, const uint64_t file_id,
    void* buffer, const int32_t offset, const int64_t length)
{
  FSName fsname;
  fsname.set_block_id(block_id);
  fsname.set_file_id(file_id);

  int64_t ret_length = -1;
  int fd = TfsClientImpl::Instance()->open(fsname.get_name(), NULL, ns_addr, T_READ);
  if (fd < 0)
  {
    TBSYS_LOG(ERROR, "open read file error, file_name: %s, fd: %d", fsname.get_name(), fd);
  }
  else
  {
    TfsClientImpl::Instance()->lseek(fd, offset, T_SEEK_SET);
    ret_length = TfsClientImpl::Instance()->read(fd, buffer, length);
    TfsClientImpl::Instance()->close(fd);
  }

  return ret_length;
}

int64_t TfsMetaManager::write_data(const char* ns_addr, const void* buffer, const int64_t offset, const int64_t length,
    common::FragMeta& frag_meta)
{
  //TODO get tfs_file from file_pool

  int ret = TFS_SUCCESS;
  int64_t cur_pos = 0;
  int64_t cur_length, left_length = length;
  char tfsname[MAX_FILE_NAME_LEN];
  int fd = TfsClientImpl::Instance()->open(NULL, NULL, ns_addr, T_WRITE);
  if (fd < 0)
  {
    TBSYS_LOG(ERROR, "open write file error, fd: %d", fd);
  }
  else
  {
    int64_t write_length = 0;
    do
    {
      cur_length = min(left_length, MAX_WRITE_DATA_IO);
      write_length = TfsClientImpl::Instance()->write(fd, reinterpret_cast<const char*>(buffer) + cur_pos, cur_length);
      if (write_length < 0)
      {
        TBSYS_LOG(ERROR, "tfs write data error, ret: %"PRI64_PREFIX"d", write_length);
        ret = TFS_ERROR;
        break;
      }
      cur_pos += write_length;
      left_length -= write_length;
    }
    while(left_length > 0);
    TfsClientImpl::Instance()->close(fd, tfsname, MAX_FILE_NAME_LEN);
    TBSYS_LOG(DEBUG, "tfs write success, tfsname: %s", tfsname);

    FSName fsname;
    fsname.set_name(tfsname);

    frag_meta.block_id_ = fsname.get_block_id();
    frag_meta.file_id_ = fsname.get_file_id();
    frag_meta.offset_ = offset;
    frag_meta.size_ = length - left_length;
  }

  return (length - left_length);
}
