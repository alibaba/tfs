/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "tfs_client_api.h"
#include "tfs_client_impl.h"
#include "client_config.h"

using namespace tfs::common;
using namespace tfs::client;
using namespace std;

TfsClient::TfsClient()
{
}

TfsClient::~TfsClient()
{
}

int TfsClient::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items,
                          const bool start_bg)
{
  return TfsClientImpl::Instance()->initialize(ns_addr, cache_time, cache_items, start_bg);
}

int TfsClient::set_default_server(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  return TfsClientImpl::Instance()->set_default_server(ns_addr, cache_time, cache_items);
}

int TfsClient::destroy()
{
  return TfsClientImpl::Instance()->destroy();
}

int64_t TfsClient::read(const int fd, void* buf, const int64_t count)
{
  return TfsClientImpl::Instance()->read(fd, buf, count);
}

int64_t TfsClient::readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* file_info)
{
  return TfsClientImpl::Instance()->readv2(fd, buf, count, file_info);
}

int64_t TfsClient::write(const int fd, const void* buf, const int64_t count)
{
  return TfsClientImpl::Instance()->write(fd, buf, count);
}

int64_t TfsClient::lseek(const int fd, const int64_t offset, const int whence)
{
  return TfsClientImpl::Instance()->lseek(fd, offset, whence);
}

int64_t TfsClient::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
{
  return TfsClientImpl::Instance()->pread(fd, buf, count, offset);
}

int64_t TfsClient::pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
{
  return TfsClientImpl::Instance()->pwrite(fd, buf, count, offset);
}

int TfsClient::fstat(const int fd, TfsFileStat* buf, const TfsStatType mode)
{
  return TfsClientImpl::Instance()->fstat(fd, buf, mode);
}

int TfsClient::close(const int fd, char* ret_tfs_name, const int32_t ret_tfs_name_len)
{
  return TfsClientImpl::Instance()->close(fd, ret_tfs_name, ret_tfs_name_len);
}

int64_t TfsClient::get_file_length(const int fd)
{
  return TfsClientImpl::Instance()->get_file_length(fd);
}

int TfsClient::set_option_flag(const int fd, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->set_option_flag(fd, option_flag);
}

int TfsClient:: unlink(int64_t& file_size, const char* file_name, const char* suffix,
                       const TfsUnlinkType action, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->unlink(file_size, file_name, suffix, action, option_flag);
}

int TfsClient::unlink(int64_t& file_size, const char* file_name, const char* suffix, const char* ns_addr,
                      const TfsUnlinkType action, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->unlink(file_size, file_name, suffix, ns_addr, action, option_flag);
}

#ifdef WITH_UNIQUE_STORE
int TfsClient::init_unique_store(const char* master_addr, const char* slave_addr,
                                 const char* group_name, const int32_t area, const char* ns_addr)
{
  return TfsClientImpl::Instance()->init_unique_store(master_addr, slave_addr, group_name, area, ns_addr);
}

int64_t TfsClient::save_buf_unique(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                               const char* buf, const int64_t count,
                               const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->save_buf_unique(ret_tfs_name, ret_tfs_name_len,
                                                buf, count, suffix, ns_addr);
}

int64_t TfsClient::save_file_unique(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                               const char* local_file,
                               const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->save_file_unique(ret_tfs_name, ret_tfs_name_len,
                                                local_file, suffix, ns_addr);
}

int32_t TfsClient::unlink_unique(int64_t& file_size, const char* file_name, const char* suffix,
                                 const char* ns_addr, const int32_t count)
{
  return TfsClientImpl::Instance()->unlink_unique(file_size, file_name, suffix, ns_addr, count);
}
#endif

void TfsClient::set_use_local_cache(const bool enable)
{
  return TfsClientImpl::Instance()->set_use_local_cache(enable);
}

void TfsClient::set_use_remote_cache(const bool enable)
{
  return TfsClientImpl::Instance()->set_use_remote_cache(enable);
}

void TfsClient::set_cache_items(const int64_t cache_items)
{
  return TfsClientImpl::Instance()->set_cache_items(cache_items);
}

int64_t TfsClient::get_cache_items() const
{
  return TfsClientImpl::Instance()->get_cache_items();
}

void TfsClient::set_cache_time(const int64_t cache_time)
{
  return TfsClientImpl::Instance()->set_cache_time(cache_time);
}

int64_t TfsClient::get_cache_time() const
{
  return TfsClientImpl::Instance()->get_cache_time();
}

#ifdef WITH_TAIR_CACHE
void TfsClient::set_remote_cache_info(const char * remote_cache_master_addr,
       const char* remote_cache_slave_addr, const char* remote_cache_group_name,
       const int32_t remote_cache_area)
{
  return TfsClientImpl::Instance()->set_remote_cache_info(remote_cache_master_addr,
           remote_cache_slave_addr, remote_cache_group_name, remote_cache_area);
}
#endif

void TfsClient::set_segment_size(const int64_t segment_size)
{
  return TfsClientImpl::Instance()->set_segment_size(segment_size);
}

int64_t TfsClient::get_segment_size() const
{
  return TfsClientImpl::Instance()->get_segment_size();
}

void TfsClient::set_batch_count(const int64_t batch_count)
{
  return TfsClientImpl::Instance()->set_batch_count(batch_count);
}

int64_t TfsClient::get_batch_count() const
{
  return TfsClientImpl::Instance()->get_batch_count();
}

void TfsClient::set_stat_interval(const int64_t stat_interval_ms)
{
  return TfsClientImpl::Instance()->set_stat_interval(stat_interval_ms);
}

int64_t TfsClient::get_stat_interval() const
{
  return TfsClientImpl::Instance()->get_stat_interval();
}

void TfsClient::set_gc_interval(const int64_t gc_interval_ms)
{
  return TfsClientImpl::Instance()->set_gc_interval(gc_interval_ms);
}

int64_t TfsClient::get_gc_interval() const
{
  return TfsClientImpl::Instance()->get_gc_interval();
}

void TfsClient::set_gc_expired_time(const int64_t gc_expired_time_ms)
{
  return TfsClientImpl::Instance()->set_gc_expired_time(gc_expired_time_ms);
}

int64_t TfsClient::get_gc_expired_time() const
{
  return TfsClientImpl::Instance()->get_gc_expired_time();
}

void TfsClient::set_batch_timeout(const int64_t timeout_ms)
{
  return TfsClientImpl::Instance()->set_batch_timeout(timeout_ms);
}

int64_t TfsClient::get_batch_timeout() const
{
  return TfsClientImpl::Instance()->get_batch_timeout();
}

void TfsClient::set_wait_timeout(const int64_t timeout_ms)
{
  return TfsClientImpl::Instance()->set_wait_timeout(timeout_ms);
}

int64_t TfsClient::get_wait_timeout() const
{
  return TfsClientImpl::Instance()->get_wait_timeout();
}

void TfsClient::set_client_retry_count(const int64_t count)
{
  return TfsClientImpl::Instance()->set_client_retry_count(count);
}

int64_t TfsClient::get_client_retry_count() const
{
  return TfsClientImpl::Instance()->get_client_retry_count();
}

void TfsClient::set_client_retry_flag(bool retry_flag)
{
  return TfsClientImpl::Instance()->set_client_retry_flag(retry_flag);
}

void TfsClient::set_log_level(const char* level)
{
  return TfsClientImpl::Instance()->set_log_level(level);
}

void TfsClient::set_log_file(const char* file)
{
  return TfsClientImpl::Instance()->set_log_file(file);
}

int32_t TfsClient::get_block_cache_time() const
{
  return TfsClientImpl::Instance()->get_block_cache_time();
}

int32_t TfsClient::get_block_cache_items() const
{
  return TfsClientImpl::Instance()->get_block_cache_items();
}

int32_t TfsClient::get_cache_hit_ratio(CacheType cache_type) const
{
  return TfsClientImpl::Instance()->get_cache_hit_ratio(cache_type);
}

uint64_t TfsClient::get_server_id()
{
  return TfsClientImpl::Instance()->get_server_id();
}

int32_t TfsClient::get_cluster_id(const char* ns_addr)
{
  return TfsClientImpl::Instance()->get_cluster_id(ns_addr);
}

int64_t TfsClient::save_buf(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                             const char* buf, const int64_t count,
                             const int32_t flag, const char* suffix, const char* ns_addr, const char* key)
{
  return TfsClientImpl::Instance()->save_buf(ret_tfs_name, ret_tfs_name_len,
                                              buf, count, flag, suffix, ns_addr, key);
}

int64_t TfsClient::save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                             const char* local_file, const int32_t flag, const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->save_file(ret_tfs_name, ret_tfs_name_len,
                                              local_file, flag, suffix, ns_addr);
}

int TfsClient::fetch_file(const char* local_file, const char* file_name,
                          const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->fetch_file(local_file, file_name, suffix, ns_addr);
}

int TfsClient::fetch_file(int64_t& ret_count, char* buf, const int64_t count, const char* file_name, const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->fetch_file(ret_count, buf, count, file_name, suffix, ns_addr);
}

int TfsClient::stat_file(TfsFileStat* file_stat, const char* file_name, const char* suffix,
                         const TfsStatType stat_type, const char* ns_addr)
{
  return TfsClientImpl::Instance()->stat_file(file_stat, file_name, suffix, stat_type, ns_addr);
}

int TfsClient::open(const char* file_name, const char* suffix, const int flags, const char* key)
{
  return open(file_name, suffix, NULL, flags, key);
}

int TfsClient::open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, const char* key)
{
  int ret = EXIT_INVALIDFD_ERROR;

  if ((flags & T_LARGE) && (flags & T_WRITE) && NULL == key)
  {
    TBSYS_LOG(ERROR, "open with T_LARGE|T_WRITE but without key");
  }
  else
  {
    ret = TfsClientImpl::Instance()->open(file_name, suffix, ns_addr, flags, key);
  }

  return ret;
}
