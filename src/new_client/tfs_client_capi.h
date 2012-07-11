/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_capi.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_C_API_H_
#define TFS_CLIENT_C_API_H_

#include <stdint.h>
#include <stdlib.h>

#if __cplusplus
extern "C"
{
#endif

#include "common/cdefine.h"

  /**
   * init tfs client
   * @param ns_addr  which cluster will be used
   * @param cache_time  the time which cache will be expired
   * @param cache_items  how many cache items in client block cache
   *
   * @return tfs client init success or fail
   */
  int t_initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items, const int start_bg);
  int t_set_default_server(const char* ns_addr, const int32_t cache_time, const int32_t cache_items);
  int t_destroy();

  /**
   * open tfs file
   * @param file_name  tfs file name
   * @param suffix  tfs file suffix
   * @param flags  open flags
   * @param local_key  set NULL if not use
   *
   * @return open tfs file success or fail
   */
  int t_open(const char* file_name, const char* suffix, const int flags, const char* local_key);

  /**
   * open tfs file
   * @param file_name  tfs file name
   * @param suffix  tfs file suffix
   * @param flags  open flags
   * @param local_key  set NULL if not use
   * @param ns_addr  set NULL if not use
   *
   * @return open tfs file success or fail
   */
  int t_open2(const char* file_name, const char* suffix, const char* ns_addr, const int flags, const char* local_key);

  int64_t t_read(const int fd, void* buf, const int64_t count);
  int64_t t_readv2(const int fd, void* buf, const int64_t count, TfsFileStat* file_info);
  int64_t t_write(const int fd, const void* buf, const int64_t count);
  int64_t t_lseek(const int fd, const int64_t offset, const int whence);
  int64_t t_pread(const int fd, void* buf, const int64_t count, const int64_t offset);
  int64_t t_pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
  int t_fstat(const int fd, TfsFileStat* buf, const TfsStatType mode);
  int64_t t_get_file_length(const int fd);

  /**
   * close tfs file
   * @param fd  file descriptor
   * @param tfs_name  used in T_WRITE mode, tfs name buffer. if the mode is not T_WRITE, set to NULL
   * @param len  used in T_WRITE mode, indicate the buffer length of tfs_name. if the mode is not T_WRITE, set to 0
   *
   * @return close tfs file success or fail
   */
  int t_close(const int fd, char* ret_tfs_name, const int32_t ret_tfs_name_len);

  /**
   * unlink tfs file
   * @param fd  file descriptor
   * @param tfs_name  tfs file name
   * @param suffix  tfs file suffix
   * @param action  unlink action.
   *
   * @return unlink tfs file success or fail
   */
  int t_unlink(const char* file_name, const char* suffix, int64_t* file_size, const TfsUnlinkType action);

  int t_unlink2(const char* file_name, const char* suffix,
                int64_t* file_size, const TfsUnlinkType action, const char* ns_addr);

  int t_set_option_flag(const int fd, const OptionFlag option_flag);

  void t_set_cache_items(const int64_t cache_items);
  int64_t t_get_cache_items();

  void t_set_cache_time(const int64_t cache_time);
  int64_t t_get_cache_time();

  void t_set_segment_size(const int64_t segment_size);
  int64_t t_get_segment_size();

  void t_set_batch_count(const int64_t batch_count);
  int64_t t_get_batch_count();

  void t_set_stat_interval(const int64_t stat_interval_ms);
  int64_t t_get_stat_interval();

  void t_set_gc_interval(const int64_t gc_interval_ms);
  int64_t t_get_gc_interval();

  void t_set_gc_expired_time(const int64_t gc_expired_time_ms);
  int64_t t_get_gc_expired_time();

  void t_set_batch_timeout(const int64_t timeout_ms);
  int64_t t_get_batch_timeout();

  void t_set_wait_timeout(const int64_t timeout_ms);
  int64_t t_get_wait_timeout();

  void t_set_client_retry_count(const int64_t count);
  int64_t t_get_client_retry_count();

  void t_set_log_level(const char* level);
  void t_set_log_file(const char* file);

  int32_t t_get_block_cache_time();
  int32_t t_get_block_cache_items();
  int32_t t_get_cache_hit_ratio();

#ifdef WITH_UNIQUE_STORE
  // unique stuff
  int t_init_unique_store(const char* master_addr, const char* slave_addr,
                          const char* group_name, const int32_t area, const char* ns_addr);
  int64_t t_save_unique_buf(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                            const char* buf, const int64_t count,
                            const char* suffix, const char* ns_addr);
  int64_t t_save_unique_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                             const char* local_file,
                             const char* suffix, const char* ns_addr);
  int32_t t_unlink_unique(int64_t* file_size, const char* file_name, const char* suffix,
                          const char* ns_addr, const int32_t count);
#endif

  // sort of utility
  uint64_t t_get_server_id();
  int32_t t_get_cluster_id();
  int64_t t_save_buf(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                     const char* buf, const int64_t count,
                     const int32_t flag, const char* suffix, const char* ns_addr, const char* key);
  int64_t t_save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
                      const char* local_file,
                      const int32_t flag, const char* suffix, const char* ns_addr);
  int t_fetch_buf(int64_t* ret_count, char* buf, const int64_t count, const char* file_name, const char* suffix, const char* ns_addr);
  int t_fetch_file(const char* local_file, const char* file_name, const char* suffix, const char* ns_addr);
  int t_stat_file(TfsFileStat* file_stat, const char* file_name, const char* suffix,
                  const TfsStatType stat_type, const char* ns_addr);
#if __cplusplus
}
#endif
#endif
