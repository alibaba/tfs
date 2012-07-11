/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software_; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#include "client_config.h"
#include "common/internal.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

string StatItem::client_access_stat_ = "client_access_stat";
string StatItem::read_success_ = "read_success";
string StatItem::read_fail_ = "read_fail";
string StatItem::stat_success_ = "stat_success";
string StatItem::stat_fail_ = "stat_fail";
string StatItem::write_success_ = "write_success";
string StatItem::write_fail_ = "write_fail";
string StatItem::unlink_success_ = "unlink_success";
string StatItem::unlink_fail_ = "unlink_fail";
string StatItem::client_cache_stat_ = "client_cache_stat";
string StatItem::local_cache_hit_ = "local_cache_hit";
string StatItem::local_cache_miss_ = "local_cache_miss";
string StatItem::local_remove_count_ = "local_remove_count";
#ifdef WITH_TAIR_CACHE
string StatItem::remote_cache_hit_ = "remote_cache_hit";
string StatItem::remote_cache_miss_ = "remote_cache_miss";
string StatItem::remote_remove_count_ = "remote_remove_count";
#endif

int32_t ClientConfig::use_cache_ = USE_CACHE_FLAG_LOCAL;
int64_t ClientConfig::cache_items_ = DEFAULT_BLOCK_CACHE_ITEMS;
int64_t ClientConfig::cache_time_ = DEFAULT_BLOCK_CACHE_TIME;
#ifdef WITH_TAIR_CACHE
string ClientConfig::remote_cache_master_addr_ = "";
string ClientConfig::remote_cache_slave_addr_ = "";
string ClientConfig::remote_cache_group_name_ = "";
int32_t ClientConfig::remote_cache_area_ = 0;
#endif
int64_t ClientConfig::segment_size_ = MAX_SEGMENT_SIZE;
int64_t ClientConfig::batch_count_ = MAX_BATCH_COUNT / 2;
int64_t ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
int64_t ClientConfig::client_retry_count_ = DEFAULT_CLIENT_RETRY_COUNT; // retry times to read or write
bool ClientConfig::client_retry_flag_ = true;
// interval unit: ms
int64_t ClientConfig::stat_interval_ = DEFAULT_STAT_INTERNAL;
int64_t ClientConfig::gc_interval_ = DEFAULT_GC_INTERNAL;
int64_t ClientConfig::expired_time_ = MIN_GC_EXPIRED_TIME * 4;
int64_t ClientConfig::batch_timeout_ = DEFAULT_NETWORK_CALL_TIMEOUT; // wait several response timeout
int64_t ClientConfig::wait_timeout_ = DEFAULT_NETWORK_CALL_TIMEOUT;  // wait single response timeout
