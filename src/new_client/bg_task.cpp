/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bg_task.cpp 520 2011-06-20 06:05:52Z daoan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */

#include "bg_task.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

tbutil::TimerPtr BgTask::timer_ = 0;
StatManager<string, string, StatEntry> BgTask::stat_mgr_;
GcManager BgTask::gc_mgr_;

int BgTask::initialize()
{
  int ret = TFS_SUCCESS;
  if (timer_ == 0)
  {
    timer_ = new tbutil::Timer();
  }
  ret = stat_mgr_.initialize(timer_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize stat manager fail. ret: %d", ret);
  }
  else
  {
    int64_t current = tbsys::CTimeUtil::getTime();
    StatEntry<string, string>::StatEntryPtr access_ptr =
      new StatEntry<string, string>(StatItem::client_access_stat_, current, true);
    access_ptr->add_sub_key(StatItem::read_success_);
    access_ptr->add_sub_key(StatItem::read_fail_);
    access_ptr->add_sub_key(StatItem::stat_success_);
    access_ptr->add_sub_key(StatItem::stat_fail_);
    access_ptr->add_sub_key(StatItem::write_success_);
    access_ptr->add_sub_key(StatItem::write_fail_);
    access_ptr->add_sub_key(StatItem::unlink_success_);
    access_ptr->add_sub_key(StatItem::unlink_fail_);

    StatEntry<string, string>::StatEntryPtr cache_ptr =
      new StatEntry<string, string>(StatItem::client_cache_stat_, current, true);
    cache_ptr->add_sub_key(StatItem::local_cache_hit_);
    cache_ptr->add_sub_key(StatItem::local_cache_miss_);
    cache_ptr->add_sub_key(StatItem::local_remove_count_);
#ifdef WITH_TAIR_CACHE
    cache_ptr->add_sub_key(StatItem::remote_cache_hit_);
    cache_ptr->add_sub_key(StatItem::remote_cache_miss_);
    cache_ptr->add_sub_key(StatItem::remote_remove_count_);
#endif

    stat_mgr_.add_entry(access_ptr, ClientConfig::stat_interval_ * 1000);
    stat_mgr_.add_entry(cache_ptr, ClientConfig::stat_interval_ * 1000);
  }

  if (TFS_SUCCESS == ret)
  {
    gc_mgr_.initialize(timer_, ClientConfig::gc_interval_);
  }
  return ret;
}

int32_t BgTask::get_cache_hit_ratio(CacheType cache_type)
{
  uint64_t cache_hit = 0;
  uint64_t cache_miss = 0;

  if (!stat_mgr_.is_init())
  {
    TBSYS_LOG(WARN, "stat bgtask not init. no cache statistics");
  }
  else
  {
    if (LOCAL_CACHE == cache_type)
    {
      cache_hit = stat_mgr_.get_stat_value(StatItem::client_cache_stat_, StatItem::local_cache_hit_);
      cache_miss = stat_mgr_.get_stat_value(StatItem::client_cache_stat_, StatItem::local_cache_miss_);
    }
#ifdef WITH_TAIR_CACHE
    else if (REMOTE_CACHE == cache_type)
    {
      cache_hit = stat_mgr_.get_stat_value(StatItem::client_cache_stat_, StatItem::remote_cache_hit_);
      cache_miss = stat_mgr_.get_stat_value(StatItem::client_cache_stat_, StatItem::remote_cache_miss_);
    }
#endif

  }

  uint64_t cache_count = cache_hit + cache_miss;
  return cache_count > 0 ? (cache_hit * 100) / cache_count : 0;
}

int BgTask::wait_for_shut_down()
{
  stat_mgr_.wait_for_shut_down();
  gc_mgr_.wait_for_shut_down();
  if (timer_ != 0)
  {
    timer_ = 0;
  }
  return TFS_SUCCESS;
}

int BgTask::destroy()
{
  stat_mgr_.destroy();
  gc_mgr_.destroy();
  if (timer_ != 0)
  {
    timer_->destroy();
  }

  return TFS_SUCCESS;
}
