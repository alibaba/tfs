/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <Memory.hpp>
#include "tfs_session.h"
#include "bg_task.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "message/block_info_message.h"
#include "message/client_cmd_message.h"


using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

#ifdef WITH_TAIR_CACHE
TairCacheHelper* TfsSession::remote_cache_helper_ = NULL;
#endif

TfsSession::TfsSession(const std::string& nsip,
           const int64_t cache_time, const int64_t cache_items)
  :ns_addr_(0), ns_addr_str_(nsip), cluster_id_(0),
  block_cache_time_(cache_time), block_cache_items_(cache_items)
{
  if (cache_items <= 0)
  {
    ClientConfig::use_cache_ &= ~USE_CACHE_FLAG_LOCAL;
  }
  else
  {
    ClientConfig::use_cache_ |= USE_CACHE_FLAG_LOCAL;
  }
  if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
    block_cache_map_.resize(block_cache_items_);
#ifdef WITH_UNIQUE_STORE
  unique_store_ = NULL;
#endif
}

TfsSession::~TfsSession()
{
  block_cache_map_.clear();
#ifdef WITH_UNIQUE_STORE
  tbsys::gDelete(unique_store_);
#endif
}

int TfsSession::initialize()
{
  int ret = TFS_SUCCESS;
  if ((ns_addr_str_.empty()) || (ns_addr_str_.compare(" ") == 0))
  {
    TBSYS_LOG(ERROR, "nameserver address %s invalid", ns_addr_str_.c_str());
    ret = TFS_ERROR;
  }

  if (TFS_SUCCESS == ret)
  {
    ns_addr_ = Func::get_host_ip(ns_addr_str_.c_str());
    if (ns_addr_ <= 0)
    {
      TBSYS_LOG(ERROR, "nameserver address %s invalid", ns_addr_str_.c_str());
      ret = TFS_ERROR;
    }

    if (TFS_SUCCESS == ret)
    {
#ifndef TFS_TEST
      ret = get_cluster_id_from_ns();
#endif
    }
  }

  return ret;
}

void TfsSession::destroy()
{
#ifdef WITH_TAIR_CACHE
  tbsys::gDelete(remote_cache_helper_);
#endif
}

#ifdef WITH_TAIR_CACHE
int TfsSession::init_remote_cache_helper()
{
  int ret = TFS_ERROR;
  tbutil::Mutex::Lock lock(mutex_);

  if (NULL == remote_cache_helper_)
  {
    remote_cache_helper_ = new TairCacheHelper();
    ret = remote_cache_helper_->initialize(ClientConfig::remote_cache_master_addr_.c_str(),
            ClientConfig::remote_cache_slave_addr_.c_str(), ClientConfig::remote_cache_group_name_.c_str(),
            ClientConfig::remote_cache_area_);
    if (TFS_SUCCESS != ret)
    {
      tbsys::gDelete(remote_cache_helper_);
    }

    TBSYS_LOG(DEBUG, "init remote cache helper(master: %s, slave: %s, group_name: %s, area: %d) %s",
              ClientConfig::remote_cache_master_addr_.c_str(), ClientConfig::remote_cache_slave_addr_.c_str(),
              ClientConfig::remote_cache_group_name_.c_str(), ClientConfig::remote_cache_area_,
              TFS_SUCCESS == ret ? "success" : "fail");
  }
  else
  {
    TBSYS_LOG(DEBUG, "remote cache helper already init");
    ret = TFS_SUCCESS;
  }
  return ret;
}

bool TfsSession::check_init()
{
  return (NULL != remote_cache_helper_);
}

void TfsSession::insert_remote_block_cache(const uint32_t block_id, const VUINT64& rds)
{
  if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
  {
    int ret = TFS_SUCCESS;
    if (!check_init())
    {
      ret = init_remote_cache_helper();
    }
    if (TFS_SUCCESS == ret)
    {
      BlockCacheKey block_cache_key;
      BlockCacheValue block_cache_value;
      block_cache_key.set_key(ns_addr_, block_id);
      block_cache_value.set_ds_list(rds);
      ret = remote_cache_helper_->put(block_cache_key, block_cache_value);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "remote cache insert, blockid: %u", block_id);
      }
      else
      {
        TBSYS_LOG(WARN, "remote cache insert fail, blockid: %u, ret: %d", block_id, ret);
      }
    }
  }
}

int TfsSession::query_remote_block_cache(const uint32_t block_id, VUINT64& rds)
{
  int ret = TFS_SUCCESS;
  if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
  {
    if (!check_init())
    {
      ret = init_remote_cache_helper();
    }
    if (TFS_SUCCESS == ret)
    {
      BlockCacheKey block_cache_key;
      BlockCacheValue block_cache_value;
      block_cache_key.set_key(ns_addr_, block_id);
      ret = remote_cache_helper_->get(block_cache_key, block_cache_value);
      if (TFS_SUCCESS == ret)
      {
        if (block_cache_value.ds_.size() > 0)
        {
          rds = block_cache_value.ds_;
          TBSYS_LOG(DEBUG, "query remote cache, blockid: %u", block_id);
        }
        else
        {
          ret = TFS_ERROR;
        }
      }
    }
  }
  else
  {
    ret = TFS_ERROR;
  }
  return ret;
}

bool TfsSession::is_hit_remote_cache(const uint32_t block_id)
{
  int ret = TFS_ERROR;
  VUINT64 rds;
  ret = query_remote_block_cache(block_id, rds);

  return ret == TFS_SUCCESS ? true : false;

}

int TfsSession::query_remote_block_cache(const SEG_DATA_LIST& seg_list, int& remote_hit_count)
{
  int ret = TFS_SUCCESS;
  if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
  {
    if (!check_init())
    {
      ret = init_remote_cache_helper();
    }
    if (TFS_SUCCESS == ret)
    {
      std::vector<BlockCacheKey*> keys;
      BLK_CACHE_KV_MAP kv_data;
      std::map<uint32_t, size_t> block_list; // block_id ==> segment index
      for (size_t i = 0; i < seg_list.size(); i++)
      {
        if (CACHE_HIT_NONE == seg_list[i]->cache_hit_)
        {
          BlockCacheKey* block_cache_key = new BlockCacheKey();
          block_cache_key->set_key(ns_addr_, seg_list[i]->seg_info_.block_id_);
          keys.push_back(block_cache_key);
          block_list.insert(std::map<uint32_t, size_t>::value_type(seg_list[i]->seg_info_.block_id_, i));
        }
      }
      ret = remote_cache_helper_->mget(keys, kv_data);
      if (TFS_SUCCESS == ret)
      {
        uint32_t block_id = 0;
        size_t seg_idx = 0;
        BLK_CACHE_KV_MAP_ITER kv_iter = kv_data.begin();
        for (; kv_data.end() != kv_iter; kv_iter++)
        {
          const BlockCacheKey& key = kv_iter->first;
          const BlockCacheValue& value = kv_iter->second;
          block_id = key.block_id_;
          std::map<uint32_t, size_t>::const_iterator iter = block_list.find(block_id);
          if (block_list.end() != iter)
          {
            if (value.ds_.size() > 0)
            {
              seg_idx = iter->second;
              if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
              {
                insert_local_block_cache(seg_list[seg_idx]->seg_info_.block_id_, value.ds_);
              }
              seg_list[seg_idx]->cache_hit_ = CACHE_HIT_REMOTE;
              seg_list[seg_idx]->ds_ = value.ds_;
              seg_list[seg_idx]->reset_status();
              remote_hit_count++;
              BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::remote_cache_hit_, 1);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "get an unexpected block's ds list, something must be wrong!");
          }
        }
      }
      // release block cache keys
      std::vector<BlockCacheKey*>::iterator key_iter = keys.begin();
      for (; keys.end() != key_iter; key_iter++)
      {
        tbsys::gDelete(*key_iter);
      }
    }
  }
  else
  {
    ret = TFS_ERROR;
  }
  return ret;
}

void TfsSession::remove_remote_block_cache(const uint32_t block_id)
{
  if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
  {
    int ret = TFS_SUCCESS;
    if (!check_init())
    {
      ret = init_remote_cache_helper();
    }
    if (TFS_SUCCESS == ret)
    {
      BlockCacheKey block_cache_key;
      block_cache_key.set_key(ns_addr_, block_id);
      int ret = remote_cache_helper_->remove(block_cache_key);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "remote cache remove, blockid: %u", block_id);
        BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::remote_remove_count_, 1);
      }
    }
  }
}
#endif

#ifdef WITH_UNIQUE_STORE
int TfsSession::init_unique_store(const char* master_addr, const char* slave_addr,
                                  const char* group_name, const int32_t area)
{
  int ret = TFS_ERROR;
  tbutil::Mutex::Lock lock(mutex_);

  if (NULL == unique_store_)
  {
    unique_store_ = new TfsUniqueStore();
    ret = unique_store_->initialize(master_addr, slave_addr, group_name, area, ns_addr_str_.c_str());
    if (ret != TFS_SUCCESS)
    {
      // reinit later?
      tbsys::gDelete(unique_store_);
    }
  }
  else
  {
    // clear and reinit?
    TBSYS_LOG(DEBUG, "unique store already init");
    ret = TFS_SUCCESS;
  }

  return ret;
}
#endif

int TfsSession::get_block_info(SegmentData& seg_data, int32_t flag)
{
  int ret = TFS_SUCCESS;
  uint32_t& block_id = seg_data.seg_info_.block_id_;
  if (flag & T_UNLINK)
  {
    ret = get_block_info_ex(block_id, seg_data.ds_, T_WRITE);
  }
  else if (flag & T_WRITE)
  {
    if ((flag & T_NEWBLK) == 0)
    {
      flag |= T_CREATE;
    }
    ret = get_block_info_ex(block_id, seg_data.ds_, flag);
  }
  else // read
  {
    if (0 == block_id)
    {
      TBSYS_LOG(ERROR, "blockid zero error for blockid: %u, mode: %d", block_id, flag);
      ret = TFS_ERROR;
    }
    else
    {
      if (ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL)
      {
        // search in the local cache
        tbutil::Mutex::Lock lock(mutex_);
        BlockCache* block_cache = block_cache_map_.find(block_id);
        if (block_cache &&
            (block_cache->last_time_ >= time(NULL) - block_cache_time_) &&
            (block_cache->ds_.size() > 0))
        {
          TBSYS_LOG(DEBUG, "local cache hit, blockid: %u", block_id);
          BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::local_cache_hit_, 1);
          seg_data.ds_ = block_cache->ds_;
          seg_data.reset_status();
          seg_data.cache_hit_ = CACHE_HIT_LOCAL;
        }
        else
        {
          TBSYS_LOG(DEBUG, "local cache miss, blockid: %u", block_id);
          BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::local_cache_miss_, 1);
        }
      }

#ifdef WITH_TAIR_CACHE
      if (ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE)
      {
        if (CACHE_HIT_NONE == seg_data.cache_hit_)
        {
          // query remote tair cache
          ret = query_remote_block_cache(block_id, seg_data.ds_);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "remote cache hit, blockid: %u", block_id);
            BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::remote_cache_hit_, 1);
            if (ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL)
            {
              insert_local_block_cache(block_id, seg_data.ds_);
            }
            seg_data.reset_status();
            seg_data.cache_hit_ = CACHE_HIT_REMOTE;
          }
          else
          {
            TBSYS_LOG(DEBUG, "remote cache miss, blockid: %u", block_id);
            BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::remote_cache_miss_, 1);
          }
        }
      }
#endif

      // get block info from ns
      if (CACHE_HIT_NONE == seg_data.cache_hit_)
      {
        if ((ret = get_block_info_ex(block_id, seg_data.ds_, T_READ)) == TFS_SUCCESS)
        {
          if (seg_data.ds_.size() <= 0)
          {
            TBSYS_LOG(ERROR, "get block %u info failed, dataserver size %zd <= 0", block_id, seg_data.ds_.size());
            ret = TFS_ERROR;
          }
          else
          {
            seg_data.reset_status();
            // update cache
            if (ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL)
            {
              insert_local_block_cache(block_id, seg_data.ds_);
              seg_data.cache_hit_ = CACHE_HIT_LOCAL;
            }
#ifdef WITH_TAIR_CACHE
            if (ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE)
            {
              insert_remote_block_cache(block_id, seg_data.ds_);
              seg_data.cache_hit_ = CACHE_HIT_REMOTE;
            }
#endif
          }
        }
      }
    }
  }
  return ret;
}

int TfsSession::get_block_info(SEG_DATA_LIST& seg_list, int32_t flag)
{
  int ret = TFS_SUCCESS;
  if (flag & T_UNLINK)
  {
    ret = get_block_info_ex(seg_list, T_WRITE);
  }
  else if (flag & T_WRITE)
  {
    ret = get_block_info_ex(seg_list, flag | T_CREATE);
  }
  else
  {
    TBSYS_LOG(DEBUG, "get block info for read. seg size: %zd", seg_list.size());
    bool all_block_cached = false;
    size_t cached_block_count = 0;
    if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
    {
      // search in the local cache
      for (size_t i = 0; i < seg_list.size(); i++)
      {
        uint32_t block_id = seg_list[i]->seg_info_.block_id_;

        if (0 == block_id)
        {
          TBSYS_LOG(ERROR, "blockid: %u zero error for mode: %d", block_id, flag);
          ret = TFS_ERROR;
          break;
        }

        tbutil::Mutex::Lock lock(mutex_);
        BlockCache* block_cache = block_cache_map_.find(block_id);
        if (block_cache &&
            (block_cache->last_time_ >= time(NULL) - block_cache_time_) &&
            (block_cache->ds_.size() > 0))
        {
          seg_list[i]->ds_ = block_cache->ds_;
          seg_list[i]->reset_status();
          seg_list[i]->cache_hit_ = CACHE_HIT_LOCAL;
          cached_block_count++;
          BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::local_cache_hit_, 1);
        }
        else
        {
          BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::local_cache_miss_, 1);
        }
      }
      if (cached_block_count == seg_list.size())
      {
        TBSYS_LOG(DEBUG, "all block local cached. count: %zd", cached_block_count);
        all_block_cached = true;
      }
      else
      {
        TBSYS_LOG(DEBUG, "partial block local cached. count: %zd", cached_block_count);
      }
    }
#ifdef WITH_TAIR_CACHE
    if (TFS_SUCCESS == ret && (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_) && cached_block_count != seg_list.size())
    {
      // query remote tair cache
      int remote_hit_count = 0;
      ret = query_remote_block_cache(seg_list, remote_hit_count);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "partial block remote cached, count: %u", remote_hit_count);
        cached_block_count += remote_hit_count;
        if (cached_block_count == seg_list.size())
        {
          TBSYS_LOG(DEBUG, "all block cached. count: %zd", cached_block_count);
          all_block_cached = true;
        }
      }
      else
      {
        ret = TFS_SUCCESS;
      }
    }
#endif

    // get block info from ns
    if (TFS_SUCCESS == ret && !all_block_cached)
    {
      ret = get_block_info_ex(seg_list, T_READ);
    }
  }
  return ret;
}

int TfsSession::get_block_info_ex(uint32_t& block_id, VUINT64& rds, const int32_t flag)
{
  int ret = TFS_ERROR;
#ifdef TFS_TEST
  UNUSED(flag);
  if (0 == block_id) // write
  {
    if (block_ds_map_.size() > 0)
    {
      block_id = (block_ds_map_.begin())->first;
      rds = (block_ds_map_.begin())->second;
      ret = TFS_SUCCESS;
    }
    else
    {
      ret = EXIT_NO_BLOCK;
    }
  }
  else // read
  {
    std::map<uint32_t, VUINT64>::iterator iter = block_ds_map_.find(block_id);
    if (block_ds_map_.end() != iter)
    {
      rds = iter->second;
      ret = TFS_SUCCESS;
    }
    else
      ret = EXIT_BLOCK_NOT_FOUND;
  }
#else
  GetBlockInfoMessage gbi_message(flag);
  gbi_message.set_block_id(block_id);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  ret = send_msg_to_server(ns_addr_, client, &gbi_message, rsp);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "call get block info failed, blockid: %u ret: %d", block_id, ret);
  }
  else if (SET_BLOCK_INFO_MESSAGE == rsp->getPCode()) //rsp will not be null
  {
    ret = TFS_ERROR;
    SetBlockInfoMessage* block_info_msg = dynamic_cast<SetBlockInfoMessage*>(rsp);
    rds = block_info_msg->get_block_ds();
    block_id = block_info_msg->get_block_id();
    if (rds.size() && block_id > 0)
    {
      if (block_info_msg->has_lease())
      {
        rds.push_back(ULONG_LONG_MAX);
        rds.push_back(block_info_msg->get_block_version());
        rds.push_back(block_info_msg->get_lease_id());
      }
      ret = TFS_SUCCESS;
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      TBSYS_LOG(ERROR, "get block %u info fail, ret: %d, error: %s, status: %d",
                block_id, ret, dynamic_cast<StatusMessage*>(rsp)->get_error(), dynamic_cast<StatusMessage*>(rsp)->get_status());
      ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
    }
    else
    {
      TBSYS_LOG(ERROR, "get block %u info fail, ret: %d, msg type: %d",
                block_id, ret, rsp->getPCode());
    }
  }
  NewClientManager::get_instance().destroy_client(client);
#endif
  return ret;
}

int TfsSession::get_block_info_ex(SEG_DATA_LIST& seg_list, const int32_t flag)
{
  int ret = TFS_ERROR;
#ifdef TFS_TEST
  if (flag & T_READ) // read
  {
    size_t block_count = seg_list.size();
    for (size_t i = 0; i < block_count; i++)
    {
      // only query those miss from cache
      if (CACHE_HIT_NONE == seg_list[i]->cache_hit_)
      {
        uint32_t block_id = seg_list[i]->seg_info_.block_id_;
        std::map<uint32_t, VUINT64>::iterator iter = block_ds_map_.find(block_id);
        if (block_ds_map_.end() != iter)
        {
          seg_list[i]->ds_ = iter->second;
          seg_list[i]->reset_status();
          // update cache
          if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
          {
            insert_local_block_cache(block_id, iter->second);
            seg_list[i]->cache_hit_ = CACHE_HIT_LOCAL;
          }
#ifdef WITH_TAIR_CACHE
          if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
          {
            insert_remote_block_cache(block_id, iter->second);
            seg_list[i]->cache_hit_ = CACHE_HIT_REMOTE;
          }
#endif
          ret = TFS_SUCCESS;
        }
        else // should never happen
        {
          ret = EXIT_BLOCK_NOT_FOUND;
          break;
        }
      }
    }
    ret = TFS_SUCCESS;
  }
#else
  size_t block_count = 0;
  BatchGetBlockInfoMessage bgbi_message(flag);
  if (flag & T_WRITE)
  {
    block_count = seg_list.size();
    bgbi_message.set_block_count(block_count);
  }
  else
  {
    for (size_t i = 0; i < seg_list.size(); i++)
    {
      // only query those miss from cache
      if (CACHE_HIT_NONE == seg_list[i]->cache_hit_)
      {
        bgbi_message.add_block_id(seg_list[i]->seg_info_.block_id_);
        block_count++;
        TBSYS_LOG(DEBUG, "batch get block info, add block: %u, count: %zd", seg_list[i]->seg_info_.block_id_, block_count);
      }
    }
  }

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  ret = send_msg_to_server(ns_addr_, client, &bgbi_message, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get blockinfo failed, ret: %d", ret);
  }
  else if (BATCH_SET_BLOCK_INFO_MESSAGE == rsp->getPCode())
  {
    BatchSetBlockInfoMessage* block_info_msg = dynamic_cast<BatchSetBlockInfoMessage*>(rsp);
    map<uint32_t, BlockInfoSeg>& block_info = block_info_msg->get_infos();
    std::map<uint32_t, BlockInfoSeg>::iterator it;

    if (flag & T_READ)
    {
      uint32_t block_id = 0;
      for (size_t i = 0; i < seg_list.size(); ++i)
      {
        block_id = seg_list[i]->seg_info_.block_id_;
        if (CACHE_HIT_NONE == seg_list[i]->cache_hit_)
        {
          if ((it = block_info.find(block_id)) == block_info.end())
          {
            TBSYS_LOG(ERROR, "get block %u info fail, blockinfo size: %zd",
                      block_id, block_info.size());
            ret = TFS_ERROR;
            break;
          }
          else
          {
            if (it->second.ds_.empty())
            {
              TBSYS_LOG(ERROR, "get block %u info fail, ds list empty", block_id);
              ret = TFS_ERROR;
              break;
            }
            seg_list[i]->ds_ = it->second.ds_;
            seg_list[i]->reset_status();

            if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
            {
              insert_local_block_cache(block_id, it->second.ds_);
              seg_list[i]->cache_hit_ = CACHE_HIT_LOCAL;
            }
#ifdef WITH_TAIR_CACHE
            if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
            {
              insert_remote_block_cache(block_id, it->second.ds_);
              seg_list[i]->cache_hit_ = CACHE_HIT_REMOTE;
            }
#endif
            TBSYS_LOG(DEBUG, "get block %u info, ds size: %zd",
                      block_id, seg_list[i]->ds_.size());
          }
        }
      }
    }
    else if (flag & T_WRITE)
    {
      if (block_info.size() != block_count)
      {
        TBSYS_LOG(ERROR, "batch get write block info fail, get count conflict, request: %zd, response: %zd",
                  block_count, block_info.size());
        ret = TFS_ERROR;
      }
      else
      {
        it = block_info.begin();
        TBSYS_LOG(DEBUG, "get write block block count: %zd, seg list size: %zd", block_info.size(), seg_list.size());

        for (size_t i = 0; i < seg_list.size(); i++, it++)
        {
          seg_list[i]->seg_info_.block_id_ = it->first;
          seg_list[i]->ds_ = it->second.ds_;
          seg_list[i]->status_ = SEG_STATUS_OPEN_OVER;

          TBSYS_LOG(DEBUG, "get write block %u success, ds list size: %zd", seg_list[i]->seg_info_.block_id_, seg_list[i]->ds_.size());

          if (it->second.has_lease()) // should have
          {
            seg_list[i]->ds_.push_back(ULONG_LONG_MAX);
            seg_list[i]->ds_.push_back(it->second.version_);
            seg_list[i]->ds_.push_back(it->second.lease_id_);
            TBSYS_LOG(DEBUG, "get write block %u success, ds list size: %zd, version: %d, lease: %d",
                      seg_list[i]->seg_info_.block_id_, seg_list[i]->ds_.size(),
                      it->second.version_, it->second.lease_id_);
          }
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "unknown flag %d", flag);
      ret = TFS_ERROR;
    }
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      TBSYS_LOG(ERROR, "get batch block info fail, ret: %d, error: %s, status: %d",
                ret, dynamic_cast<StatusMessage*>(rsp)->get_error(), dynamic_cast<StatusMessage*>(rsp)->get_status());
    }
    else
    {
      TBSYS_LOG(ERROR, "get batch block info fail, ret: %d, msg type: %d",
                ret, rsp->getPCode());
    }
  }
  NewClientManager::get_instance().destroy_client(client);
#endif
  return ret;
}

int TfsSession::get_cluster_id_from_ns()
{
  ClientCmdMessage cc_message;
  cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
  cc_message.set_value3(20);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(ns_addr_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get cluster id from ns fail, ret: %d", ret);
  }
  else if (STATUS_MESSAGE == rsp->getPCode())
  {
    StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
    //ugly use error msg
    if (status_msg->get_status() == STATUS_MESSAGE_OK &&
        strlen(status_msg->get_error()) > 0)
    {
      char cluster_id = static_cast<char> (atoi(status_msg->get_error()));
      if (isdigit(cluster_id) || isalpha(cluster_id))
      {
        cluster_id_ = cluster_id - '0';
        TBSYS_LOG(INFO, "get cluster id from nameserver success. cluster id: %d", cluster_id_);
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster id from nameserver fail. cluster id: %c", cluster_id);
        ret = TFS_ERROR;
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get cluster id from ns failed, msg type error. type: %d", rsp->getPCode());
    ret = EXIT_UNKNOWN_MSGTYPE;
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int TfsSession::get_cluster_group_count_from_ns()
{
  ClientCmdMessage cc_message;
  cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
  cc_message.set_value3(22);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(ns_addr_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get cluster group count from ns fail, ret: %d", ret);
    ret = -1;
  }
  else if (STATUS_MESSAGE == rsp->getPCode())
  {
    StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
    if (status_msg->get_status() == STATUS_MESSAGE_OK &&
        strlen(status_msg->get_error()) > 0)
    {
      ret = atoi(status_msg->get_error());
      if (ret > 0)
      {
        TBSYS_LOG(INFO, "get cluster group count from nameserver success. cluster group count: %d", ret);
      }
      else
      {
        TBSYS_LOG(WARN, "get cluster group count from nameserver fail.");
        return DEFAULT_CLUSTER_GROUP_COUNT;
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get cluster group count from nameserver failed, msg type error. type: %d", rsp->getPCode());
    ret = -1;
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int TfsSession::get_cluster_group_seq_from_ns()
{
  ClientCmdMessage cc_message;
  cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
  cc_message.set_value3(23);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(ns_addr_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get cluster group seq from ns fail, ret: %d", ret);
    ret = -1;
  }
  else if (STATUS_MESSAGE == rsp->getPCode())
  {
    StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
    if (status_msg->get_status() == STATUS_MESSAGE_OK &&
        strlen(status_msg->get_error()) > 0)
    {
      ret = atoi(status_msg->get_error());
      if (ret >= 0)
      {
        TBSYS_LOG(INFO, "get cluster group seq from nameserver success. cluster group seq: %d", ret);
      }
      else
      {
        TBSYS_LOG(WARN, "get cluster group seq from nameserver fail.");
        return DEFAULT_CLUSTER_GROUP_SEQ;
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get cluster group seq from nameserver failed, msg type error. type: %d", rsp->getPCode());
    ret = -1;
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

void TfsSession::insert_local_block_cache(const uint32_t block_id, const VUINT64& rds)
{
  if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
  {
    TBSYS_LOG(DEBUG, "local cache insert, blockid: %u", block_id);
    BlockCache block_cache;
    block_cache.last_time_ = time(NULL);
    block_cache.ds_ = rds;
    tbutil::Mutex::Lock lock(mutex_);
    block_cache_map_.insert(block_id, block_cache);
  }
}

void TfsSession::remove_local_block_cache(const uint32_t block_id)
{
  if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
  {
    TBSYS_LOG(DEBUG, "local cache remove, blockid: %u", block_id);
    BgTask::get_stat_mgr().update_entry(StatItem::client_cache_stat_, StatItem::local_remove_count_, 1);
    tbutil::Mutex::Lock lock(mutex_);
    block_cache_map_.remove(block_id);
  }
}

bool TfsSession::is_hit_local_cache(const uint32_t block_id)
{
  bool ret = false;
  if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
  {
    tbutil::Mutex::Lock lock(mutex_);
    BlockCache* block_cache = block_cache_map_.find(block_id);
    if (block_cache
       && (block_cache->last_time_ >= time(NULL) - block_cache_time_)
       && (block_cache->ds_.size() > 0))
    {
      TBSYS_LOG(DEBUG, "local cache hit, blockid: %u", block_id);
      ret = true;
    }
  }
  return ret;
}
