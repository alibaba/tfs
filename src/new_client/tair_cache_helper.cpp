/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_cache_helper.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   mingyan.zc <mingyan.zc@taobao.com>
 *      - initial release
 *
 */

#include "Memory.hpp"

#include "common/serialization.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "tair_cache_helper.h"

using namespace tair;
using namespace tfs::common;

namespace tfs
{
  namespace client
  {
    //////////////////////
    // BlockCacheKey
    //////////////////////
    BlockCacheKey::BlockCacheKey() : ns_addr_(0), block_id_(0)
    {
    }

    BlockCacheKey::~BlockCacheKey()
    {
    }

    int BlockCacheKey::serialize(char *data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;;
      if (TFS_SUCCESS == ret)
      {
        add_tair_cache_tag(data, pos);
        ret = Serialization::set_int64(data, data_len, pos, ns_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      return ret;
    }

    int BlockCacheKey::deserialize(const char *data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;;
      if (TFS_SUCCESS == ret)
      {
        pos += TAIR_TAG_LENGTH;
        ret = Serialization::get_int64(data, data_len, pos, &ns_addr_);
        TBSYS_LOG(DEBUG, "deserialize ns addr: %s", Func::addr_to_str(ns_addr_, true).c_str());

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
          TBSYS_LOG(DEBUG, "deserialize block id: %d", block_id_);
        }

      }
      return ret;
    }

    void BlockCacheKey::set_key(const int64_t& ns_addr, const uint32_t& block_id)
    {
      ns_addr_ = ns_addr;
      block_id_ = block_id;
    }

    int64_t BlockCacheKey::length() const
    {
      return TAIR_TAG_LENGTH + INT_SIZE + INT64_SIZE;
    }

    //////////////////////
    // BlockCacheValue
    //////////////////////
    BlockCacheValue::BlockCacheValue() : version_(0)
    {
    }

    BlockCacheValue::~BlockCacheValue()
    {
    }

    int BlockCacheValue::serialize(char *data, const int64_t data_len, int64_t& pos) const
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;;
      if (TFS_SUCCESS == ret)
      {
        add_tair_cache_tag(data, pos);

        int32_t ds_count = ds_.size();
        ret = Serialization::set_int32(data, data_len, pos, ds_count);
        TBSYS_LOG(DEBUG, "serialize ds_count: %d", ds_count);

        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < ds_count; i++)
          {
            Serialization::set_int64(data, data_len, pos, ds_[i]);
            TBSYS_LOG(DEBUG, "serialize ds: %s", Func::addr_to_str(ds_[i], true).c_str());
          }
        }
      }
      return ret;
    }

    int BlockCacheValue::deserialize(const char *data, const int64_t data_len, int64_t& pos)
    {
      int ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;;
      if (TFS_SUCCESS == ret)
      {
        pos += TAIR_TAG_LENGTH;
        int32_t ds_count = 0;
        ret = Serialization::get_int32(data, data_len, pos, &ds_count);
        TBSYS_LOG(DEBUG, "deserialize ds count: %d", ds_count);

        if (TFS_SUCCESS == ret)
        {
          ds_.clear();
          int64_t ds_addr = 0;
          for (int32_t i = 0; i < ds_count; i++)
          {
            ret = Serialization::get_int64(data, data_len, pos, &ds_addr);
            TBSYS_LOG(DEBUG, "deserialize ds: %s", Func::addr_to_str(ds_addr, true).c_str());
            ds_.push_back(ds_addr);
          }
        }
      }
      return ret;
    }

    int64_t BlockCacheValue::length() const
    {
      return TAIR_TAG_LENGTH + INT_SIZE + INT64_SIZE * ds_.size();
    }

    void BlockCacheValue::set_ds_list(const VUINT64& ds)
    {
      ds_ = ds;
    }

    void BlockCacheValue::clear()
    {
      version_ = 0;
      ds_.clear();
    }

    //////////////////////
    // TairCacheHelper
    //////////////////////
    TairCacheHelper::TairCacheHelper() :
      tair_client_(NULL), area_(0)
    {
    }

    TairCacheHelper::~TairCacheHelper()
    {
      tbsys::gDelete(tair_client_);
    }

    int TairCacheHelper::initialize(const char* master_addr, const char* slave_addr,
                                      const char* group_name, const int32_t area)
    {
#ifdef TFS_TEST
      UNUSED(master_addr);
      UNUSED(slave_addr);
      UNUSED(group_name);
      UNUSED(area);
      return TFS_SUCCESS;
#else
      int ret = TFS_ERROR;

      if (area < 0)
      {
        TBSYS_LOG(WARN, "invalid area: %d", area);
      }
      else
      {
        tbsys::gDelete(tair_client_);
        tair_client_ = new tair_client_api();

        if (!tair_client_->startup(master_addr, slave_addr, group_name))
        {
          TBSYS_LOG(WARN, "starup tair client fail. master addr: %s, slave addr: %s, group name: %s, area: %d",
                    master_addr, slave_addr, group_name, area);
        }
        else
        {
          ret = TFS_SUCCESS;
          tair_client_->set_timeout(DEFAULT_TAIR_TIMEOUT);
          area_ = area;
        }
      }

      return ret;
#endif
    }

    int TairCacheHelper::get(const BlockCacheKey& key, BlockCacheValue& value)
    {
#ifdef TFS_TEST
      std::map<BlockCacheKey, BlockCacheValue>::iterator iter = remote_block_cache_.find(key);
      if (remote_block_cache_.end() != iter)
      {
        value = iter->second;
        return TFS_SUCCESS;
      }
      else
      {
        return TFS_ERROR;
      }
#else
      int64_t key_len = key.length();
      int ret = key_len > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* key_buf = new char[key_len];
        int64_t pos = 0;
        ret = key.serialize(key_buf, key_len, pos);
        if (TFS_SUCCESS == ret)
        {
          data_entry* key_entry = new data_entry(key_buf, key_len, false);
          data_entry* value_entry = NULL;
          int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
          do
          {
            ret = tair_client_->get(area_, *key_entry, value_entry);
          } while (TAIR_RETURN_TIMEOUT == ret && --retry_count > 0);

          if (TAIR_RETURN_SUCCESS == ret)
          {
            // deserialize value
            int64_t value_len = value_entry->get_size();
            char* value_buf = value_entry->get_data();

            // get value version
            value.version_ = value_entry->get_version();
            TBSYS_LOG(DEBUG, "deserialize value. value len: %"PRI64_PREFIX"d, version: %d", value_len, value.version_);

            pos = 0;
            ret = value.deserialize(value_buf, value_len, pos);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize value fail. ret: %d", ret);
              ret = TFS_ERROR;
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "get value from tair fail, ret: %d", ret);
            ret = TFS_ERROR;
          }
          tbsys::gDelete(key_entry);
          tbsys::gDelete(value_entry);
        }
        else
        {
          TBSYS_LOG(WARN, "serialiaze key fail. ret: %d", ret);
        }
        delete [] key_buf;
      }

      return ret;
#endif
    }

    int TairCacheHelper::put(const BlockCacheKey& key, const BlockCacheValue& value)
    {
#ifdef TFS_TEST
      remote_block_cache_.insert(BLK_CACHE_KV_MAP::value_type(key, value));
      return TFS_SUCCESS;
#else
      int64_t key_len = key.length();
      int64_t value_len = value.length();
      int ret = key_len > 0 && value_len > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* key_buf = new char[key_len];
        int64_t pos = 0;
        ret = key.serialize(key_buf, key_len, pos);
        if (TFS_SUCCESS == ret)
        {
          char* value_buf = new char[value_len];
          pos = 0;
          ret = value.serialize(value_buf, value_len, pos);
          if (TFS_SUCCESS == ret)
          {
            data_entry* key_entry = new data_entry(key_buf, key_len, false);
            data_entry* value_entry = new data_entry(value_buf, value_len, false);
            int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
            do
            {
              ret = tair_client_->put(area_, *key_entry, *value_entry, DEFAULT_EXPIRE_TIME, value.version_);
            } while (TAIR_RETURN_TIMEOUT == ret && --retry_count > 0);

            if (TAIR_RETURN_VERSION_ERROR == ret)
            {
              TBSYS_LOG(WARN, "put to tair version error.");
            }

            tbsys::gDelete(key_entry);
            tbsys::gDelete(value_entry);

            ret = TAIR_RETURN_SUCCESS == ret || TAIR_RETURN_VERSION_ERROR == ret ? TFS_SUCCESS : TFS_ERROR;
          }
          else
          {
            TBSYS_LOG(WARN, "serialize value fail. ret: %d", ret);
          }
          tbsys::gDelete(value_buf);
        }
        else
        {
          TBSYS_LOG(WARN, "serialiaze key fail. ret: %d", ret);
        }
        delete [] key_buf;
      }

      return ret;
#endif
    }

    int TairCacheHelper::remove(BlockCacheKey& key)
    {
#ifdef TFS_TEST
      int ret = remote_block_cache_.erase(key);
      ret = (1 == ret) ? TFS_SUCCESS : TFS_ERROR;
      return ret;
#else
      int64_t key_len = key.length();
      int ret = key_len > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* key_buf = new char[key_len];
        int64_t pos = 0;
        ret = key.serialize(key_buf, key_len, pos);
        if (TFS_SUCCESS == ret)
        {
          data_entry* key_entry = new data_entry(key_buf, key_len, false);
          int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
          do
          {
            ret = tair_client_->remove(area_, *(key_entry));
          } while (TAIR_RETURN_TIMEOUT == ret && --retry_count > 0);

          tbsys::gDelete(key_entry);

          ret = TAIR_RETURN_SUCCESS == ret ? TFS_SUCCESS : TFS_ERROR;
        }
        else
        {
          TBSYS_LOG(WARN, "serialize key fail. ret: %d", ret);
        }
        delete [] key_buf;
      }

      return ret;
#endif
    }

    int TairCacheHelper::mget(const BLK_CACHE_KEY_VEC & keys, BLK_CACHE_KV_MAP & kv_data)
    {
#ifdef TFS_TEST
      BLK_CACHE_KEY_VEC::const_iterator key_iter = keys.begin();
      for (; keys.end() != key_iter; key_iter++)
      {
        BLK_CACHE_KV_MAP::iterator iter = remote_block_cache_.find(**key_iter);
        if (remote_block_cache_.end() != iter)
        {
          BlockCacheKey blk_cache_key;
          BlockCacheValue blk_cache_value;
          blk_cache_key.set_key(iter->first.ns_addr_, iter->first.block_id_);
          blk_cache_value.set_ds_list(iter->second.ds_);
          kv_data.insert(BLK_CACHE_KV_MAP::value_type(blk_cache_key, blk_cache_value));
        }
      }
      return TFS_SUCCESS;
#else
      int ret = keys.size() > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        vector<data_entry *> key_entries;
        BLK_CACHE_KEY_VEC::const_iterator key_iter = keys.begin();
        int64_t key_len = (*key_iter)->length();
        ret = key_len > 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == ret)
        {
          char* key_buf = new char[key_len];
          int64_t pos = 0;
          for (; keys.end() != key_iter; key_iter++)
          {
            pos = 0;
            ret = (*key_iter)->serialize(key_buf, key_len, pos);
            if (TFS_SUCCESS == ret)
            {
              data_entry* key_entry = new data_entry(key_buf, key_len, true); // tair will alloc mem
              key_entries.push_back(key_entry);
            }
            else
            {
              break;
            }
          }

          if (TFS_SUCCESS == ret)
          {
            // mget
            tair_keyvalue_map kv_entries;
            int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
            do
            {
              ret = tair_client_->mget(area_, key_entries, kv_entries);
            } while (TAIR_RETURN_TIMEOUT == ret && --retry_count > 0);

            if (TAIR_RETURN_SUCCESS == ret || TAIR_RETURN_PARTIAL_SUCCESS == ret)
            {
              tair_keyvalue_map::iterator kv_entries_iter = kv_entries.begin();
              int64_t get_key_len = 0;
              int64_t get_value_len = 0;
              char* get_key_buf = NULL;
              char* get_value_buf = NULL;
              for (; kv_entries.end() != kv_entries_iter;)
              {
                data_entry* key = kv_entries_iter->first;
                data_entry* value = kv_entries_iter->second;

                BlockCacheKey blk_cache_key;
                BlockCacheValue blk_cache_value;

                // deserialize key
                get_key_len = key->get_size();
                get_key_buf = key->get_data();

                TBSYS_LOG(DEBUG, "deserialize key. key len: %"PRI64_PREFIX"d", get_key_len);
                pos = 0;
                ret = blk_cache_key.deserialize(get_key_buf, get_key_len, pos);
                if (TFS_SUCCESS == ret)
                {
                  // deserialize value
                  get_value_len = value->get_size();
                  get_value_buf = value->get_data();

                  // get value version
                  blk_cache_value.version_ = value->get_version();
                  TBSYS_LOG(DEBUG, "deserialize value. value len: %"PRI64_PREFIX"d, version: %d", get_value_len, blk_cache_value.version_);

                  pos = 0;
                  ret = blk_cache_value.deserialize(get_value_buf, get_value_len, pos);
                  if (TFS_SUCCESS == ret)
                  {
                    kv_data.insert(BLK_CACHE_KV_MAP::value_type(blk_cache_key, blk_cache_value));
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialize value fail. ret: %d", ret);
                  }
                }
                else
                {
                  TBSYS_LOG(WARN, "deserialize key fail. ret: %d", ret);
                }
                //kv_entries.erase(kv_entries_iter++);
                kv_entries_iter++;
                tbsys::gDelete(key);
                tbsys::gDelete(value);
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "get value from tair fail, ret: %d", ret);
              ret = TFS_ERROR;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "serialize key fail. ret: %d", ret);
          }

          // release key data_entry
          vector<data_entry *>::iterator key_entry_iter = key_entries.begin();
          for (; key_entry_iter != key_entries.end(); key_entry_iter++)
          {
            tbsys::gDelete(*key_entry_iter);
          }
          delete [] key_buf;
        }
      }

      return ret;
#endif
    }

  }
}


