/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_cache_helper.h 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   mingyan.zc <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TAIRCACHEHELPER_H_
#define TFS_CLIENT_TAIRCACHEHELPER_H_

#include "tair_client_api.hpp"
#include "define.hpp"

#include "common/internal.h"

namespace tfs
{
  namespace client
  {
    enum { TAIR_TAG_LENGTH = 2 }; // tair tag length, for compatible with tair java client in serialization

    inline void add_tair_cache_tag(char* buf, int64_t& pos)
    {
      if (buf != NULL)
      {
        buf[pos] = '\0';
        buf[pos+1] = '\02';
        pos += TAIR_TAG_LENGTH;
      }
    }

    struct BlockCacheKey
    {
      BlockCacheKey();
      ~BlockCacheKey();

      int deserialize(const char*data, const int64_t data_len, int64_t& pos);
      int serialize(char *data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      void set_key(const int64_t& ns_addr, const uint32_t& block_id);

      bool operator<(const BlockCacheKey& rhs) const
      {
        if (ns_addr_ == rhs.ns_addr_)
        {
          return block_id_ < rhs.block_id_;
        }
        return ns_addr_ < rhs.ns_addr_;
      }

      int64_t ns_addr_;
      uint32_t block_id_;
    };

    struct BlockCacheValue
    {
      BlockCacheValue();
      ~BlockCacheValue();

      int deserialize(const char *data, const int64_t data_len, int64_t& pos);
      int serialize(char *data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      void set_ds_list(const common::VUINT64& ds);
      void clear();

      int32_t version_;
      common::VUINT64 ds_;
    };

    typedef std::vector<BlockCacheKey*> BLK_CACHE_KEY_VEC;
    typedef BLK_CACHE_KEY_VEC::iterator BLK_CACHE_KEY_VEC_ITER;
    typedef std::map<BlockCacheKey, BlockCacheValue> BLK_CACHE_KV_MAP;
    typedef BLK_CACHE_KV_MAP::iterator BLK_CACHE_KV_MAP_ITER;

    class TairCacheHelper
    {
    public:
      TairCacheHelper();
      virtual ~TairCacheHelper();

      int initialize(const char* master_addr, const char* slave_addr,
                     const char* group_name, const int32_t area);

      int put(const BlockCacheKey& key, const BlockCacheValue& value);
      int get(const BlockCacheKey& key, BlockCacheValue& value);
      int remove(BlockCacheKey& key);

      int mget(const BLK_CACHE_KEY_VEC & keys, BLK_CACHE_KV_MAP & kv_data);
#ifdef TFS_TEST
      BLK_CACHE_KV_MAP remote_block_cache_;
#endif

    private:
      DISALLOW_COPY_AND_ASSIGN(TairCacheHelper);
      const static int32_t TAIR_CLIENT_TRY_COUNT = 2;
      const static int32_t DEFAULT_EXPIRE_TIME = 0;

    private:
      tair::tair_client_api* tair_client_;
      int32_t area_;
    };
  }
}

#endif
