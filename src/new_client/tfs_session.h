/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSSESSION_H_
#define TFS_CLIENT_TFSSESSION_H_

#include <Mutex.h>

#include "common/internal.h"
#include "lru.h"
#include "local_key.h"

#ifdef WITH_TAIR_CACHE
#include "tair_cache_helper.h"
#endif

#ifdef WITH_UNIQUE_STORE
#include "tfs_unique_store.h"
#endif

namespace tfs
{
  namespace client
  {
    struct BlockCache
    {
      time_t last_time_;
      common::VUINT64 ds_;
    };

    class TfsSession
    {
#ifdef TFS_TEST
    public:
#endif
      typedef lru<uint32_t, BlockCache> BLOCK_CACHE_MAP;
      typedef BLOCK_CACHE_MAP::iterator BLOCK_CACHE_MAP_ITER;
    public:
			TfsSession(const std::string& nsip, const int64_t cache_time, const int64_t cache_items);
      virtual ~TfsSession();

      int initialize();
      static void destroy();
      int get_block_info(SegmentData& seg_data, int32_t flag);
      int get_block_info(SEG_DATA_LIST& seg_list, const int32_t flag);

      void insert_local_block_cache(const uint32_t block_id, const common::VUINT64& rds);
      void remove_local_block_cache(const uint32_t block_id);
      bool is_hit_local_cache(const uint32_t block_id);

      inline int32_t get_cluster_id() const
      {
        return cluster_id_;
      }

      inline const std::string& get_ns_addr_str() const
      {
        return ns_addr_str_;
      }

      inline const uint64_t get_ns_addr() const
      {
        return ns_addr_;
      }

      inline const int64_t get_cache_time() const
      {
        return block_cache_time_;
      }

      inline const int64_t get_cache_items() const
      {
        return block_cache_items_;
      }

#ifdef WITH_UNIQUE_STORE
    private:
      TfsUniqueStore* unique_store_;
    public:
      int init_unique_store(const char* master_addr, const char* slave_addr,
                            const char* group_name, const int32_t area);
      inline TfsUniqueStore* get_unique_store() const
      {
        return unique_store_;
      }
#endif

#ifdef TFS_TEST
      BLOCK_CACHE_MAP* get_block_cache_map()
      {
        return &block_cache_map_;
      }
#ifdef WITH_TAIR_CACHE
      TairCacheHelper* get_remote_cache_helper()
      {
        return remote_cache_helper_;
      }
#endif
#endif

    private:
      TfsSession();
      DISALLOW_COPY_AND_ASSIGN(TfsSession);
      int get_block_info_ex(SEG_DATA_LIST& seg_list, const int32_t flag);
      int get_block_info_ex(uint32_t& block_id, common::VUINT64& rds, const int32_t flag);
      int get_cluster_id_from_ns();
    public:
      int get_cluster_group_count_from_ns();
      int get_cluster_group_seq_from_ns();

    private:
      tbutil::Mutex mutex_;
#ifdef TFS_TEST
    public:
      std::map<uint32_t, common::VUINT64> block_ds_map_; // fake meta info on ns
#endif
      uint64_t ns_addr_;
      std::string ns_addr_str_;
      int32_t cluster_id_;

#ifdef WITH_TAIR_CACHE
    private:
      static TairCacheHelper* remote_cache_helper_;
    public:
      int init_remote_cache_helper();
      bool is_hit_remote_cache(uint32_t block_id);
      bool check_init();
      void insert_remote_block_cache(const uint32_t block_id, const common::VUINT64& rds);
      int query_remote_block_cache(const uint32_t block_id, common::VUINT64& rds);
      int query_remote_block_cache(const SEG_DATA_LIST& seg_list, int& remote_hit_count);
      void remove_remote_block_cache(const uint32_t block_id);
#endif

    private:
      const int64_t block_cache_time_;
      const int64_t block_cache_items_;
      BLOCK_CACHE_MAP block_cache_map_;
    };
  }
}
#endif
