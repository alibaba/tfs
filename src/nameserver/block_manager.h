/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMESERVER_BLOCK_MANAGER_H_
#define TFS_NAMESERVER_BLOCK_MANAGER_H_

#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include "gc.h"
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"
#include "block_collect.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    struct BlockIdCompare
    {
      bool operator ()(const BlockCollect* lhs, const BlockCollect* rhs) const
      {
        return lhs->id() < rhs->id();
      }
    };
    class BlockManager
    {
      #ifdef TFS_GTEST
      friend class BlockManagerTest;
      friend class LayoutManagerTest;
      FRIEND_TEST(BlockManagerTest, insert_remove_get_exist);
      FRIEND_TEST(BlockManagerTest, get_servers);
      FRIEND_TEST(BlockManagerTest, scan);
      FRIEND_TEST(BlockManagerTest, update_relation);
      FRIEND_TEST(BlockManagerTest, build_relation);
      FRIEND_TEST(BlockManagerTest, update_block_info);
      FRIEND_TEST(BlockManagerTest, scan_ext);
      void clear_();
      #endif
      typedef std::map<uint32_t, int64_t> LAST_WRITE_BLOCK_MAP;
      typedef LAST_WRITE_BLOCK_MAP::iterator LAST_WRITE_BLOCK_MAP_ITER;
      typedef LAST_WRITE_BLOCK_MAP::const_iterator LAST_WRITE_BLOCK_MAP_CONST_ITER;
      typedef common::TfsSortedVector<BlockCollect*, BlockIdCompare> BLOCK_MAP;
      typedef common::TfsSortedVector<BlockCollect*, BlockIdCompare>::iterator BLOCK_MAP_ITER;
      public:
        explicit BlockManager(LayoutManager& manager);
        virtual ~BlockManager();
        BlockCollect* insert(const uint32_t block, const time_t now, const bool set = false);
        //bool remove(std::vector<GCObject*>& rms, const uint32_t block);
        bool remove(GCObject*& gc_object, const uint32_t block);

        bool push_to_delete_queue(const uint32_t block, const uint64_t server);
        bool pop_from_delete_queue(std::pair<uint32_t, uint64_t>& pairs);
        bool delete_queue_empty() const;
        void clear_delete_queue();

        //emergency replicate method only call by build thread,no lock
        bool push_to_emergency_replicate_queue(BlockCollect* block);
        BlockCollect* pop_from_emergency_replicate_queue();
        bool has_emergency_replicate_in_queue() const;
        int64_t get_emergency_replicate_queue_size() const;

        BlockCollect* get(const uint32_t block) const;
        bool exist(const uint32_t block) const;
        void dump(const int32_t level) const;
        void dump_write_block(const int32_t level) const;
        void clear_write_block();
        bool scan(common::ArrayHelper<BlockCollect*>& result, uint32_t& begin, const int32_t count) const;
        int scan(common::SSMScanParameter& param, int32_t& next, bool& all_over,
            bool& cutover, const int32_t should) const;
        int get_servers(std::vector<uint64_t>& servers, const uint32_t block) const;
        int get_servers(std::vector<ServerCollect*>& servers, const uint32_t block) const;
        int get_servers(common::ArrayHelper<ServerCollect*>& server, const uint32_t block) const;
        int get_servers(common::ArrayHelper<ServerCollect*>& server, const BlockCollect* block) const;
        int64_t get_servers_size(const BlockCollect* block) const;

        int update_relation(ServerCollect* server, const std::set<common::BlockInfo>& blocks, const time_t now);
        int build_relation(BlockCollect* block, bool& writable, bool& master,
            ServerCollect*& invalid_server, const ServerCollect* server, const time_t now, const bool set =false);
        bool relieve_relation(BlockCollect* block, const ServerCollect* server, const time_t now, const int8_t flag);
        int update_block_info(BlockCollect*& output, bool& isnew, bool& writable, bool& master,
            const common::BlockInfo& info, const ServerCollect* server, const time_t now, const bool addnew);

        bool need_replicate(const BlockCollect* block) const;
        bool need_replicate(const BlockCollect* block, const time_t now) const;
        bool need_replicate(common::ArrayHelper<ServerCollect*>& servers, common::PlanPriority& priority,
             const BlockCollect* block, const time_t now) const;
        bool need_compact(common::ArrayHelper<ServerCollect*>& servers, const BlockCollect* block, const time_t now) const;
        bool need_balance(common::ArrayHelper<ServerCollect*>& servers, const BlockCollect* block, const time_t now) const;

        int expand_ratio(int32_t& index, const float expand_ratio = 0.1);

        int update_block_last_wirte_time(uint32_t& id, const uint32_t block, const time_t now);
        bool has_write(const uint32_t block, const time_t now) const;
        void timeout(const time_t now);
      private:
        DISALLOW_COPY_AND_ASSIGN(BlockManager);
        common::RWLock& get_mutex_(const uint32_t block) const;

        BlockCollect* get_(const uint32_t block) const;

        BlockCollect* insert_(const uint32_t block, const time_t now, const bool set = false);
        BlockCollect* remove_(const uint32_t block);

        int build_relation_(BlockCollect* block, bool& writable, bool& master,
            ServerCollect*& invalid_server, const ServerCollect* server, const time_t now, const bool set = false);

        bool pop_from_delete_queue_(std::pair<uint32_t, uint64_t>& pairs);

        bool has_write_(const uint32_t block, const time_t now) const;

        int32_t get_chunk_(const uint32_t block) const;

        int get_servers_(common::ArrayHelper<ServerCollect*>& server, const BlockCollect* block) const;

      private:
        LayoutManager& manager_;
        int32_t last_wirte_block_nums_;
        mutable common::RWLock rwmutex_[MAX_BLOCK_CHUNK_NUMS];
        LAST_WRITE_BLOCK_MAP last_write_blocks_[MAX_BLOCK_CHUNK_NUMS];
        BLOCK_MAP * blocks_[MAX_BLOCK_CHUNK_NUMS];

        tbutil::Mutex delete_block_queue_muetx_;
        std::deque<std::pair<uint32_t, uint64_t> > delete_block_queue_;

        std::deque<uint32_t> emergency_replicate_queue_;
    };
  }/** nameserver **/
}/** tfs **/

#endif /* BLOCKCHUNK_H_ */
