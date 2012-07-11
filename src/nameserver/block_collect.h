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

#ifndef TFS_NAMESERVER_BLOCK_COLLECT_H_
#define TFS_NAMESERVER_BLOCK_COLLECT_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include "ns_define.h"
#include "common/internal.h"
#include "common/parameter.h"
#include "common/array_helper.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class ServerCollect;
    class LayoutManager;
    class BlockCollect : public GCObject
    {
      #ifdef TFS_GTEST
      friend class BlockCollectTest;
      FRIEND_TEST(BlockCollectTest, add);
      FRIEND_TEST(BlockCollectTest, remove);
      FRIEND_TEST(BlockCollectTest, exist);
      FRIEND_TEST(BlockCollectTest, check_version);
      FRIEND_TEST(BlockCollectTest, check_replicate);
      FRIEND_TEST(BlockCollectTest, check_compact);
      #endif
      public:
      explicit BlockCollect(const uint32_t block_id);
      BlockCollect(const uint32_t block_id, const time_t now);
      virtual ~BlockCollect();

      bool add(bool& writable, bool& master, ServerCollect*& invalid_server, const ServerCollect* server);
      bool remove(const ServerCollect* server, const time_t now, const int8_t flag);
      bool exist(const ServerCollect* const server, const bool pointer /*= true*/) const;
      bool is_master(const ServerCollect* const server) const;
      bool is_writable() const;
      bool is_creating() const;
      bool in_replicate_queue() const;
      bool check_version(LayoutManager& manager, common::ArrayHelper<ServerCollect*>& removes,
          bool& expire_self, common::ArrayHelper<ServerCollect*>& other_expires, ServerCollect*& invalid_server,
          const ServerCollect* server,const int8_t role, const bool isnew, const common::BlockInfo& block_info,
          const time_t now);
      common::PlanPriority check_replicate(const time_t now) const;
      bool check_compact() const;
      bool check_balance() const;
      int check_redundant() const;
      inline int32_t size() const { return info_.size_;}
      void get_servers(common::ArrayHelper<ServerCollect*>& servers) const;
      void get_servers(std::vector<uint64_t>& servers) const;
      void get_servers(std::vector<ServerCollect*>& servers) const;
      inline void update(const common::BlockInfo& info) { info_ = info;}
      inline bool is_full() const { return info_.size_ >= common::SYSPARAM_NAMESERVER.max_block_size_; }
      inline uint32_t id() const { return info_.block_id_;}
      inline int32_t version() const { return info_.version_;}
      inline void set_create_flag(const int8_t flag = BLOCK_CREATE_FLAG_NO) { create_flag_ = flag;}
      inline void set_in_replicate_queue(const int8_t flag = BLOCK_IN_REPLICATE_QUEUE_YES) {in_replicate_queue_ = flag;}
      int8_t get_servers_size() const;
      int scan(common::SSMScanParameter& param) const;
      void dump(int32_t level, const char* file = __FILE__,
          const int32_t line = __LINE__, const char* function = __FUNCTION__) const;

      void callback(LayoutManager& manager);
      bool clear(LayoutManager& manager, const time_t now);
      static const int8_t BLOCK_CREATE_FLAG_NO;
      static const int8_t BLOCK_CREATE_FLAG_YES;
      static const int8_t VERSION_AGREED_MASK;
      private:
      ServerCollect** get_(const ServerCollect* const server, const bool pointer = true) const;
      DISALLOW_COPY_AND_ASSIGN(BlockCollect);

      private:
      common::BlockInfo info_; //7 * 4 = 28
      ServerCollect** servers_;
      int8_t create_flag_:4;
      int8_t in_replicate_queue_:4;
      int8_t reserve[7];
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* BLOCKCOLLECT_H_ */
