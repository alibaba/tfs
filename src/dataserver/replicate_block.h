/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: replicate_block.h 390 2011-06-01 08:11:49Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_REPLICATEBLOCK_H_
#define TFS_DATASERVER_REPLICATEBLOCK_H_

#include "common/new_client.h"
#include "dataserver_define.h"
#include "blockfile_manager.h"
#include "logic_block.h"
#include <Mutex.h>
#include <Monitor.h>

namespace tfs
{
  namespace dataserver
  {
    class ReplicateBlock
    {
      public:
        explicit ReplicateBlock(const uint64_t ns_ip);
        ~ReplicateBlock();

        void stop();
        int add_repl_task(ReplBlockExt& repl_blk);

        int add_cloned_block_map(const uint32_t block_id);
        int del_cloned_block_map(const uint32_t block_id);
        int expire_cloned_block_map();
        int run_replicate_block();

      private:
        void init();
        int replicate_block_to_server(const ReplBlockExt& b);
        int send_repl_block_complete_info(const int status, const ReplBlockExt& b);
        int clear_cloned_block_map();

      private:
        ReplicateBlock();
        DISALLOW_COPY_AND_ASSIGN(ReplicateBlock);

        int stop_;
        std::deque<ReplBlockExt> repl_block_queue_; // repl block queue
        ReplBlockMap replicating_block_map_; // replicating
        tbutil::Monitor<tbutil::Mutex> repl_block_monitor_;

        ClonedBlockMap cloned_block_map_;
        tbutil::Mutex cloned_block_mutex_;
        int32_t expire_cloned_interval_; // check interval
        int32_t last_expire_cloned_block_time_;

        uint64_t ns_ip_;
    };

  }
}
#endif  //TFS_DATASERVER_REPLICATEBLOCK_H_
