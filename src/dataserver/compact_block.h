/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compact_block.h 390 2011-06-01 08:11:49Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_COMPACTBLOCK_H_
#define TFS_DATASERVER_COMPACTBLOCK_H_

#include <Mutex.h>
#include <Monitor.h>
#include "logic_block.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
//#include "common/config.h"

namespace tfs
{
  namespace dataserver
  {

    struct CompactBlkInfo
    {
      int64_t seqno_;
      uint32_t block_id_;
      int32_t preserve_time_;
      int32_t owner_;
    };

    class CompactBlock
    {
      public:
        CompactBlock();
        CompactBlock(const uint64_t ns_ip, const uint64_t dataserver_id);
        ~CompactBlock();

        // stop compact tasks
        void stop();

        // add logic block compact task
        int add_cpt_task(CompactBlkInfo* cpt_blk);
        int real_compact(const uint32_t block_id);
        int real_compact(LogicBlock* src, LogicBlock *dest);
        // delete expired compact block files
        int expire_compact_block_map();

        int run_compact_block();

      private:
        void init();

        int clear_compact_block_map();
        int write_big_file(LogicBlock* src, LogicBlock* dest, const common::FileInfo& src_info,
            const common::FileInfo& dest_info, int32_t woffset);
        int req_block_compact_complete(const uint32_t block_id, const int32_t success, const int64_t seqno);

      private:
        DISALLOW_COPY_AND_ASSIGN(CompactBlock);

        std::deque<CompactBlkInfo*> compact_block_queue_;
        tbutil::Monitor<tbutil::Mutex> compact_block_monitor_;
        uint64_t ns_ip_;

        int32_t stop_;
        int32_t expire_compact_interval_;
        int32_t last_expire_compact_block_time_;
        uint64_t dataserver_id_;
    };
  }
}
#endif //TFS_DATASERVER_COMPACTBLOCK_H_
