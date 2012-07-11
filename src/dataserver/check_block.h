/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: check_block.h 390 2012-04-12 13:58:49Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_CHECKBLOCK_H_
#define TFS_DATASERVER_CHECKBLOCK_H_

#include "common/internal.h"
#include "common/new_client.h"
#include "dataserver_define.h"
#include "blockfile_manager.h"
#include "logic_block.h"
#include "visit_stat.h"
#include <Mutex.h>

namespace tfs
{
  namespace dataserver
  {
    /**
     * @brief class for implement cluster data check
     */
    class CheckBlock
    {
      public:

      /**
       * @brief construtor
       */
       CheckBlock()
       {

       }

       /**
        * @brief destructor
        */
       ~CheckBlock()
       {
       }

       /**
       * @brief add modified block
       *
       * @param block_id: the modified block id
       *
       */
       void add_check_task(const uint32_t block_id);

       /**
       * @brief remove block, the block is deleted, transfered or expired?
       *
       * @param block_id: block id to remove
       */
       void remove_check_task(const uint32_t block_id);

       /**
       * @brief check all stable blocks
       *
       * @param check_result: check result
       * @param check_flag: check flag
       * @param check_time: checkserver start time
       *
       * @return
       */
       int check_all_blocks(common::CheckBlockInfoVec& check_result,
          const int32_t check_flag, const uint32_t check_time, const uint32_t last_check_time);

       /**
       * @brief check one block
       *
       * @param block_id:
       *
       * @return: 0 on success
       */
       int check_one_block(const uint32_t block_id, common::CheckBlockInfo& result);

        /**
        * @brief repair block information
        *
        * @param block_id: block to repair
        *
        * @return
        */
        int repair_block_info(const uint32_t block_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(CheckBlock);

        ChangedBlockMap changed_block_map_;
        tbutil::Mutex changed_block_mutex_;

        uint32_t block_stable_time_;   // minutes
    };
  }
}
#endif  //TFS_DATASERVER_CHECKBLOCK_H_
