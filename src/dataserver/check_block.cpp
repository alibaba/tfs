/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: check_block.cpp 476 2012-04-12 14:43:42Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include <Time.h>
#include <Memory.hpp>
#include "message/replicate_block_message.h"
#include "message/write_data_message.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "blockfile_manager.h"
#include "check_block.h"


namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;

    void CheckBlock::add_check_task(const uint32_t block_id)
    {
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.find(block_id);
      if (iter == changed_block_map_.end())
      {
        uint32_t mtime = time(NULL);
        changed_block_map_.insert(std::make_pair(block_id, mtime));
        TBSYS_LOG(DEBUG, "add check task. block id: %u, modify time: %u", block_id, mtime);
      }
      else
      {
        iter->second = time(NULL);
        TBSYS_LOG(DEBUG, "update check task. block id: %u, modify time: %u",
            iter->first, iter->second);
      }
      changed_block_mutex_.unlock();
    }

    void CheckBlock::remove_check_task(const uint32_t block_id)
    {
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.find(block_id);
      if (iter != changed_block_map_.end())
      {
        changed_block_map_.erase(iter);
      }
      changed_block_mutex_.unlock();
    }

    int CheckBlock::check_all_blocks(common::CheckBlockInfoVec& check_result,
        const int32_t check_flag, const uint32_t check_time, const uint32_t last_check_time)
    {
      TBSYS_LOG(DEBUG, "check all blocks: check_flag: %d", check_flag);
      TIMER_START();

      // get the list need to check, then free lock
      UNUSED(check_flag);
      VUINT should_check_blocks;
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.begin();
      for ( ; iter != changed_block_map_.end(); iter++)
      {
        // check if modify time between check range
        if (iter->second < check_time &&
            iter->second >= last_check_time)
        {
          should_check_blocks.push_back(iter->first);
        }
      }
      changed_block_mutex_.unlock();

      // do real check, call check_one_block
      int ret = 0;
      CheckBlockInfo result;
      VUINT::iterator it = should_check_blocks.begin();
      for ( ; it != should_check_blocks.end(); it++)
      {
         ret = check_one_block(*it, result);
         if (TFS_SUCCESS == ret)
         {
           check_result.push_back(result);
         }
      }
      TIMER_END();
      TBSYS_LOG(INFO, "check all blocks. count: %zd, cost: %"PRI64_PREFIX"d\n",
          should_check_blocks.size(), TIMER_DURATION());

      return TFS_SUCCESS;
    }

    int CheckBlock::check_one_block(const uint32_t block_id, common::CheckBlockInfo& result)
    {
      TBSYS_LOG(DEBUG, "check one, block_id: %u", block_id);
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)  // already deleted block, remove it
      {
        TBSYS_LOG(WARN, "blockid: %u is not exist.", block_id);
        remove_check_task(block_id);
        ret = EXIT_NO_LOGICBLOCK_ERROR;
      }
      else
      {
        logic_block->rlock();   // lock block
        common::BlockInfo bi;
        ret = logic_block->get_block_info(&bi);
        logic_block->unlock();

        if (TFS_SUCCESS == ret)
        {
          result.block_id_ = bi.block_id_;    // block id
          result.version_ = bi.version_;     // version
          result.file_count_ = bi.file_count_ - bi.del_file_count_; // file count
          result.total_size_ = bi.size_ - bi.del_size_;   // file size
        }

        TBSYS_LOG(DEBUG, "blockid: %u, file count: %u, total size: %u",
              result.block_id_, result.file_count_, result.total_size_);
      }
      return ret;
    }

    int CheckBlock::repair_block_info(const uint32_t block_id)
    {
      TBSYS_LOG(DEBUG, "repair block info %u", block_id);
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(WARN, "block %u not found\n", block_id);
      }
      else
      {
        BlockInfo bi;
        BlockInfo bi_old;
        BlockInfo bi_new;
        RawMetaVec raw_metas;
        RawMetaVecIter meta_it;

        // rlock block
        logic_block->rlock();
        ret = logic_block->get_block_info(&bi);
        if (TFS_SUCCESS == ret)
        {
          bi_old = bi;
          logic_block->get_meta_infos(raw_metas);
        }
        logic_block->unlock();

        // repair, lock free
        FileInfo fi;
        if (TFS_SUCCESS == ret)
        {
          bi.file_count_ =  0;
          bi.size_ = 0;
          bi.del_file_count_ = 0;
          bi.del_size_ = 0;
          meta_it = raw_metas.begin();
          for ( ; meta_it != raw_metas.end(); meta_it++)
          {
            int32_t offset = meta_it->get_offset();
            int32_t size = meta_it->get_size();
            int32_t finfo_size = sizeof(FileInfo);
            if (TFS_SUCCESS == logic_block->read_raw_data((char*)&fi, finfo_size, offset))
            {
              if (fi.flag_ & FI_DELETED)
              {
                bi.del_file_count_++;
                bi.del_size_ += size;
              }
              bi.file_count_++;
              bi.size_ += size;
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          // update, block may be deleted already
          logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
          if (NULL == logic_block)
          {
            TBSYS_LOG(WARN, "block %u not found\n", block_id);
          }
          else
          {
            // wlock block
            logic_block->wlock();
            ret = logic_block->get_block_info(&bi_new);
            if (TFS_SUCCESS == ret && bi_new == bi_old && !(bi_old == bi))
            {

              TBSYS_LOG(DEBUG, "will update block info");
              ret = logic_block->copy_block_info(&bi);
            }
            logic_block->unlock();
          }
        }

        // result
        if (TFS_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "repair block info %u succeed", block_id);
        }
        else
        {
          TBSYS_LOG(WARN, "repair block info %u fail, ret: %d", block_id, ret);
        }
      }
      return ret;
    }
  }
}
