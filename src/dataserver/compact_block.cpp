/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compact_block.cpp 706 2011-08-12 08:24:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <Memory.hpp>
#include "compact_block.h"
#include "message/compact_block_message.h"
#include "common/new_client.h"
#include "common/client_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;
    using namespace std;

    CompactBlock::CompactBlock()
    {
      init();
    }

    CompactBlock::CompactBlock(const uint64_t ns_ip, const uint64_t dataserver_id)
    {
      init();
      ns_ip_ = ns_ip;
      dataserver_id_ = dataserver_id;
    }

    CompactBlock::~CompactBlock()
    {
    }

    void CompactBlock::init()
    {
      stop_ = 0;
      expire_compact_interval_ = SYSPARAM_DATASERVER.expire_compact_time_;
      last_expire_compact_block_time_ = 0;
      dataserver_id_ = 0;
    }

    void CompactBlock::stop()
    {
      stop_ = 1;
      clear_compact_block_map();
      //Monitor<Mutex>::Lock lock(compact_block_monitor_);
      compact_block_monitor_.lock();
      compact_block_monitor_.notifyAll();
      compact_block_monitor_.unlock();
    }

    //execute compacting tasks
    int CompactBlock::run_compact_block()
    {
      while (!stop_)
      {
        compact_block_monitor_.lock();
        while (!stop_ && compact_block_queue_.empty())
        {
          compact_block_monitor_.wait();
        }
        if (stop_)
        {
          compact_block_monitor_.unlock();
          break;
        }

        // get the first task
        CompactBlkInfo* cpt_blk = compact_block_queue_.front();
        compact_block_queue_.pop_front();
        compact_block_monitor_.unlock();

        int64_t start_time = Func::curr_time();

        TBSYS_LOG(INFO, "start compact block. blockid: %d", cpt_blk->block_id_);
        // compact the first block
        int ret = real_compact(cpt_blk->block_id_);
        // send complete message
        req_block_compact_complete(cpt_blk->block_id_, ret, cpt_blk->seqno_);
        // failed, clear compact files
        if (TFS_SUCCESS != ret)
        {
          int del_ret = BlockFileManager::get_instance()->del_block(cpt_blk->block_id_, C_COMPACT_BLOCK);
          if (TFS_SUCCESS != del_ret)
          {
            TBSYS_LOG(ERROR, "compact blockid: %u, del old block error ret: %d\n", cpt_blk->block_id_, del_ret);
          }
        }

        int64_t end_time = Func::curr_time();
        TBSYS_LOG(INFO, "finish compact. blockid: %u, cost time: %" PRI64_PREFIX "u, ret: %d", cpt_blk->block_id_,
            (end_time - start_time), ret);

        tbsys::gDelete(cpt_blk);
      }

      compact_block_monitor_.lock();
      // when stoped, clear the left tasks
      while (!compact_block_queue_.empty())
      {
        CompactBlkInfo *b = compact_block_queue_.front();
        compact_block_queue_.pop_front();
        tbsys::gDelete(b);
      }
      compact_block_monitor_.unlock();
      return EXIT_SUCCESS;
    }

    //	add one logic block compact
    int CompactBlock::add_cpt_task(CompactBlkInfo* cpt_blk)
    {
      CompactBlkInfo t_cpt_blk = *cpt_blk;
      int32_t cpt_exist = 0;
      compact_block_monitor_.lock();
      // check whether requested block existed
      for (uint32_t i = 0; i < compact_block_queue_.size(); ++i)
      {
        if (compact_block_queue_[i]->block_id_ == cpt_blk->block_id_)
        {
          cpt_exist = 1;
          break;
        }
      }

      // add if nonexist
      if (0 == cpt_exist)
      {
        compact_block_queue_.push_back(cpt_blk);
      }
      else
      {
        tbsys::gDelete(cpt_blk);
      }
      compact_block_monitor_.unlock();
      compact_block_monitor_.lock();
      compact_block_monitor_.notify();
      compact_block_monitor_.unlock();

      TBSYS_LOG(INFO, "add compact block. cpt_exist: %d, blockid: %u, owner: %d, preserve_time: %d\n",
        cpt_exist, t_cpt_blk.block_id_, t_cpt_blk.owner_, t_cpt_blk.preserve_time_);

      return cpt_exist;
    }

    int CompactBlock::real_compact(const uint32_t block_id)
    {
      TBSYS_LOG(DEBUG, "compact start blockid: %u\n", block_id);
      int ret = TFS_SUCCESS;
      LogicBlock* src_logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == src_logic_block)
      {
        TBSYS_LOG(ERROR, "block is not exist. blockid: %u\n", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      // create dest block info
      BlockInfo* src_blk = src_logic_block->get_block_info();
      assert(src_blk->block_id_ == block_id);

      // create the dest block
      uint32_t physical_block_id = 0;
      ret = BlockFileManager::get_instance()->new_block(block_id, physical_block_id, C_COMPACT_BLOCK);
      if (TFS_SUCCESS != ret)
        return ret;
      TBSYS_LOG(DEBUG, "compact new block blockid: %u, physical blockid: %d\n", block_id, physical_block_id);

      // get dest block
      LogicBlock* dest_logic_block = BlockFileManager::get_instance()->get_logic_block(block_id, C_COMPACT_BLOCK);
      if (NULL == dest_logic_block)
      {
        TBSYS_LOG(ERROR, "get compact dest block fail. blockid: %u\n", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      ret = real_compact(src_logic_block, dest_logic_block);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "inner real compact blockid: %u fail. ret: %d\n", block_id, ret);
        return ret;
      }
      TBSYS_LOG(DEBUG, "compact blockid : %u, switch compact blk\n", block_id);

      // switch the compact block with serve block
      ret = BlockFileManager::get_instance()->switch_compact_blk(block_id);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact blockid: %u, switch compact blk fail. ret: %d\n", block_id, ret);
        return ret;
      }

      TBSYS_LOG(DEBUG, "compact del old blockid: %u\n", block_id);
      // del serve block
      ret = BlockFileManager::get_instance()->del_block(block_id, C_COMPACT_BLOCK);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact blockid: %u after switch, del old block fail. ret: %d\n", block_id, ret);
      }

      return TFS_SUCCESS;
    }

    // send complete message to ns
    int CompactBlock::req_block_compact_complete(const uint32_t block_id, const int32_t success, const int64_t seqno)
    {
      LogicBlock* LogicBlock = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == LogicBlock)
      {
        TBSYS_LOG(ERROR, "get block failed. blockid: %u\n", block_id);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      CompactBlockCompleteMessage req_cbc_msg;
      req_cbc_msg.set_block_id(block_id);
      CompactStatus compact_status = COMPACT_STATUS_SUCCESS;
      if (TFS_SUCCESS != success)
      {
        compact_status = COMPACT_STATUS_FAILED;
      }
      req_cbc_msg.set_seqno(seqno);
      req_cbc_msg.set_success(compact_status);
      req_cbc_msg.set_server_id(dataserver_id_);
      BlockInfo* blk = LogicBlock->get_block_info();
      req_cbc_msg.set_block_info(*blk);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp_msg = NULL;
      send_msg_to_server(ns_ip_, client, &req_cbc_msg, rsp_msg);
      NewClientManager::get_instance().destroy_client(client);

      return TFS_SUCCESS;
    }

    //	delete compact block files
    int CompactBlock::clear_compact_block_map()
    {
      int ret = TFS_SUCCESS;
      VUINT compact_blocks;
      BlockFileManager::get_instance()->get_logic_block_ids(compact_blocks, C_COMPACT_BLOCK);
      for (uint32_t i = 0; i < compact_blocks.size(); i++)
      {
        ret = BlockFileManager::get_instance()->del_block(compact_blocks[i], C_COMPACT_BLOCK);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "in check thread: del compact blockid: %u fail. ret: %d", compact_blocks[i], ret);
        }
      }
      return TFS_SUCCESS;
    }

    //	delete expired compact block files
    int CompactBlock::expire_compact_block_map()
    {
      int32_t current_time = time(NULL);
      int32_t now_time = current_time - expire_compact_interval_;
      // delete expired compact blocks
      if (last_expire_compact_block_time_ < now_time)
      {
        set < uint32_t > erase_blocks;
        BlockFileManager::get_instance()->expire_compact_blk((uint32_t) now_time, erase_blocks);

        for (set<uint32_t>::iterator mit = erase_blocks.begin(); mit != erase_blocks.end(); ++mit)
        {
          BlockFileManager::get_instance()->del_block(*mit, C_COMPACT_BLOCK);
        }
        last_expire_compact_block_time_ = current_time;
      }
      return TFS_SUCCESS;
    }

    int CompactBlock::real_compact(LogicBlock* src, LogicBlock* dest)
    {
      assert(NULL != src && NULL != dest);

      BlockInfo dest_blk;
      BlockInfo* src_blk = src->get_block_info();
      dest_blk.block_id_ = src_blk->block_id_;
      dest_blk.seq_no_ = src_blk->seq_no_;
      dest_blk.version_ = src_blk->version_;
      dest_blk.file_count_ = 0;
      dest_blk.size_ = 0;
      dest_blk.del_file_count_ = 0;
      dest_blk.del_size_ = 0;

      dest->set_last_update(time(NULL));
      TBSYS_LOG(DEBUG, "compact block set last update. blockid: %u\n", dest->get_logic_block_id());

      char* dest_buf = new char[MAX_COMPACT_READ_SIZE];
      int32_t write_offset = 0, data_len = 0;
      int32_t w_file_offset = 0;
      RawMetaVec dest_metas;
      FileIterator* fit = new FileIterator(src);

      int ret = TFS_SUCCESS;
      while (fit->has_next())
      {
        ret = fit->next();
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }

        const FileInfo* pfinfo = fit->current_file_info();
        if (pfinfo->flag_ & (FI_DELETED | FI_INVALID))
        {
          continue;
        }

        FileInfo dfinfo = *pfinfo;
        dfinfo.offset_ = w_file_offset;
        // the size returned by FileIterator.current_file_info->size is
        // the size of file content!!!
        dfinfo.size_ = pfinfo->size_ + sizeof(FileInfo);
        dfinfo.usize_ = pfinfo->size_ + sizeof(FileInfo);
        w_file_offset += dfinfo.size_;

        dest_blk.file_count_++;
        dest_blk.size_ += dfinfo.size_;

        RawMeta tmp_meta;
        tmp_meta.set_file_id(dfinfo.id_);
        tmp_meta.set_size(dfinfo.size_);
        tmp_meta.set_offset(dfinfo.offset_);
        dest_metas.push_back(tmp_meta);

        // need flush write buffer
        if ((0 != data_len) && (fit->is_big_file() || data_len + dfinfo.size_ > MAX_COMPACT_READ_SIZE))
        {
          TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %d\n", dest->get_logic_block_id(),
              write_offset);
          ret = dest->write_raw_data(dest_buf, data_len, write_offset);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "write raw data fail, blockid: %u, offset %d, readinglen: %d, ret :%d",
                dest->get_logic_block_id(), write_offset, data_len, ret);
            tbsys::gDeleteA(dest_buf);
            tbsys::gDelete(fit);
            return ret;
          }
          write_offset += data_len;
          data_len = 0;
        }

        if (fit->is_big_file())
        {
          ret = write_big_file(src, dest, *pfinfo, dfinfo, write_offset);
          write_offset += dfinfo.size_;
        }
        else
        {
          memcpy(dest_buf + data_len, &dfinfo, sizeof(FileInfo));
          int left_len = MAX_COMPACT_READ_SIZE - data_len;
          ret = fit->read_buffer(dest_buf + data_len + sizeof(FileInfo), left_len);
          data_len += dfinfo.size_;
        }
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }

      } // end of iterate

      if (0 != data_len) // flush the last buffer
      {
        TBSYS_LOG(DEBUG, "write one, blockid: %u, write offset: %d\n", dest->get_logic_block_id(), write_offset);
        ret = dest->write_raw_data(dest_buf, data_len, write_offset);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write raw data fail, blockid: %u, offset %d, readinglen: %d, ret :%d",
              dest->get_logic_block_id(), write_offset, data_len, ret);
          tbsys::gDeleteA(dest_buf);
          tbsys::gDelete(fit);
          return ret;
        }
      }

      tbsys::gDeleteA(dest_buf);
      tbsys::gDelete(fit);
      TBSYS_LOG(DEBUG, "compact write complete. blockid: %u\n", dest->get_logic_block_id());

      ret = dest->batch_write_meta(&dest_blk, &dest_metas);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact write segment meta failed. blockid: %u, meta size %zd\n", dest->get_logic_block_id(),
            dest_metas.size());
        return ret;
      }

      TBSYS_LOG(DEBUG, "compact set dirty flag. blockid: %u\n", dest->get_logic_block_id());
      ret = dest->set_block_dirty_type(C_DATA_CLEAN);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "compact blockid: %u set dirty flag fail. ret: %d\n", dest->get_logic_block_id(), ret);
        return ret;
      }

      return TFS_SUCCESS;
    }

    int CompactBlock::write_big_file(LogicBlock* src, LogicBlock* dest, const FileInfo& src_info,
        const FileInfo& dest_info, int32_t woffset)
    {
      int32_t rsize = src_info.size_;
      int32_t roffset = src_info.offset_ + sizeof(FileInfo);
      char* buf = new char[MAX_COMPACT_READ_SIZE];
      int32_t read_len = 0;
      int ret = TFS_SUCCESS;

      memcpy(buf, &dest_info, sizeof(FileInfo));
      int32_t data_len = sizeof(FileInfo);
      while (read_len < rsize)
      {
        int32_t cur_read = MAX_COMPACT_READ_SIZE - data_len;
        if (cur_read > rsize - read_len)
          cur_read = rsize - read_len;
        ret = src->read_raw_data(buf + data_len, cur_read, roffset);
        if (TFS_SUCCESS != ret)
          break;
        data_len += cur_read;
        read_len += cur_read;
        roffset += cur_read;

        ret = dest->write_raw_data(buf, data_len, woffset);
        if (TFS_SUCCESS != ret)
          break;
        woffset += data_len;

        data_len = 0;
      }

      tbsys::gDeleteA(buf);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write big file error, blockid: %u, ret: %d", dest->get_logic_block_id(), ret);
      }

      return ret;
    }
  }
}
