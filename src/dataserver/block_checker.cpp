/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_checker.cpp 356 2011-05-26 07:51:09Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <Memory.hpp>
#include "block_checker.h"
#include "blockfile_manager.h"
#include "common/func.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace tbutil;
    using namespace std;

    BlockChecker::~BlockChecker()
    {
      check_mutex_.lock();
      while (!check_file_queue_.empty())
      {
        CrcCheckFile* ccf = check_file_queue_.front();
        check_file_queue_.pop_front();
        tbsys::gDelete(ccf);
      }
      check_mutex_.unlock();
    }

    int BlockChecker::init(const uint64_t dataserver_id, Requester* ds_requester)
    {
      dataserver_id_ = dataserver_id;
      stop_ = 0;
      ds_requester_ = ds_requester;
      return TFS_SUCCESS;
    }

    void BlockChecker::stop()
    {
      stop_ = 1;
    }

    int BlockChecker::add_repair_task(CrcCheckFile* repair_task)
    {
      int check_exist = 0;
      tbutil::Mutex::Lock lock(check_mutex_);
      if (check_file_queue_.size() >= static_cast<uint32_t>(MAX_CHECK_BLOCK_SIZE))
      {
        // too much
        tbsys::gDelete(repair_task);
        check_mutex_.unlock();
        return EXIT_BLOCK_CHECKER_OVERLOAD;
      }

      // find if exist
      for (uint32_t i = 0; i < check_file_queue_.size(); ++i)
      {
        if ((check_file_queue_[i]->block_id_ == repair_task->block_id_ && check_file_queue_[i]->file_id_
              == repair_task->file_id_) || (check_file_queue_[i]->block_id_ == repair_task->block_id_
                && check_file_queue_[i]->flag_ == CHECK_BLOCK_EIO))
        {
          check_exist = 1;
          break;
        }
      }

      if (0 == check_exist)
      {
        check_file_queue_.push_back(repair_task);
      }
      else
      {
        tbsys::gDelete(repair_task);
      }

      return check_exist;
    }

    //consume one task every time
    int BlockChecker::consume_repair_task()
    {
      CrcCheckFile* ccf = NULL;
      check_mutex_.lock();
      if (!check_file_queue_.empty())
      {
        ccf = check_file_queue_.front();
        check_file_queue_.pop_front();
      }
      check_mutex_.unlock();

      if (NULL == ccf)
        return TFS_SUCCESS;

      TBSYS_LOG(INFO, "start repair blockid: %u, fileid: %" PRI64_PREFIX "u", ccf->block_id_, ccf->file_id_);
      int64_t start_time = Func::curr_time();
      int ret = TFS_SUCCESS;
      if (CHECK_BLOCK_EIO == ccf->flag_)
      {
        ret = do_repair_eio(ccf->block_id_);
      }
      else
      {
        ret = do_repair_crc(*ccf);
      }
      int64_t end_time = Func::curr_time();

      TBSYS_LOG(INFO,
          "end repair blockid: %u, fileid: %" PRI64_PREFIX "u, flag: %d, cost time: %" PRI64_PREFIX "u, ret: %d",
          ccf->block_id_, ccf->file_id_, ccf->flag_, (end_time - start_time), ret);

      tbsys::gDelete(ccf);
      return ret;
    }

    //reaction async
    int BlockChecker::do_repair_crc(const CrcCheckFile& check_file)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(check_file.block_id_);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block is not exist. blockid: %u.\n", check_file.block_id_);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      bool init_status = file_repair_.init(dataserver_id_);
      if (!init_status)
      {
        TBSYS_LOG(ERROR, "file repair init fail.\n");
        return TFS_ERROR;
      }

      check_mutex_.lock();
      //add crc
      logic_block->add_crc_error();
      int ret = check_block(logic_block);
      //0:no expire; !0 expire
      if (1 == ret)
      {
        set_error_bitmap(logic_block);
        check_mutex_.unlock();
        return ds_requester_->req_update_block_info(check_file.block_id_, UPDATE_BLOCK_REPAIR);
      }

      char tmp_file[MAX_PATH_LENGTH];
      ret = file_repair_.fetch_file(check_file, tmp_file);
      if (TFS_SUCCESS != ret)
      {
        check_mutex_.unlock();
        unlink(tmp_file);
        TBSYS_LOG(ERROR, "fetch file fail. ret: %d, unlink tmp file: %s\n", ret, tmp_file);
        return ret;
      }

      ret = file_repair_.repair_file(check_file, tmp_file);
      unlink(tmp_file);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "repair file fail. ret: %d\n", ret);
        set_error_bitmap(logic_block);
        check_mutex_.unlock();
        return ds_requester_->req_update_block_info(check_file.block_id_, UPDATE_BLOCK_REPAIR);
      }
      logic_block->set_last_abnorm(time(NULL));
      check_mutex_.unlock();

      return TFS_SUCCESS;
    }

    int BlockChecker::do_repair_eio(const uint32_t blockid)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(blockid);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "block is not exist. blockid: %u.\n", blockid);
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      check_mutex_.lock();
      logic_block->add_eio_error();
      int ret = check_block(logic_block);
      //0:no expire ; !0 expire
      if (1 == ret)
      {
        set_error_bitmap(logic_block);
        check_mutex_.unlock();
        return ds_requester_->req_update_block_info(blockid, UPDATE_BLOCK_REPAIR);
      }
      logic_block->set_last_abnorm(time(NULL));
      check_mutex_.unlock();
      return TFS_SUCCESS;
    }

    int BlockChecker::expire_error_block()
    {
      list<LogicBlock*> logic_blocks;
      int ret = BlockFileManager::get_instance()->get_all_logic_block(logic_blocks);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "list logic block fail ret: %d", ret);
      }

      TBSYS_LOG(INFO, "BlockChecker get logic block size: %zd", logic_blocks.size());
      int32_t expire_block_nums = 0;
      time_t mark = time(NULL) - SYSPARAM_DATASERVER.expire_check_block_time_;
      int64_t start_time = Func::curr_time();
      for (list<LogicBlock*>::iterator lit = logic_blocks.begin(); lit != logic_blocks.end(); ++lit)
      {
        if (stop_)
          break;

        //expire
        if ((*lit)->get_last_abnorm() < mark)
        {
          // no need to lock. if abnorm, it will continue from the abnorm occur
          (*lit)->reset_health();
          continue;
        }

        //0:no expire ; !0 expire
        if (1 == check_block(*lit))
        {
          ++expire_block_nums;
          set_error_bitmap(*lit);
          ds_requester_->req_update_block_info((*lit)->get_logic_block_id(), UPDATE_BLOCK_REPAIR);
        }

      }
      int64_t end_time = Func::curr_time();
      TBSYS_LOG(INFO, "end expire error block expire blocks: %d, cost time: %" PRI64_PREFIX "d", expire_block_nums,
          (end_time - start_time));
      return TFS_SUCCESS;
    }

    int BlockChecker::check_block(const LogicBlock* logic_block)
    {
      if (NULL == logic_block)
      {
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      if (logic_block->get_crc_error() >=  SYSPARAM_DATASERVER.max_crc_error_nums_ || logic_block->get_eio_error()
          >= (int) SYSPARAM_DATASERVER.max_eio_error_nums_ || (((logic_block->get_crc_error() + logic_block->get_eio_error()
                * (SYSPARAM_DATASERVER.max_crc_error_nums_ / SYSPARAM_DATASERVER.max_eio_error_nums_))
              >= SYSPARAM_DATASERVER.max_crc_error_nums_)))
      {
        TBSYS_LOG(ERROR, "block may be error, will be removed. blockid: %u, crc error nums: %d, eio error nums: %d\n",
            logic_block->get_logic_block_id(), logic_block->get_crc_error(), logic_block->get_eio_error());
        return 1;
      }
      return 0;
    }

    int BlockChecker::set_error_bitmap(LogicBlock* logic_block)
    {
      if (NULL == logic_block)
      {
        return EXIT_NO_LOGICBLOCK_ERROR;
      }

      list<PhysicalBlock*>* physic_block_list = NULL;
      //no need to lock. we consider the new extend phy block's status is normal
      physic_block_list = logic_block->get_physic_block_list();
      set <uint32_t> phy_block_ids;
      if (physic_block_list)
      {
        phy_block_ids.clear();
        for (list<PhysicalBlock*>::iterator lit = physic_block_list->begin(); lit != physic_block_list->end(); ++lit)
        {
          phy_block_ids.insert((*lit)->get_physic_block_id());
        }
      }

      BlockFileManager::get_instance()->set_error_bitmap(phy_block_ids);
      return TFS_SUCCESS;
    }
  }
}
