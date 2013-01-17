/* * (C) 2007-2010 Alibaba Group Holding Limited.
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
#include "server_collect.h"
#include "block_collect.h"
#include "global_factory.h"
#include <tbsys.h>
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t ServerCollect::MULTIPLE = 2;
    const int8_t ServerCollect::AVERAGE_USED_CAPACITY_MULTIPLE = 2;
    static void calculate_server_init_block_parameter(int32_t& block_nums, int32_t& min_expand_size,
        float& expand_ratio, const int64_t capacity)
    {
      static const int32_t init_block_nums[] = {2048, 4096, 8192, 16384, 16384};
      static const int32_t init_min_expand_size[] = {256, 512,1024,1024,1024};
      static const float   init_expand_ratio[] = {0.1, 0.1, 0.1,0.1,0.1};
      static const int64_t GB = 1 * 1024 * 1024 * 1024;
      int32_t index = 0;
      int64_t capacity_gb = capacity / GB;
      if (capacity_gb <= 300)//300GB
        index = 0;
      else if (capacity_gb > 300 && capacity_gb <= 600)//600GB
        index = 1;
      else if (capacity_gb > 600 && capacity_gb <= 1024)//1024GB
        index = 2;
      else if (capacity_gb > 1024 && capacity_gb <= 2048)//2048GB
        index = 3;
      else if (capacity_gb > 2048) //> 2048GB
        index = 4;
      block_nums = init_block_nums[index];
      min_expand_size = init_min_expand_size[index];
      expand_ratio = init_expand_ratio[index];
    }

    ServerCollect::ServerCollect(const uint64_t id):
      GCObject(0),
      id_(id),
      hold_(NULL),
      writable_(NULL),
      hold_master_(NULL)
    {
      //for query
    }

    ServerCollect::ServerCollect(const DataServerStatInfo& info, const time_t now):
      GCObject((now + common::SYSPARAM_NAMESERVER.heart_interval_ * MULTIPLE)),
      id_(info.id_),
      write_byte_(0),
      read_byte_(0),
      write_count_(0),
      read_count_(0),
      unlink_count_(0),
      use_capacity_(info.use_capacity_),
      total_capacity_(info.total_capacity_),
      startup_time_(now),
      rb_expired_time_(0xFFFFFFFF),
      next_report_block_time_(0xFFFFFFFF),
      in_dead_queue_timeout_(0xFFFFFFFF),
      current_load_(info.current_load_ <= 0 ? 1 : info.current_load_),
      block_count_(info.block_count_),
      write_index_(0),
      writable_index_(0),
      status_(common::DATASERVER_STATUS_ALIVE),
      rb_status_(REPORT_BLOCK_STATUS_NONE)
    {
        int32_t block_nums = 0;
        int32_t min_expand_size = 0;
        float   expand_ratio = 0.0;
        calculate_server_init_block_parameter(block_nums, min_expand_size, expand_ratio, total_capacity_);
        hold_     = new (std::nothrow)TfsSortedVector<BlockCollect*, BlockIdCompare>(block_nums, min_expand_size, expand_ratio);
        writable_ = new (std::nothrow)TfsSortedVector<BlockCollect*, BlockIdCompare>(block_nums / 4, min_expand_size / 4, expand_ratio);
        hold_master_ = new (std::nothrow)TfsVector<BlockCollect*>(MAX_WRITE_FILE_COUNT, 16, 0.1);
        assert(NULL != hold_);
        assert(NULL != writable_);
        assert(NULL != hold_master_);
    }

    ServerCollect::~ServerCollect()
    {
      tbsys::gDelete(hold_);
      tbsys::gDelete(writable_);
      tbsys::gDelete(hold_master_);
    }

    bool ServerCollect::add(const BlockCollect* block, const bool master, const bool writable)
    {
      int return_value = TFS_ERROR;
      bool ret = block != NULL;
      if (ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        BlockCollect* result = NULL;
        return_value = hold_->insert_unique(result, const_cast<BlockCollect*>(block));
        assert(NULL != result);
        //TBSYS_LOG(DEBUG, "id: %u, ret: %d master: %d, writable: %d, %d", block->id(), ret, master, writable, is_equal_group(block->id()));
        if (((TFS_SUCCESS == return_value)
             ||(EXIT_ELEMENT_EXIST == return_value))
            && (writable)
            && (master)
            && (is_equal_group(block->id())))
        {
          return_value = writable_->insert_unique(result, const_cast<BlockCollect*>(block));
          assert(NULL != result);
        }
      }
      return ((TFS_SUCCESS == return_value) || (EXIT_ELEMENT_EXIST == return_value));
    }

    bool ServerCollect::remove(BlockCollect* block)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      return  NULL != block ? remove_(block) : false;
    }

    bool ServerCollect::remove_(BlockCollect* block)
    {
      bool ret = block != NULL;
      if (ret)
      {
        hold_->erase(block);
        writable_->erase(block);
        hold_master_->erase(block);
      }
      return ret;
    }

    bool ServerCollect::remove(const common::ArrayHelper<BlockCollect*>& blocks)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      for (int32_t i = 0; i < blocks.get_array_index(); ++i)
      {
        remove_(*blocks.at(i));
      }
      return true;
    }

    bool ServerCollect::add_writable(const BlockCollect* block)
    {
      bool ret = block != NULL;
      if (ret)
      {
        ret = !block->is_full();
        if ((ret)
            && (is_equal_group(block->id())))
        {
          RWLock::Lock lock(mutex_, WRITE_LOCKER);
          BlockCollect* result = NULL;
          int return_value = writable_->insert_unique(result, const_cast<BlockCollect*>(block));
          ret = ((TFS_SUCCESS == return_value) || (EXIT_ELEMENT_EXIST == return_value));
          assert(result);
        }
      }
      return ret;
    }

    bool ServerCollect::clear_()
    {
      hold_->clear();
      writable_->clear();
      hold_master_->clear();
      return true;
    }

    bool ServerCollect::clear(LayoutManager& manager, const time_t now)
    {
      mutex_.wrlock();
      int32_t size = hold_->size();
      BlockCollect** blocks = NULL;
      if (size > 0)//这里size大部份情况下都为0,所以拷贝的代价是能接受的
      {
        int32_t index = 0;
        blocks = new (std::nothrow)BlockCollect*[size];
        assert(NULL != blocks);
        for (BLOCKS_ITER iter = hold_->begin(); iter != hold_->end(); iter++)
        {
          blocks[index++] = (*iter);
        }
      }
      clear_();
      mutex_.unlock();
      if (size > 0)
      {
        BlockCollect* pblock = NULL;
        ArrayHelper<BlockCollect*> helper(size, blocks, size);
        for (int32_t i = 0; i < helper.get_array_index(); ++i)
        {
          //这里有点问题，如果server刚下线，在上次server结构过期之前又上来了，此时解除了关系是不正确的
          //这里先在这里简单搞下在block解除关系的时候，如果检测ID和指针都一致时才解除关系
          pblock = *helper.at(i);
          assert(NULL != pblock);
          manager.get_block_manager().relieve_relation(pblock, this, now, BLOCK_COMPARE_SERVER_BY_ID_POINTER);
        }
        tbsys::gDeleteA(blocks);
      }
      return true;
    }

    int ServerCollect::choose_writable_block(BlockCollect*& result)
    {
      mutex_.rdlock();
      int32_t count = hold_master_->size();
      mutex_.unlock();
      result = NULL;
      int32_t ret = TFS_SUCCESS;
      BlockCollect* blocks[MAX_WRITE_FILE_COUNT];
      ArrayHelper<BlockCollect*> helper(MAX_WRITE_FILE_COUNT, blocks);
      for (int32_t index = 0; index < count && NULL == result; ++index)
      {
        ret = choose_writable_block_(result);
        if (TFS_SUCCESS == ret)
          ret = (result->is_writable() && is_equal_group(result->id())) ? TFS_SUCCESS : EXIT_BLOCK_NO_WRITABLE;
        if (TFS_SUCCESS != ret && NULL != result)
        {
          helper.push_back(result);
          result = NULL;
        }
      }
      remove_writable_(helper);
      return NULL == result ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
    }

    int ServerCollect::choose_writable_block_(BlockCollect*& result) const
    {
      result = NULL;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      int32_t ret = !hold_master_->empty() ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = write_index_;
        if (write_index_ >= hold_master_->size())
          write_index_ = 0;
        if (index >= hold_master_->size())
          index = 0;
        ++write_index_;
        result = hold_master_->at(index);
        assert(NULL != result);
      }
      return NULL != result ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
    }

    int ServerCollect::choose_writable_block_force(BlockCollect*& result) const
    {
      result = NULL;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      int32_t ret = !hold_master_->empty() ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = rand() % hold_master_->size();
        result = hold_master_->at(index);
        assert(NULL != result);
      }
      return NULL != result ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
    }

    int ServerCollect::scan(SSMScanParameter& param, const int8_t scan_flag) const
    {
      if (scan_flag & SSM_CHILD_SERVER_TYPE_INFO)
      {
        param.data_.writeInt64(id_);
        param.data_.writeInt64(use_capacity_);
        param.data_.writeInt64(total_capacity_);
        param.data_.writeInt32(current_load_);
        param.data_.writeInt32(block_count_);
        param.data_.writeInt64(last_update_time_);
        param.data_.writeInt64(startup_time_);
        param.data_.writeInt64(write_byte_);
        param.data_.writeInt64(write_count_);
        param.data_.writeInt64(read_byte_);
        param.data_.writeInt64(read_count_);
        param.data_.writeInt64(time(NULL));
        param.data_.writeInt32(status_);
      }

      RWLock::Lock lock(mutex_, READ_LOCKER);
      if (scan_flag & SSM_CHILD_SERVER_TYPE_HOLD)
      {
        param.data_.writeInt32(hold_->size());
        for (BLOCKS_ITER iter = hold_->begin(); iter != hold_->end(); iter++)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
      {
        param.data_.writeInt32(writable_->size());
        for (BLOCKS_ITER iter = writable_->begin(); iter != writable_->end(); iter++)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_MASTER)
      {
        param.data_.writeInt32(hold_master_->size());
        for (BLOCKS_ITER iter = hold_master_->begin(); iter != hold_master_->end(); iter++)
        {
          param.data_.writeInt32((*iter)->id());
        }
      }
      return TFS_SUCCESS;
    }

    bool ServerCollect::touch(bool& promote, int32_t& count, const double average_used_capacity)
    {
      bool ret = ((is_report_block_complete() || promote) /*&& !is_full()*/ && count > 0);
      if (!ret)
      {
        count = 0;
      }
      else
      {
        BlockCollect* invalid_blocks[1024];
        ArrayHelper<BlockCollect*> helper(1024, invalid_blocks);
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        int32_t current = hold_master_->size();
        TBSYS_LOG(DEBUG, "%s touch current %d, writable size: %d", CNetUtil::addrToString(id()).c_str(),current, writable_->size());
        if (current >= SYSPARAM_NAMESERVER.max_write_file_count_)
        {
          count = 0;
        }
        else
        {
          count = std::min(count, (SYSPARAM_NAMESERVER.max_write_file_count_ - current));
          if (count <= 0)
          {
            TBSYS_LOG(DEBUG, "%s", "there is any block need add into hold_master_");
          }
          else
          {
            int32_t actual = 0;
            double use_capacity_ratio = use_capacity_ / total_capacity_;
            double max_average_use_capacity_ratio = average_used_capacity * AVERAGE_USED_CAPACITY_MULTIPLE;
            BlockCollect* block = NULL;
            TfsVector<BlockCollect*>::iterator where;
            while ((!writable_->empty())
                  && (writable_index_ < writable_->size())
                  && (actual < count))
            {
              block = writable_->at(writable_index_);
              ++writable_index_;
              if (!block->is_writable())
              {
                if (block->is_full())
                  helper.push_back(block);
                continue;
              }
              if (!is_equal_group(block->id()))
              {
                helper.push_back(block);
                continue;
              }
              if (!block->is_master(this))
                continue;

              where = hold_master_->find(block);
              if (hold_master_->end() == where)
              {
                ++actual;
                hold_master_->push_back(block);
              }
            }

            if (writable_index_ >= writable_->size())
              writable_index_ = 0;

            for (int32_t i = 0; i < helper.get_array_index(); ++i)
            {
              block = *helper.at(i);
              writable_->erase(block);
            }

            if (!is_full() && ((use_capacity_ratio < max_average_use_capacity_ratio)
                              || max_average_use_capacity_ratio <= PERCENTAGE_MIN))
            {
              count -= actual;
            }
            else
            {
              count = 0;
            }
          }
        }
      }
      return ret;
    }

    bool ServerCollect::get_range_blocks_(common::ArrayHelper<BlockCollect*>& blocks, const uint32_t begin, const int32_t count) const
    {
      blocks.clear();
      int32_t actual = 0;
      BlockCollect query(begin);
      BLOCKS_ITER iter = 0 == begin ? hold_->begin() : hold_->upper_bound(&query);
      while(iter != hold_->end() && actual < count)
      {
        blocks.push_back((*iter));
        actual++;
        iter++;
      }
      return actual < count;
    }

    bool ServerCollect::get_range_blocks(common::ArrayHelper<BlockCollect*>& blocks, const uint32_t begin, const int32_t count) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return get_range_blocks_(blocks, begin, count);
    }

    void ServerCollect::statistics(NsGlobalStatisticsInfo& stat, const bool is_new) const
    {
      if (is_new)
      {
        stat.use_capacity_ += use_capacity_;
        stat.total_capacity_ += total_capacity_;
        stat.total_load_ += current_load_;
        stat.total_block_count_ += block_count_;
        stat.alive_server_count_ += 1;
      }
      stat.max_load_ = std::max(current_load_, stat.max_load_);
      stat.max_block_count_ = std::max(block_count_, stat.max_block_count_);
    }

    void ServerCollect::update(const common::DataServerStatInfo& info, const time_t now, const bool is_new)
    {
      //id_ = info.id_;
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      current_load_ = info.current_load_;
      block_count_ = info.block_count_;
      update_last_time(!is_new ? now : (now + common::SYSPARAM_NAMESERVER.heart_interval_ * MULTIPLE));
      startup_time_ = is_new ? now : info.startup_time_;
      status_ = info.status_;
      read_count_ = info.total_tp_.read_file_count_;
      read_byte_ = info.total_tp_.read_byte_;
      write_count_ = info.total_tp_.write_file_count_;
      write_byte_ = info.total_tp_.write_byte_;
    }

    void ServerCollect::reset(LayoutManager& manager, const common::DataServerStatInfo& info, const time_t now)
    {
      clear(manager, now);
      update(info, now, true);
      rb_expired_time_ = 0xFFFFFFFF;
      next_report_block_time_ = 0xFFFFFFFF;
      in_dead_queue_timeout_  = 0xFFFFFFFF;
      write_index_ = 0;
      writable_index_ = 0;
      status_ = common::DATASERVER_STATUS_ALIVE;
      rb_status_ = REPORT_BLOCK_STATUS_NONE;
    }

    void ServerCollect::callback(LayoutManager& manager)
    {
      clear(manager, Func::get_monotonic_time());
    }

    void ServerCollect::set_next_report_block_time(const time_t now, const int64_t time_seed, const bool ns_switch)
    {
      int32_t hour = common::SYSPARAM_NAMESERVER.report_block_time_upper_ - common::SYSPARAM_NAMESERVER.report_block_time_lower_ ;
      time_t current = time(NULL);
      time_t next    = current;
      if (ns_switch)
      {
        next += (time_seed % (hour * 3600));
      }
      else
      {
        if (common::SYSPARAM_NAMESERVER.report_block_time_interval_ > 0)
        {
          next += (common::SYSPARAM_NAMESERVER.report_block_time_interval_ * 24 * 3600);
          struct tm lt;
          localtime_r(&next, &lt);
          if (hour > 0)
            lt.tm_hour = time_seed % hour + common::SYSPARAM_NAMESERVER.report_block_time_lower_;
          else
            lt.tm_hour = common::SYSPARAM_NAMESERVER.report_block_time_lower_;
          lt.tm_min  = time_seed % 60;
          lt.tm_sec =  time_seed % 60;
          next = mktime(&lt);
        }
        else
        {
          next += (common::SYSPARAM_NAMESERVER.report_block_time_interval_min_ * 60);
        }
      }
      time_t diff_sec = next - current;
      next_report_block_time_ = now + diff_sec;
      TBSYS_LOG(DEBUG, "%s next: %"PRI64_PREFIX"d, diff: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d, hour: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), next_report_block_time_, diff_sec, now, hour);
    }

    bool ServerCollect::remove_writable_(const common::ArrayHelper<BlockCollect*>& blocks)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      BlockCollect* block = NULL;
      for (int32_t i = 0; i < blocks.get_array_index(); ++i)
      {
        block = *blocks.at(i);
        TBSYS_LOG(DEBUG, "%s remove_writable: is_full: %d, servers size: %d",
          tbsys::CNetUtil::addrToString(id()).c_str(), block->is_full(), block->get_servers_size());
        if (!block->is_writable() || !is_equal_group(block->id()))
           hold_master_->erase(block);
        if (block->is_full() || !is_equal_group(block->id()))
           writable_->erase(block);
      }
      return true;
    }

    int ServerCollect::choose_move_block_random(BlockCollect*& result) const
    {
      result = NULL;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      int32_t ret = !hold_->empty() ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = random() % hold_->size();
        result = hold_->at(index);
      }
      return NULL == result ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
    }

    int ServerCollect::expand_ratio(const float expand_ratio)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      if (hold_->need_expand(expand_ratio))
        hold_->expand_ratio(expand_ratio);
      if (writable_->need_expand(expand_ratio))
        writable_->expand_ratio(expand_ratio);
      TBSYS_LOG(DEBUG, "%s expand, hold: %d, writable: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), hold_->size(), writable_->size());
      return TFS_SUCCESS;
    }

    #ifdef TFS_GTEST
    bool ServerCollect::exist_writable(const BlockCollect* block)
    {
      bool ret = NULL == block;
      if (!ret)
      {
        BLOCKS_ITER iter = writable_->find(const_cast<BlockCollect*>(block));
        ret = iter != writable_->end();
      }
      return ret;
    }
    #endif
  } /** nameserver **/
}/** tfs **/
