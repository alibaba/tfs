/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: logic_block.h 643 2011-08-02 07:38:33Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_LOGICALBLOCK_H_
#define TFS_DATASERVER_LOGICALBLOCK_H_

#include <string>
#include <list>
#include "dataserver_define.h"
#include "data_file.h"
#include "physical_block.h"
#include "index_handle.h"
#include "data_handle.h"
#include "block_status.h"
#include "common/lock.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace dataserver
  {

    class LogicBlock: public GCObject
    {
      public:
        LogicBlock(const uint32_t logic_block_id, const uint32_t main_blk_key, const std::string& base_path, const time_t now = time(NULL));
        LogicBlock(const uint32_t logic_block_id, const time_t now = time(NULL));

        ~LogicBlock();

        int set_block_id(const uint32_t logic_block_id)
        {
          if (INVALID_BlOCKID == logic_block_id_)
          {
            return common::EXIT_BLOCK_SETED_ERROR;
          }
          logic_block_id_ = logic_block_id;
          return common::TFS_SUCCESS;
        }

        int load_block_file(const int32_t bucket_size, const common::MMapOption mmap_option);
        int init_block_file(const int32_t bucket_size, const common::MMapOption mmap_option, const BlockType block_type);
        int delete_block_file();
        int rename_index_file();

        void add_physic_block(PhysicalBlock* physic_block);

        int open_write_file(uint64_t& inner_file_id);
        int check_block_version(int32_t& remote_version, common::UpdateBlockType &repair);
        int close_write_file(const uint64_t inner_file_id, DataFile* datafile, const uint32_t crc);

        int read_file(const uint64_t inner_file_id, char* buf, int32_t& nbytes, const int32_t offset, const int8_t flag);
        int read_file_info(const uint64_t inner_file_id, common::FileInfo& finfo);

        int rename_file(const uint64_t old_inner_file_id, const uint64_t new_inner_file_id);
        int unlink_file(const uint64_t inner_file_id, const int32_t action, int64_t& file_size);
        int read_raw_data(char* buf, int32_t& nbyte, const int32_t offset);
        int write_raw_data(const char* buf, const int32_t nbytes, const int32_t offset);

        void reset_seq_id(uint64_t file_id);
        int reset_block_version();
        int get_meta_infos(common::RawMetaVec& raw_metas);
        int get_sorted_meta_infos(common::RawMetaVec& meta_infos);
        int get_file_infos(std::vector<common::FileInfo>& fileinfos);
        int get_block_info(common::BlockInfo* blk_info);
        int copy_block_info(const common::BlockInfo* blk_info);

        //gc callback
        void callback();
        void clear();

        common::BlockInfo* get_block_info() const
        {
          return index_handle_->block_info();
        }

        int32_t get_data_file_size() const
        {
          return index_handle_->data_file_size();
        }

        std::list<PhysicalBlock*>* get_physic_block_list()
        {
          return &physical_block_list_;
        }

        int batch_write_meta(const common::BlockInfo* blk_info, const common::RawMetaVec* meta_list);
        int set_block_dirty_type(DirtyFlag dirty_flag);

        uint32_t get_logic_block_id() const
        {
          return logic_block_id_;
        }

        int32_t get_visit_count() const
        {
          return visit_count_;
        }

        void add_visit_count()
        {
          ++visit_count_;
        }

        void set_last_update(time_t time)
        {
          last_update_ = time;
        }

        time_t get_last_update() const
        {
          return last_update_;
        }

        void rlock()
        {
          rw_lock_.rdlock();
        }
        void wlock()
        {
          rw_lock_.wrlock();
        }
        void unlock()
        {
          rw_lock_.unlock();
        }

        int add_crc_error()
        {
          return block_health_status.add_crc_error();
        }

        int get_crc_error() const
        {
          return block_health_status.get_crc_error();
        }

        int add_eio_error()
        {
          return block_health_status.add_eio_error();
        }

        int get_eio_error() const
        {
          return block_health_status.get_eio_error();
        }

        int add_oper_warn()
        {
          return block_health_status.add_oper_warn();
        }

        int get_oper_warn() const
        {
          return block_health_status.get_oper_warn();
        }

        void reset_health()
        {
          block_health_status.reset();
        }

        void set_last_abnorm(time_t time)
        {
          last_abnorm_time_ = time;
        }
        time_t get_last_abnorm() const
        {
          return last_abnorm_time_;
        }

      private:
        int extend_block(const int32_t size, const int32_t offset);

      private:
        DISALLOW_COPY_AND_ASSIGN(LogicBlock);

        static const uint32_t INVALID_BlOCKID = (uint32_t)(-1);
        static const int32_t MAX_EXTEND_TIMES = 30;

      protected:
        uint32_t logic_block_id_; // logic block id

        int32_t avail_data_size_; // the data space of this logic block
        int32_t visit_count_;     // accumlating visit count
        time_t last_update_;      // last update time
        time_t last_abnorm_time_; // last abnormal time
        BlockStatus block_health_status; // block status info

        DataHandle* data_handle_;   // data operation handle
        IndexHandle* index_handle_; // associate index handle
        std::list<PhysicalBlock*> physical_block_list_; // the physical block list of this logic block
        common::RWLock rw_lock_;   // read-write lock

        friend class FileIterator;
    };

    // simulated iterator of logic block file
    class FileIterator
    {
      public:
        FileIterator(LogicBlock* logic_block);
        ~FileIterator();
        bool has_next() const;
        int next();
        const common::FileInfo* current_file_info() const;
        bool is_big_file() const;
        int read_buffer(char* buf, int32_t& nbytes);

      private:
        int fill_buffer();

      private:
        char* buf_;             // inner buffer
        int32_t data_len_, data_offset_; // data length, offset
        int32_t read_offset_, buf_len_;  // read offset, buffer length
        common::FileInfo cur_fileinfo_;  // current file meta info
        LogicBlock* logic_block_;        // associate logic block
        bool is_big_file_;               // big file or not
        common::RawMetaVec meta_infos_;  // meta info vector
        common::RawMetaVecIter meta_it_; // meta info vector iterator
    };

  }
}

#endif //TFS_DATASERVER_LOGICALBLOCK_H_
