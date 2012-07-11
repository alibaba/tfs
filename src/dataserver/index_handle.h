/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: index_handle.h 764 2011-09-07 06:04:06Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_INDEXHANDLE_H_
#define TFS_DATASERVER_INDEXHANDLE_H_

#include "common/internal.h"
#include "mmap_file_op.h"
#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {
    struct IndexHeader
    {
      public:
        IndexHeader()
        {
          memset(this, 0, sizeof(IndexHeader));
        }

        common::BlockInfo block_info_; // meta block info
        DirtyFlag flag_;               // dirty status flag
        int32_t bucket_size_;   // hash bucket size
        int32_t data_file_offset_; // offset to write next data in block(total data size of block)
        int32_t index_file_size_; // offset after: index_header + all buckets
        int32_t free_head_offset_; // free node list, for reuse

      private:
        DISALLOW_COPY_AND_ASSIGN(IndexHeader);
    };

    class IndexHandle
    {
      public:
        IndexHandle(const std::string& base_path, const uint32_t main_block_id);

        // for test
        IndexHandle(MMapFileOperation* mmap_op)
        {
          file_op_ = mmap_op;
          is_load_ = false;
        }
        ~IndexHandle();

        // create blockfile ,write index header and buckets info into the file
        int create(const uint32_t logic_block_id, const int32_t cfg_bucket_size, const common::MMapOption map_option,
            const DirtyFlag dirty_flag);
        // load blockfile into memory, check block info
        int load(const uint32_t logic_block_id, const int32_t bucket_size, const common::MMapOption map_option);
        // clear memory map, delete blockfile
        int remove(const uint32_t logic_block_id);
        // rename index file ==> current idex file + "." + current time
        int rename(const uint32_t logic_block_id);
        // flush file to disk
        int flush();
        int set_block_dirty_type(const DirtyFlag dirty_flag);

        // find the next available key greater than key
        int find_avail_key(uint64_t& key);
        // update next available key
        void reset_avail_key(uint64_t key);
        // merge the version info from ns and local
        int check_block_version(int32_t& remote_version);
        int reset_block_version();

        // write meta into block, key must not be existed
        int write_segment_meta(const uint64_t key, const common::RawMeta& meta);
        // read meta info
        int read_segment_meta(const uint64_t key, common::RawMeta& meta);
        // write metainfo; if already existed update it, or insert into the block
        int override_segment_meta(const uint64_t key, const common::RawMeta& meta);
        int batch_override_segment_meta(const common::RawMetaVec& meta_list);
        // update meta info
        int update_segment_meta(const uint64_t key, const common::RawMeta& meta);
        // delete meta(key) from metainfo list, add it to free list
        int delete_segment_meta(const uint64_t key);
        // put all meta info into param meta
        int traverse_segment_meta(common::RawMetaVec& raw_metas);
        int traverse_sorted_segment_meta(common::RawMetaVec& meta);

        // update block info after operation
        int update_block_info(const OperType oper_type, const uint32_t modify_size);
        int copy_block_info(const common::BlockInfo* blk_info);

        int get_block_data_offset() const
        {
          int32_t offset = -1;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            offset = reinterpret_cast<IndexHeader*>(data)->data_file_offset_;
          }
          return offset;
        }

        void commit_block_data_offset(const int file_size)
        {
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            reinterpret_cast<IndexHeader*> (data)->data_file_offset_ += file_size;
          }
        }

        IndexHeader* index_header()
        {
          IndexHeader* header = NULL;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            header = reinterpret_cast<IndexHeader*>(data);
          }
          return header;
        }

        int32_t* bucket_slot()
        {
          int32_t* slot = NULL;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            slot = reinterpret_cast<int32_t*> (reinterpret_cast<char*> (data) + sizeof(IndexHeader));
          }
          return slot;
        }

        int32_t bucket_size() const
        {
          int32_t size = -1;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            size = reinterpret_cast<IndexHeader*> (data)->bucket_size_;
          }
          return size;
        }

        common::BlockInfo* block_info()
        {
          common::BlockInfo* info = NULL;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            info = reinterpret_cast<common::BlockInfo*> (data);
          }
          return info;
        }

        int32_t data_file_size() const
        {
          int32_t size= -1;
          void* data = file_op_->get_map_data();
          if (NULL != data)
          {
            size = reinterpret_cast<IndexHeader*>(data)->data_file_offset_;
          }
          return size;
        }

      private:
        bool hash_compare(const uint64_t left_key, const uint64_t right_key)
        {
          return (left_key == right_key);
        }

        int hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset);
        int hash_insert(const int32_t slot, const int32_t previous_offset, const common::RawMeta& meta);

      private:
        DISALLOW_COPY_AND_ASSIGN(IndexHandle);
        static const int32_t MAX_RETRY_TIMES = 3000;

      private:
        MMapFileOperation* file_op_;
        bool is_load_;
    };

    struct RawMetaSort
    {
        bool operator()(const common::RawMeta& left, const common::RawMeta& right) const
        {
          return (left.get_offset() < right.get_offset());
        }
    };

  }
}
#endif //TFS_DATASERVER_INDEXHANDLE_H_
