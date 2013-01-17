/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: physical_block.cpp 33 2010-11-01 05:24:35Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "physical_block.h"
#include "common/error_msg.h"
#include "blockfile_manager.h"
#include <tbsys.h>
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;

    MMapFileOperation* PhysicalBlock::prefix_op_ = NULL;

    // physical block initialization, inner format:
    // --------------------------------------------------------------------------------------------
    // |       block meta info prefix       |           file data(current stored data)            |
    // --------------------------------------------------------------------------------------------
    // | BlockPrefix(BLOCK_RESERVER_LENGTH) | FileInfo+data | FileInfo+data | ... | FileInfo+data |
    // --------------------------------------------------------------------------------------------
    PhysicalBlock::PhysicalBlock(const uint32_t physical_block_id, const std::string& mount_path,
        const int32_t block_length, const BlockType block_type) :
      physical_block_id_(physical_block_id)
    {
      int32_t head_len = sizeof(BlockPrefix);
      // assure not out of BLOCK_RESERVER_LENGTH range
      assert(head_len <= BLOCK_RESERVER_LENGTH);

      data_start_ = BLOCK_RESERVER_LENGTH;
      total_data_len_ = block_length - data_start_; // data area
      assert(total_data_len_ > 0);

      std::stringstream tmp_stream;
      // get main or extend block file path
      if (C_MAIN_BLOCK == block_type)
      {
        tmp_stream << mount_path << MAINBLOCK_DIR_PREFIX << physical_block_id;
      }
      else
      {
        tmp_stream << mount_path << EXTENDBLOCK_DIR_PREFIX << physical_block_id;
      }
      std::string physical_block_path;
      tmp_stream >> physical_block_path;

      file_op_ = new FileOperation(physical_block_path);
      TBSYS_LOG(
          DEBUG,
          "new physicalblock. physical blockid: %u, file name: %s, block length: %d, total data len: %d, data start offset: %d",
          physical_block_id_, physical_block_path.c_str(), block_length, total_data_len_, data_start_);
    }

    PhysicalBlock::~PhysicalBlock()
    {
      tbsys::gDelete(file_op_);
    }

    int PhysicalBlock::init_prefix_op(std::string& mount_path)
    {
      // map block_prefix file
      int ret = TFS_SUCCESS;
      SuperBlock super_block;
      ret = BlockFileManager::get_instance()->query_super_block(super_block);
      if (TFS_SUCCESS == ret)
      {
        struct stat st;
        std::string block_prefix_file = mount_path + BLOCK_HEADER_PREFIX;
        if ((0 == access(block_prefix_file.c_str(), F_OK))
          && (0 == stat(block_prefix_file.c_str(), &st)))
        {
          prefix_op_ = new MMapFileOperation(block_prefix_file.c_str());
          MMapOption mmap_option;
          mmap_option.first_mmap_size_ = st.st_size;
          mmap_option.max_mmap_size_ = st.st_size;
          mmap_option.per_mmap_size_= st.st_size;
          ret = prefix_op_->mmap_file(mmap_option);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "mmap prefix file fail. ret: %d", ret);
          }
        }

        // if (FS_SPEEDUP_VERSION == super_block.version_), don't compare for
        // upgrade
        /*std::string block_prefix_file = mount_path + BLOCK_HEADER_PREFIX;
        if (0 != access(block_prefix_file.c_str(), F_OK))  // not exist
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "fs version is 2, but block_prefix file not exist.");
        }
        else
        {
          struct stat st;
          if (0 != stat(block_prefix_file.c_str(), &st))
          {
            ret = TFS_ERROR;
            TBSYS_LOG(ERROR, "stat prefix file fail. ret: %d", errno);
          }
          else
          {
            prefix_op_ = new MMapFileOperation(block_prefix_file.c_str());
            MMapOption mmap_option;
            mmap_option.first_mmap_size_ = st.st_size;
            mmap_option.max_mmap_size_ = st.st_size;
            mmap_option.per_mmap_size_= st.st_size;
            ret = prefix_op_->mmap_file(mmap_option);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "mmap prefix file fail. ret: %d", ret);
            }
          }
        }*/
      }
      return ret;
    }

    int PhysicalBlock::pread_data(char* buf, const int32_t nbytes, const int32_t offset)
    {
      if (offset + nbytes > total_data_len_)
      {
        return EXIT_PHYSIC_BLOCK_OFFSET_ERROR;
      }
      // converse reletive data offset to absolute offset in block
      return file_op_->pread_file(buf, nbytes, data_start_ + offset);
    }

    int PhysicalBlock::pwrite_data(const char* buf, const int32_t nbytes, const int32_t offset)
    {
      if (offset + nbytes > total_data_len_)
      {
        return EXIT_PHYSIC_BLOCK_OFFSET_ERROR;
      }
      int64_t time_start = tbsys::CTimeUtil::getTime();
      int ret = file_op_->pwrite_file(buf, nbytes, data_start_ + offset);
      int64_t time_end = tbsys::CTimeUtil::getTime();
      if (time_end - time_start >= 1000000)
      {
        TBSYS_LOG(
            WARN,
            "physical blockid: %u, offset: %d, data len: %d, timestart: %" PRI64_PREFIX "u ,time end: %" PRI64_PREFIX "u, time cost: %" PRI64_PREFIX "u",
            physical_block_id_, offset, nbytes, time_start, time_end, time_end - time_start);
      }
      return ret;
    }

    int PhysicalBlock::load_block_prefix()
    {
      memset(&block_prefix_, 0, sizeof(BlockPrefix));
      int ret = 0;
      if (NULL == prefix_op_)
      {
        ret = file_op_->pread_file((char*) (&block_prefix_), sizeof(BlockPrefix), 0);
      }
      else
      {
        TBSYS_LOG(INFO, "load block prefix by new interface %d", physical_block_id_);
        ret = prefix_op_->pread_file((char*) (&block_prefix_), sizeof(BlockPrefix),
            (physical_block_id_ - 1) * sizeof(BlockPrefix));
      }

      TBSYS_LOG(
          DEBUG,
          "load block prefix. physical blockid: %u, logic blockid: %u, prev physical blockid: %u, next physical blockid: %u, ret: %d",
          physical_block_id_, block_prefix_.logic_blockid_, block_prefix_.prev_physic_blockid_,
          block_prefix_.next_physic_blockid_, ret);
      return ret;
    }

    int PhysicalBlock::clear_block_prefix()
    {
      memset(&block_prefix_, 0, sizeof(BlockPrefix));
      return TFS_SUCCESS;
    }

    int PhysicalBlock::dump_block_prefix()
    {
      int ret = TFS_SUCCESS;
      if (NULL == prefix_op_)
      {
        ret = file_op_->pwrite_file((const char*) (&block_prefix_), sizeof(BlockPrefix), 0);
        if (TFS_SUCCESS == ret)
        {
          file_op_->flush_file();  // if fail, it will be flushed in background
        }
      }
      else
      {
        TBSYS_LOG(INFO, "dump block prefix by new interface %d", physical_block_id_);
        ret = prefix_op_->pwrite_file((const char*) (&block_prefix_), sizeof(BlockPrefix),
                (physical_block_id_ - 1) * sizeof(BlockPrefix));
        if (TFS_SUCCESS == ret)
        {
          prefix_op_->flush_file();  // if fail, it will be flushed in background
        }
      }

      TBSYS_LOG(DEBUG, "dump block prefix. logic blockid: %u, prev physical blockid: %u, next physical blockid: %u, ret: %d",
          block_prefix_.logic_blockid_, block_prefix_.prev_physic_blockid_, block_prefix_.next_physic_blockid_, ret);

      return ret;
    }

    void PhysicalBlock::get_block_prefix(BlockPrefix& block_prefix)
    {
      block_prefix = block_prefix_;
      return;
    }

    void PhysicalBlock::set_block_prefix(const uint32_t block_id, const uint32_t prev_physic_blockid,
        const uint32_t next_physical_blockid)
    {
      block_prefix_.logic_blockid_ = block_id;
      block_prefix_.prev_physic_blockid_ = prev_physic_blockid;
      block_prefix_.next_physic_blockid_ = next_physical_blockid;
      return;
    }

  }
}
