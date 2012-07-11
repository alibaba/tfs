/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: superblock_impl.cpp 33 2010-11-01 05:24:35Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "superblock_impl.h"
#include "dataserver_define.h"
#include <tbsys.h>
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;

    // super block file implementation, inner format:
    // ------------------------------------------------------------------------------------------
    // |    not used     |     double superblock   |not used |  double normal  |  double error  |
    // |                 |                         |         |   bitmap        |  bitmap        |
    // ------------------------------------------------------------------------------------------
    // | reserver offset | SuperBlock | SuperBlock | int32_t | BitMap | BitMap | BitMap | BitMap|
    // ------------------------------------------------------------------------------------------
    SuperBlockImpl::SuperBlockImpl(const std::string& mount_name, const int32_t super_start_offset, const bool flag) :
      super_reserve_offset_(super_start_offset)
    {
      // dir
      if (true == flag)
      {
        super_block_file_ = mount_name + SUPERBLOCK_NAME;
      }
      else
      {
        super_block_file_ = mount_name;
      }
      // bitmap start offset
      bitmap_start_offset_ = super_reserve_offset_ + 2 * sizeof(SuperBlock) + sizeof(int32_t);
      file_op_ = new FileOperation(super_block_file_, O_RDWR | O_SYNC);
      // superblock file is already exist, should not happen
      if (NULL == file_op_)
      {
        assert(false);
      }
    }

    SuperBlockImpl::~SuperBlockImpl()
    {
      tbsys::gDelete(file_op_);
    }

    int SuperBlockImpl::read_super_blk(SuperBlock& super_block) const
    {
      memset(&super_block, 0, sizeof(SuperBlock));
      return file_op_->pread_file((char*) (&super_block), sizeof(SuperBlock), super_reserve_offset_);
    }

    int SuperBlockImpl::write_super_blk(const SuperBlock& super_block)
    {
      char* tmp_buf = new char[2 * sizeof(SuperBlock)];
      memcpy(tmp_buf, &super_block, sizeof(SuperBlock));
      memcpy(tmp_buf + sizeof(SuperBlock), &super_block, sizeof(SuperBlock));
      int ret = file_op_->pwrite_file(tmp_buf, 2 * sizeof(SuperBlock), super_reserve_offset_);
      tbsys::gDeleteA(tmp_buf);
      if (ret)
      {
        TBSYS_LOG(ERROR, "write super block error. ret: %d, desc: %s", ret, strerror(errno));
        return ret;
      }
      return TFS_SUCCESS;
    }

    bool SuperBlockImpl::check_status(const char* block_tag, const SuperBlock& super_block) const
    {
      // check magic tag
      if (memcmp(block_tag, super_block.mount_tag_, strlen(block_tag) + 1) != 0)
      {
        return false;
      }
      return true;
    }

    int SuperBlockImpl::read_bit_map(char* buf, const int32_t size) const
    {
      return file_op_->pread_file(buf, size, bitmap_start_offset_);
    }

    int SuperBlockImpl::write_bit_map(const BitMap* normal_bitmap, const BitMap* error_bitmap)
    {
      assert(normal_bitmap->get_slot_count() == error_bitmap->get_slot_count());
      uint32_t bit_map_size = normal_bitmap->get_slot_count();
      char* tmp_bit_map_buf = new char[4 * bit_map_size];
      memcpy(tmp_bit_map_buf, normal_bitmap->get_data(), bit_map_size);
      memcpy(tmp_bit_map_buf + bit_map_size, normal_bitmap->get_data(), bit_map_size);
      memcpy(tmp_bit_map_buf + 2 * bit_map_size, error_bitmap->get_data(), bit_map_size);
      memcpy(tmp_bit_map_buf + 3 * bit_map_size, error_bitmap->get_data(), bit_map_size);

      int ret = file_op_->pwrite_file(tmp_bit_map_buf, 4 * bit_map_size, bitmap_start_offset_);
      tbsys::gDeleteA(tmp_bit_map_buf);
      if (ret)
      {
        TBSYS_LOG(ERROR, "write bitmap error. ret: %d, desc: %s", ret, strerror(errno));
        return ret;
      }
      return TFS_SUCCESS;
    }

    int SuperBlockImpl::flush_file()
    {
      return file_op_->flush_file();
    }

  }
}
