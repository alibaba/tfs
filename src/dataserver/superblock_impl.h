/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: superblock_impl.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_SUPERBLOCK_IMPL_H_
#define TFS_DATASERVER_SUPERBLOCK_IMPL_H_

#include <string>
#include "common/file_op.h"
#include "bit_map.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace dataserver
  {
    class SuperBlockImpl
    {
      public:
        SuperBlockImpl(const std::string& filename, const int32_t super_start_offset, const bool flag = true);
        ~SuperBlockImpl();

        int read_super_blk(common::SuperBlock& super_block) const;
        int write_super_blk(const common::SuperBlock& super_block);
        bool check_status(const char* block_tag, const common::SuperBlock& super_block) const;
        int read_bit_map(char* buf, const int32_t size) const;
        int write_bit_map(const BitMap* normal_bitmap, const BitMap* error_bitmap);
        int flush_file();

        int dump_super_block(const common::SuperBlock&)
        {
          return common::TFS_SUCCESS;
        }
        int backup_super_block(const common::SuperBlock&)
        {
          return common::TFS_SUCCESS;
        }

      private:
        SuperBlockImpl();
        DISALLOW_COPY_AND_ASSIGN(SuperBlockImpl);

        std::string super_block_file_; // associate super block file
        int32_t super_reserve_offset_; // super block reserved offset
        int32_t bitmap_start_offset_;  // super block bitmap offset
        common::FileOperation* file_op_;
    };
  }
}

#endif //TFS_DATASERVER_SUPERBLOCK_IMPL_H_
