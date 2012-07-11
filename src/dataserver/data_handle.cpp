/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_handle.cpp 33 2010-11-01 05:24:35Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "data_handle.h"
#include <list>
#include "logic_block.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;

    int DataHandle::read_segment_info(FileInfo* inner_file_info, const int32_t offset)
    {
      if (NULL == inner_file_info)
      {
        return EXIT_POINTER_NULL;
      }
      return read_segment_data(reinterpret_cast<char*>(inner_file_info), FILEINFO_SIZE, offset);
    }

    int DataHandle::write_segment_info(const FileInfo* inner_file_info, const int32_t offset)
    {
      if (NULL == inner_file_info)
      {
        return EXIT_POINTER_NULL;
      }
      return write_segment_data(reinterpret_cast<const char*>(inner_file_info), FILEINFO_SIZE, offset);
    }

    int DataHandle::write_segment_data(const char* buf, const int32_t nbytes, const int32_t offset)
    {
      if (NULL == buf)
      {
        return EXIT_POINTER_NULL;
      }
      PhysicalBlock* tmp_physical_block = NULL;
      int32_t inner_offset = 0;
      int32_t written_len = 0, writting_len = 0;
      int ret = TFS_SUCCESS;

      while (written_len < nbytes)
      {
        writting_len = nbytes - written_len;
        ret = choose_physic_block(&tmp_physical_block, offset + written_len, inner_offset, writting_len);
        if (TFS_SUCCESS != ret)
          return ret;

        ret = tmp_physical_block->pwrite_data(buf + written_len, writting_len, inner_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        written_len += writting_len;
      }
      return ret;
    }

    int DataHandle::read_segment_data(char* buf, const int32_t nbytes, const int32_t offset)
    {
      if (NULL == buf)
      {
        return EXIT_POINTER_NULL;
      }
      PhysicalBlock* tmp_physical_block = NULL;
      int32_t inner_offset = 0;
      int32_t has_read_len = 0, reading_len = 0;
      int ret = TFS_SUCCESS;

      while (has_read_len < nbytes)
      {
        reading_len = nbytes - has_read_len;
        ret = choose_physic_block(&tmp_physical_block, offset + has_read_len, inner_offset, reading_len);
        if (TFS_SUCCESS != ret)
          return ret;

        ret = tmp_physical_block->pread_data(buf + has_read_len, reading_len, inner_offset);
        if (TFS_SUCCESS != ret)
          return ret;

        has_read_len += reading_len;
      }

      return ret;
    }

    // choose physic block base on offset
    int DataHandle::choose_physic_block(PhysicalBlock** tmp_physical_block, const int32_t offset,
        int32_t& inner_offset, int32_t& inner_len)
    {
      std::list<PhysicalBlock*>* physic_block_list = logic_block_->get_physic_block_list();
      std::list<PhysicalBlock*>::iterator lit = physic_block_list->begin();
      int32_t sum_offset = 0;
      int32_t prev_sum_offset = 0;
      int32_t data_len = 0;

      for (; lit != physic_block_list->end(); ++lit)
      {
        data_len = (*lit)->get_total_data_len();
        sum_offset += data_len;

        // offset is in current physic block's range, bingo find.
        if (offset < sum_offset)
        {
          *tmp_physical_block = (*lit);
          // convert absolute offset to relative offset in current block
          inner_offset = offset - prev_sum_offset;

          if ((offset + inner_len) > sum_offset)
          {
            inner_len = sum_offset - offset;
          }

          break;
        }
        prev_sum_offset += data_len;
      }

      // oops,can not find
      if (lit == physic_block_list->end())
      {
        return EXIT_PHYSIC_BLOCK_OFFSET_ERROR;
      }

      return TFS_SUCCESS;
    }

    int DataHandle::fadvise_readahead(const int64_t offset, const int64_t size)
    {
      std::list<PhysicalBlock*>* physic_block_list = logic_block_->get_physic_block_list();
      // just consider main block now
      if (!physic_block_list->empty())
      {
        std::list<PhysicalBlock*>::iterator lit = physic_block_list->begin();
        return posix_fadvise((*lit)->get_block_fd(), offset, size, POSIX_FADV_WILLNEED);
      }
      return 0;
    }

  }
}
