/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file_op.cpp 326 2011-05-24 07:18:18Z daoan@taobao.com $
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
#include "mmap_file_op.h"
#include "common/error_msg.h"
#include "common/func.h"
#include <tbsys.h>

namespace tfs
{
  namespace dataserver
  {

    using namespace common;
    int MMapFileOperation::mmap_file(const MMapOption& mmap_option)
    {
      if (mmap_option.max_mmap_size_ < mmap_option.first_mmap_size_)
      {
        return TFS_ERROR;
      }

      if (0 == mmap_option.max_mmap_size_)
      {
        return TFS_ERROR;
      }

      int fd = check_file();
      if (fd < 0)
        return fd;

      if (!is_mapped_)
      {
        tbsys::gDelete(map_file_);
        map_file_ = new MMapFile(mmap_option, fd);
        is_mapped_ = map_file_->map_file(true);
      }

      if (is_mapped_)
      {
        return TFS_SUCCESS;
      }
      else
      {
        return TFS_ERROR;
      }
    }

    int MMapFileOperation::munmap_file()
    {
      if (is_mapped_ && NULL != map_file_)
      {
        tbsys::gDelete(map_file_);
        is_mapped_ = false;
      }
      return TFS_SUCCESS;
    }

    int MMapFileOperation::rename_file()
    {
      int ret = TFS_ERROR;
      if (is_mapped_ && NULL != map_file_)
      {
        std::string new_file_name = file_name_;
        new_file_name += "." + Func::time_to_str(time(NULL), 1);
        ret = ::rename(file_name_, new_file_name.c_str());
        free(file_name_);
        file_name_ = strdup(new_file_name.c_str());
      }
      return ret;
    }

    void* MMapFileOperation::get_map_data() const
    {
      if (is_mapped_)
        return map_file_->get_data();

      return NULL;
    }

    int MMapFileOperation::pread_file(char* buf, const int32_t size, const int64_t offset)
    {
      if (is_mapped_ && (offset + size) > map_file_->get_size())
      {
        TBSYS_LOG(DEBUG, "mmap file pread, size: %d, offset: %" PRI64_PREFIX "d, map file size: %d. need remap",
            size, offset, map_file_->get_size());
        map_file_->remap_file();
      }

      if (is_mapped_ && (offset + size) < map_file_->get_size())
      {
        memcpy(buf, (char *) map_file_->get_data() + offset, size);
        return TFS_SUCCESS;
      }

      return FileOperation::pread_file(buf, size, offset);
    }

    int MMapFileOperation::pread_file(ParaInfo& para_info, const int32_t size, const int64_t offset)
    {
      if (is_mapped_ && (offset + size) > map_file_->get_size())
      {
        map_file_->remap_file();
      }

      if (is_mapped_ && (offset + size) <= map_file_->get_size())
      {
        para_info.set_self_buf((char*) map_file_->get_data() + offset);
        return TFS_SUCCESS;
      }

      return FileOperation::pread_file(para_info.get_new_buf(), size, offset);
    }

    int MMapFileOperation::pwrite_file(const char* buf, const int32_t size, const int64_t offset)
    {
      if (is_mapped_ && (offset + size) > map_file_->get_size())
      {
        TBSYS_LOG(DEBUG, "mmap file write, size: %d, offset: %" PRI64_PREFIX "d, map file size: %d, need remap",
            size, offset, map_file_->get_size());
        map_file_->remap_file();
      }

      if (is_mapped_ && (offset + size) <= map_file_->get_size())
      {
        memcpy((char *) map_file_->get_data() + offset, buf, size);
        return TFS_SUCCESS;
      }

      return FileOperation::pwrite_file(buf, size, offset);
    }

    int MMapFileOperation::flush_file()
    {
      if (is_mapped_)
      {
        if (map_file_->sync_file())
        {
          return TFS_SUCCESS;
        }
        else
        {
          return TFS_ERROR;
        }
      }
      return FileOperation::flush_file();
    }
  }
}
