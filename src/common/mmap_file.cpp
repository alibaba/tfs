/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file.cpp 552 2011-06-24 08:44:50Z duanfei@taobao.com $
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

#include <tbsys.h>
#include "mmap_file.h"

namespace tfs
{
  namespace common
  {
    MMapFile::MMapFile() :
      size_(0), fd_(-1), data_(NULL)
    {
    }

    MMapFile::MMapFile(const int fd) :
      size_(0), fd_(fd), data_(NULL)
    {
    }

    MMapFile::MMapFile(const common::MMapOption& mmap_option, const int fd) :
      size_(0), fd_(fd), data_(NULL)
    {
      mmap_file_option_.max_mmap_size_ = mmap_option.max_mmap_size_;
      mmap_file_option_.per_mmap_size_ = mmap_option.per_mmap_size_;
      mmap_file_option_.first_mmap_size_ = mmap_option.first_mmap_size_;
    }

    MMapFile::~MMapFile()
    {
      if (data_)
      {
        TBSYS_LOG(INFO, "mmap file destruct, fd: %d maped size: %d, data: %p", fd_, size_, data_);
        msync(data_, size_, MS_SYNC); // make sure synced
        munmap(data_, size_);

        mmap_file_option_.max_mmap_size_ = 0;
        mmap_file_option_.per_mmap_size_ = 0;
        mmap_file_option_.first_mmap_size_ = 0;

        size_ = 0;
        fd_ = -1;
        data_ = NULL;
      }
    }

    bool MMapFile::sync_file()
    {
      if (NULL != data_ && size_ > 0)
      {
        //use MS_ASYNC
        return msync(data_, size_, MS_ASYNC) == 0;
      }
      return true;
    }

    bool MMapFile::map_file(const bool write)
    {
      int flags = PROT_READ;

      if (write)
      {
        flags |= PROT_WRITE;
      }

      if (fd_ < 0)
      {
        return false;
      }

      if (0 == mmap_file_option_.max_mmap_size_)
      {
        return false;
      }

      if (size_ < mmap_file_option_.max_mmap_size_)
      {
        size_ = mmap_file_option_.first_mmap_size_;
      }
      else
      {
        size_ = mmap_file_option_.max_mmap_size_;
      }

      if (!ensure_file_size(size_))
      {
        TBSYS_LOG(ERROR, "ensure file size failed in mremap. size: %d", size_);
        return false;
      }

      data_ = mmap(0, size_, flags | PROT_EXEC, MAP_SHARED, fd_, 0);

      if (MAP_FAILED == data_)
      {
        TBSYS_LOG(ERROR, "map file failed: %s", strerror(errno));
        size_ = 0;
        fd_ = -1;
        data_ = NULL;
        return false;
      };

      TBSYS_LOG(INFO, "mmap file successed, fd: %d maped size: %d, data: %p", fd_, size_, data_);

      return true;
    }

    bool MMapFile::remap_file()
    {
      TBSYS_LOG(INFO, "map file need remap. size: %d, fd: %d", size_, fd_);
      if (fd_ < 0 || data_ == NULL)
      {
        TBSYS_LOG(INFO, "mremap not mapped yet");
        return false;
      }

      if (size_ == mmap_file_option_.max_mmap_size_)
      {
        TBSYS_LOG(INFO, "already mapped max size, now size: %d, max size: %d", size_, mmap_file_option_.max_mmap_size_);
        return false;
      }

      int32_t new_size = size_ + mmap_file_option_.per_mmap_size_;
      if (new_size > mmap_file_option_.max_mmap_size_)
        new_size = mmap_file_option_.max_mmap_size_;

      if (!ensure_file_size(new_size))
      {
        TBSYS_LOG(ERROR, "ensure file size failed in mremap. new size: %d", new_size);
        return false;
      }

      TBSYS_LOG(INFO, "mremap start. fd: %d, now size: %d, new size: %d, old data: %p", fd_, size_, new_size, data_);
      void* new_map_data = mremap(data_, size_, new_size, MREMAP_MAYMOVE);
      if (MAP_FAILED == new_map_data)
      {
        TBSYS_LOG(ERROR, "ensure file size failed in mremap. new size: %d, error desc: %s", new_size, strerror(errno));
        return false;
      }
      else
      {
        TBSYS_LOG(INFO, "mremap success. fd: %d, now size: %d, new size: %d, old data: %p, new data: %p", fd_, size_, new_size,
            data_, new_map_data);
      }

      data_ = new_map_data;
      size_ = new_size;
      return true;
    }

    void* MMapFile::get_data() const
    {
      return data_;
    }

    int32_t MMapFile::get_size() const
    {
      return size_;
    }

    bool MMapFile::munmap_file()
    {
      if (munmap(data_, size_) == 0)
      {
        return true;
      }
      else
      {
        return false;
      }
    }

    bool MMapFile::ensure_file_size(const int32_t size)
    {
      struct stat s;
      if (fstat(fd_, &s) < 0)
      {
        TBSYS_LOG(ERROR, "fstat error, error desc: %s", strerror(errno));
        return false;
      }
      if (s.st_size < size)
      {
        if (ftruncate(fd_, size) < 0)
        {
          TBSYS_LOG(ERROR, "ftruncate error, size: %d, error desc: %s", size, strerror(errno));
          return false;
        }
      }

      return true;
    }
  } /** common **/
} /** tfs **/
