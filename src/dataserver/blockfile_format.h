/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blockfile_format.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   daozong <daozong@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_BLOCKFILEFORMATER_H_
#define TFS_DATASERVER_BLOCKFILEFORMATER_H_

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "common/error_msg.h"
#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {
    class FileFormater
    {
      public:
        FileFormater()
        {
        }
        virtual ~FileFormater()
        {
        }

      public:
        virtual int block_file_format(const int fd, const int64_t size) = 0;
    };

    class Ext4FileFormater: public FileFormater
    {
      public:
        Ext4FileFormater()
        {
        }
        ~Ext4FileFormater()
        {
        }

      public:
        int block_file_format(const int file_fd, const int64_t file_size);

      private:
    };

    int Ext4FileFormater::block_file_format(const int file_fd, const int64_t file_size)
    {
      if (file_fd < 0)
      {
        return file_fd;
      }

      if (file_size <= 0)
      {
        return file_size;
      }

      int ret = common::TFS_SUCCESS;
#if TFS_DS_FALLOCATE
      ret = fallocate(file_fd, 0, 0, file_size);
#else
#ifdef __NR_fallocate
      ret = syscall(__NR_fallocate, file_fd, 0, 0, file_size);
#else
      ret = common::EXIT_FALLOCATE_NOT_IMPLEMENT;
#endif
#endif

      return ret;
    }

    class Ext3FullFileFormater: public FileFormater
    {
      public:
        Ext3FullFileFormater()
        {
        }
        ~Ext3FullFileFormater()
        {
        }

      public:
        int block_file_format(const int file_fd, const int64_t file_size);

      private:
    };

    int Ext3FullFileFormater::block_file_format(const int file_fd, const int64_t file_size)
    {
      if (file_fd < 0)
      {
        return file_fd;
      }

      if (file_size <= 0)
      {
        return file_size;
      }

      int offset = 0;
      int ret = posix_fallocate(file_fd, offset, file_size);
      if (ret != 0)
      {
        return ret;
      }

      return common::TFS_SUCCESS;
    }

    class Ext3SimpleFileFormater: public FileFormater
    {
      public:
        Ext3SimpleFileFormater()
        {
        }
        ~Ext3SimpleFileFormater()
        {
        }

      public:
        int block_file_format(const int file_fd, const int64_t file_size);

      private:
    };

    int Ext3SimpleFileFormater::block_file_format(const int file_fd, const int64_t file_size)
    {
      if (file_fd < 0)
      {
        return file_fd;
      }

      if (file_size <= 0)
      {
        return file_size;
      }

      int ret = ftruncate(file_fd, file_size);
      if (ret < 0)
      {
        return ret;
      }

      return common::TFS_SUCCESS;
    }

  }
}
#endif //TFS_DATASERVER_BLOCKFILEFORMATER_H_
