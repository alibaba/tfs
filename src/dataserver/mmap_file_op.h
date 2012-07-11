/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file_op.h 764 2011-09-07 06:04:06Z duanfei@taobao.com $
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
#ifndef TFS_DATASERVER_MMAPFILE_OP_H_
#define TFS_DATASERVER_MMAPFILE_OP_H_

#include "common/file_op.h"
#include "common/mmap_file.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {
    class ParaInfo
    {
      public:
        explicit ParaInfo(const int32_t size) :
          flag_(false)
        {
          new_buf_ = new char[size];
          self_buf_ = NULL;
        }

        ~ParaInfo()
        {
          tbsys::gDeleteA(new_buf_);
        }

        inline char* get_actual_buf() const
        {
          if (flag_)
          {
            return self_buf_;
          }
          else
          {
            return new_buf_;
          }
        }

        inline char* get_new_buf()
        {
          flag_ = false;
          return new_buf_;
        }

        inline bool get_flag() const
        {
          return flag_;
        }

        inline void set_self_buf(char* buf)
        {
          self_buf_ = buf;
          flag_ = true;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ParaInfo);
        bool flag_;
        char* self_buf_;
        char* new_buf_;
    };

    class MMapFileOperation: public common::FileOperation
    {
      public:
        explicit MMapFileOperation(const std::string& file_name, int open_flags = O_RDWR | O_LARGEFILE) :
          FileOperation(file_name, open_flags), is_mapped_(false), map_file_(NULL)
        {

        }

        ~MMapFileOperation()
        {
          tbsys::gDelete(map_file_);
        }

        int pread_file(char* buf, const int32_t size, const int64_t offset);
        int pread_file(ParaInfo& m_meta_info, const int32_t size, const int64_t offset);
        int pwrite_file(const char* buf, const int32_t size, const int64_t offset);

        int mmap_file(const common::MMapOption& mmap_option);
        int munmap_file();
        int rename_file();
        void* get_map_data() const;
        int flush_file();

      private:
        MMapFileOperation();
        DISALLOW_COPY_AND_ASSIGN(MMapFileOperation);

        bool is_mapped_;
        common::MMapFile* map_file_;
    };

  }
}
#endif //TFS_DATASERVER_MMAPFILE_OP_H_
