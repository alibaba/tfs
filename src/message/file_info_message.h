/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_info_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_FILEINFOMESSAGE_H_
#define TFS_MESSAGE_FILEINFOMESSAGE_H_

#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class FileInfoMessage: public common::BasePacket 
    {
      public:
        FileInfoMessage();
        virtual ~FileInfoMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }
        inline uint64_t get_file_id() const
        {
          return file_id_;
        }
        inline void set_mode(const int32_t mode)
        {
          mode_ = mode;
        }
        inline int32_t get_mode() const
        {
          return mode_;
        }
      protected:
        uint32_t block_id_;
        uint64_t file_id_;
        int32_t mode_;
    };

    class RespFileInfoMessage: public common::BasePacket 
    {
      public:
        RespFileInfoMessage();
        virtual ~RespFileInfoMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_file_info(common::FileInfo* const file_info)
        {
          if (NULL != file_info)
          {
            file_info_ = *file_info;
          }
        }
        inline const common::FileInfo* get_file_info()  const
        {
          return &file_info_;
        }
      protected:
        common::FileInfo file_info_;
    };
  }
}
#endif
