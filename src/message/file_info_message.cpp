/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_info_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "file_info_message.h"

namespace tfs
{
  namespace message
  {
    FileInfoMessage::FileInfoMessage() :
      block_id_(0), file_id_(0), mode_(0)
    {
      _packetHeader._pcode = common::FILE_INFO_MESSAGE;
    }

    FileInfoMessage::~FileInfoMessage()
    {
    }

    int FileInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*> (&file_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*> (&mode_));
      }
      return iret;
    }

    int64_t FileInfoMessage::length() const
    {
      return common::INT_SIZE * 2 + common::INT64_SIZE;
    }

    int FileInfoMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(file_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(mode_);
      }
      return iret;
    }

    RespFileInfoMessage::RespFileInfoMessage()
    {
      _packetHeader._pcode = common::RESP_FILE_INFO_MESSAGE;
      memset(&file_info_, 0, file_info_.length());
    }

    RespFileInfoMessage::~RespFileInfoMessage()
    {
    }

    int RespFileInfoMessage::deserialize(common::Stream& input)
    {
      int32_t size;
      int32_t iret = input.get_int32(&size);
      if (size > 0)
      {
        if (common::TFS_SUCCESS == iret)
        {
          int64_t pos = 0;
          iret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(file_info_.length());
          }
        }
      }
      return iret;
    }

    int64_t RespFileInfoMessage::length() const
    {
      return file_info_ .id_ > 0 ? common::INT_SIZE + file_info_.length() : common::INT_SIZE;
    }

    int RespFileInfoMessage::serialize(common::Stream& output) const 
    {
      int32_t size = file_info_.id_ > 0 ? file_info_.length() : 0;
      int32_t iret = output.set_int32(size);
      if (size > 0)
      {
        if (common::TFS_SUCCESS == iret)
        {
          int64_t pos = 0;
          iret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(file_info_.length());
          }
        }
      }
      return iret;
    }
  }
}
