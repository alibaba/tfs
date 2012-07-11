/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rollback_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "rollback_message.h"

namespace tfs
{
  namespace message
  {
    RollbackMessage::RollbackMessage() :
      act_type_(0)
    {
      memset(&block_info_, 0, block_info_.length());
      memset(&file_info_, 0, file_info_.length());
      _packetHeader._pcode = common::ROLLBACK_MESSAGE;
    }

    RollbackMessage::~RollbackMessage()
    {
    }

    int RollbackMessage::deserialize(common::Stream& input)
    {
      int32_t block_len = 0;
      int32_t file_info_len = 0;
      int32_t iret = input.get_int32(&act_type_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&block_len);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&file_info_len);
      }
      int64_t pos = 0;
      if (common::TFS_SUCCESS == iret)
      {
        if (block_len > 0)
        {
          iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(block_info_.length());
          }
        }
      }
      
      if (common::TFS_SUCCESS == iret)
      {
        if (file_info_len > 0)
        {
          pos = 0;
          iret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(file_info_.length());
          }
        }
      }
      return iret;
    }

    int64_t RollbackMessage::length() const
    {
      int64_t len = common::INT_SIZE * 3;
      if (block_info_.block_id_ > 0)
        len += block_info_.length();
      if (file_info_.id_ > 0)
        len += file_info_.length();
      return len;
    }

    int RollbackMessage::serialize(common::Stream& output) const 
    {
      int32_t block_len = block_info_.block_id_ > 0 ? block_info_.length() : 0;
      int32_t file_info_len = file_info_.id_ > 0 ? file_info_.length() : 0;
      int32_t iret = output.set_int32(act_type_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(block_len);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(file_info_len);
      }
      int64_t pos = 0;
      if (common::TFS_SUCCESS == iret)
      {
        if (block_len > 0)
        {
          iret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(block_info_.length());
          }
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (file_info_len > 0)
        {
          pos = 0;
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
