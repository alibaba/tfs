/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog_sync_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "oplog_sync_message.h"

namespace tfs
{
  namespace message
  {
    OpLogSyncMessage::OpLogSyncMessage() :
      alloc_(false), length_(0), data_(NULL)
    {
      _packetHeader._pcode = common::OPLOG_SYNC_MESSAGE;
    }

    OpLogSyncMessage::~OpLogSyncMessage()
    {
      if ((data_ != NULL) && alloc_)
      {
        tbsys::gDeleteA(data_);
      }
    }

    void OpLogSyncMessage::set_data(const char* data, const int64_t length)
    {
      assert(length > 0);
      assert(data != NULL);
      tbsys::gDeleteA(data_);
      length_ = length;
      data_ = new char[length];
      memcpy(data_, data, length);
      alloc_ = true;
    }

    int OpLogSyncMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&length_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = length_ > 0 && length_ <= input.get_data_length() ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          set_data(input.get_data(), length_);
          input.drain(length_);
        }
      }
      return iret;
    }

    int64_t OpLogSyncMessage::length() const
    {
      return (length_ > 0 && NULL != data_ ) ? common::INT_SIZE + length_ : common::INT_SIZE;
    }

    int OpLogSyncMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(length_);
      if (common::TFS_SUCCESS == iret)
      {
        if (length_ > 0 && NULL != data_)
        {
          iret = output.set_bytes(data_, length_);
        }
      }
      return iret;
    }

    OpLogSyncResponeMessage::OpLogSyncResponeMessage() :
      complete_flag_(OPLOG_SYNC_MSG_COMPLETE_YES)
    {
      _packetHeader._pcode = common::OPLOG_SYNC_RESPONSE_MESSAGE;
    }

    OpLogSyncResponeMessage::~OpLogSyncResponeMessage()
    {

    }

    int OpLogSyncResponeMessage::deserialize(common::Stream& input)
    {
      return input.get_int8(reinterpret_cast<int8_t*>(&complete_flag_));
    }

    int64_t OpLogSyncResponeMessage::length() const
    {
      return common::INT8_SIZE;
    }

    int OpLogSyncResponeMessage::serialize(common::Stream& output) const 
    {
      return output.set_int8(complete_flag_);
    }
  }
}
