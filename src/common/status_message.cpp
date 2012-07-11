/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: status_packet.cpp 186 2011-04-28 16:07:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "status_message.h"
namespace tfs
{
  namespace common
  {
    StatusMessage::StatusMessage() :
      length_(0),
      status_(STATUS_MESSAGE_ERROR)
    {
      msg_[0] = '\0';
      _packetHeader._pcode = STATUS_MESSAGE;
    }

    StatusMessage::StatusMessage(const int32_t status, const char* const str):
      length_(0),
      status_(status)
    {
      msg_[0] = '\0';
      _packetHeader._pcode = STATUS_MESSAGE;
      set_message(status, str);
    }

    void StatusMessage::set_message(const int32_t status, const char* const str)
    {
      status_ = status;
      if (NULL != str)
      {
        int64_t length = strlen(str);
        if (length > MAX_ERROR_MSG_LENGTH)
        {
          length = MAX_ERROR_MSG_LENGTH;
        }
        memcpy(msg_, str, length);
        msg_[length] = '\0';
        length_ = length;
      }
      //TBSYS_LOG(DEBUG, "status msg : status: %d, length: %ld, %p", status_, length_, str);
    }

    StatusMessage::~StatusMessage()
    {

    }

    int StatusMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_int32(status_);
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(msg_);
      }
      return iret;
    }

    int StatusMessage::deserialize(Stream& input)
    {
      //Func::hex_dump(input.get_data(), input.get_data_length());
      int32_t iret = input.get_int32(&status_);
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(MAX_ERROR_MSG_LENGTH, msg_, length_);
        if (TFS_SUCCESS == iret)
        {
          msg_[length_] = '\0';
        }
      }
      return iret;
    }

    int64_t StatusMessage::length() const
    {
      return INT_SIZE + Serialization::get_string_length(msg_);
    }
  }
}
