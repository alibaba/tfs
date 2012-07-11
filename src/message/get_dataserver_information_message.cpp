/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: unlink_file_message.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "get_dataserver_information_message.h"

namespace tfs
{
  namespace message
  {
    GetDataServerInformationMessage::GetDataServerInformationMessage():
      flag_(0)
    {
      setPCode(common::GET_DATASERVER_INFORMATION_MESSAGE);
    }
    GetDataServerInformationMessage::~GetDataServerInformationMessage()
    {

    }
    int GetDataServerInformationMessage::serialize(common::Stream& output) const 
    {
      return output.set_int16(flag_);
    }
    int GetDataServerInformationMessage::deserialize(common::Stream& input)
    {
      return input.get_int16(&flag_);
    }
    int64_t GetDataServerInformationMessage::length() const
    {
      return common::INT16_SIZE;
    }
    GetDataServerInformationResponseMessage::GetDataServerInformationResponseMessage():
      bit_map_element_count_(0),
      data_length_(0),
      data_(NULL),
      flag_(0),
      alloc_(false)
    {
      setPCode(common::GET_DATASERVER_INFORMATION_RESPONSE_MESSAGE);
    }
    GetDataServerInformationResponseMessage::~GetDataServerInformationResponseMessage()
    {
      if (alloc_)
      {
        tbsys::gDeleteA(data_);
      }
    }
    int GetDataServerInformationResponseMessage::serialize(common::Stream& output) const 
    {
      int64_t pos = 0;
      int32_t iret = sblock_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        iret = info_.serialize(output.get_free(), output.get_free_length(), pos);
      }
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(sblock_.length() + info_.length());
        iret = output.set_int16(flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(bit_map_element_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(data_length_); 
        if (common::TFS_SUCCESS == iret)
        {
          iret = output.set_bytes(data_, data_length_);
        }
      }
      return iret;
    }

    int GetDataServerInformationResponseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = sblock_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        iret = info_.deserialize(input.get_data(), input.get_data_length(), pos);
      }
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(sblock_.length() + info_.length());
        iret = input.get_int16(&flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&bit_map_element_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&data_length_);
        if (common::TFS_SUCCESS == iret)
        {
          if (data_length_ > 0)
          {
            data_ = input.get_data();
          }
        }
      }
      return iret;
    }

    int64_t GetDataServerInformationResponseMessage::length() const
    {
      return sblock_.length() + info_.length() + common::INT16_SIZE + common::INT_SIZE * 2 + data_length_;
    }

    char* GetDataServerInformationResponseMessage::alloc_data(const int64_t length)
    {
      char* data = NULL;
      if (length > 0)
      {
        tbsys::gDeleteA(data_);
        data_length_ = length;
        alloc_  = true;
        data = data_ = new char[length];
      }
      return data;
    }
  }
}
