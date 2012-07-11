/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: server_status_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "server_status_message.h"

namespace tfs
{
  namespace message
  {
    GetServerStatusMessage::GetServerStatusMessage() :
      from_row_(0), return_row_(1000)
    {
      _packetHeader._pcode = common::GET_SERVER_STATUS_MESSAGE;
    }

    GetServerStatusMessage::~GetServerStatusMessage()
    {
    }

    int GetServerStatusMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&status_type_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&from_row_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&return_row_);
      }
      return iret;
    }

    int64_t GetServerStatusMessage::length() const
    {
      return common::INT_SIZE * 3;
    }

    int GetServerStatusMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(status_type_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(from_row_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(return_row_);
      }
      return iret;
    }

    AccessStatInfoMessage::AccessStatInfoMessage() :
      from_row_(0), return_row_(0), has_next_(0)
    {
      _packetHeader._pcode = common::ACCESS_STAT_INFO_MESSAGE;
    }

    AccessStatInfoMessage::~AccessStatInfoMessage()
    {

    }

    int AccessStatInfoMessage::serialize(common::Stream& output) const
    {
      int32_t size = stats_.size();
      size -= from_row_;
      if (size < 0)
      {
        size = 0;
      }
      if (size > return_row_)
      {
        has_next_ = 1;
        size = return_row_;
      }
      int32_t iret = output.set_int32(has_next_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = set_counter_map(output, stats_, from_row_, return_row_, size);
      }
      return iret;
    }

    int AccessStatInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&has_next_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = get_counter_map(input, stats_);
      }
      return iret;
    }

    int AccessStatInfoMessage::set_counter_map(common::Stream& output, const COUNTER_TYPE & map, int32_t from_row,
        int32_t return_row, int32_t size) const
    {
      int32_t iret = output.set_int32(size);
      if (common::TFS_SUCCESS == iret)
      {
        if (size > 0)
        {
          int64_t pos = 0;
          int32_t count = 0;
          COUNTER_TYPE::const_iterator iter = map.begin();
          for (; iter != map.end(); ++iter, ++count)
          {
            if (count < from_row)
              continue;
            if (count >= from_row + return_row)
              break;
            iret = output.set_int32(iter->first);
            if (common::TFS_SUCCESS != iret)
              break;
            pos = 0;
            iret = const_cast<common::Throughput*>(&iter->second)->serialize(output.get_free(), output.get_free_length(), pos);
            if (common::TFS_SUCCESS == iret)
              output.pour(iter->second.length());
            else
              break;
          }
        }
      }
      return iret;
    }

    int AccessStatInfoMessage::get_counter_map(common::Stream& input, COUNTER_TYPE & map)
    {
      int32_t size = 0;
      int32_t iret = input.get_int32(&size);
      if (common::TFS_SUCCESS == iret)
      {
        common::Throughput t;
        int64_t pos = 0;
        int32_t id = 0;
        for (int32_t i = 0; i < size; ++i)
        {
          iret = input.get_int32(&id);
          if (common::TFS_SUCCESS != iret)
            break;
          pos = 0;
          iret = t.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS != iret)
            break;
          else
          {
            input.drain(t.length());
            map.insert(COUNTER_TYPE::value_type(id, t));
          }
        }
      }
      return iret;
    }

    int64_t AccessStatInfoMessage::length() const
    {
      int64_t len = common::INT_SIZE;//has_next_
      len += common::INT_SIZE; //size of return row
      int32_t size = stats_.size();
      size -= from_row_;
      if (size < 0)
        size = 0;
      if (size > return_row_)
        size = return_row_;
      common::Throughput t;
      len += size * (common::INT_SIZE + t.length());
      return len;
    }

    ShowServerInformationMessage::ShowServerInformationMessage()
    {
      _packetHeader._pcode = common::SHOW_SERVER_INFORMATION_MESSAGE;
      memset(&param, 0, sizeof(param));
    }

    ShowServerInformationMessage::~ShowServerInformationMessage()
    {

    }
    int ShowServerInformationMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = param.deserialize(input.get_data(), input.get_data_length(),  pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(pos);
      }
      return iret;
    }

    int ShowServerInformationMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = param.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(param.length());
      }
      return iret;
    }

    int64_t ShowServerInformationMessage::length() const
    {
      return param.length();
    }
  }
}
