/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: close_file_message.cpp 491 2011-06-14 03:11:29Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "close_file_message.h"

namespace tfs
{
  namespace message
  {
    CloseFileMessage::CloseFileMessage() :
      option_flag_(0), version_(0), lease_id_(common::INVALID_LEASE_ID)
    {
      _packetHeader._pcode = common::CLOSE_FILE_MESSAGE;
      memset(&close_file_info_, 0, sizeof(close_file_info_));
      memset(&block_, 0, sizeof(block_));
      memset(&file_info_, 0, sizeof(file_info_));
      close_file_info_.mode_ = common::CLOSE_FILE_MASTER;
    }

    CloseFileMessage::~CloseFileMessage()
    {
    }

    int CloseFileMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = close_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(close_file_info_.length());
        iret = input.get_vint64(ds_);
      }
      int32_t size = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }
      int32_t file_size = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&file_size);
      }
      if (size > 0 && common::TFS_SUCCESS == iret)
      {
        pos = 0;
        iret = block_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(block_.length());
        }
      }
      if (file_size > 0 && common::TFS_SUCCESS == iret)
      {
        pos = 0;
        iret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(file_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        input.get_int32(&option_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

    int64_t CloseFileMessage::length() const
    {
      int64_t len = close_file_info_.length() + common::Serialization::get_vint64_length(ds_) + common::INT_SIZE * 2;
      if (block_.block_id_ > 0)
      {
        len += block_.length();
      }
      if (file_info_.id_ > 0)
      {
        len += file_info_.length();
      }
      len += common::INT_SIZE;
      if (has_lease())
      {
        len += common::INT64_SIZE * 3;
      }
      return len;
    }

    int CloseFileMessage::serialize(common::Stream& output) const
    {
      int32_t size = block_.block_id_ > 0 ? block_.length() : 0;
      int32_t file_size = file_info_.id_ > 0 ? file_info_.length() : 0;
      if (has_lease())
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_id_));
      }
      int64_t pos = 0;
      int32_t iret = close_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(close_file_info_.length());
        iret = output.set_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(size);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(file_size);
      }
      if (size > 0 && common::TFS_SUCCESS == iret)
      {
        pos = 0;
        iret = block_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(block_.length());
        }
      }
      if (file_size > 0 && common::TFS_SUCCESS == iret)
      {
        iret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(file_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(option_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        // reparse, avoid push verion&lease again when clone twice;
        BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }
  }
}
