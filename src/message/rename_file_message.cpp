/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rename_file_message.cpp 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "rename_file_message.h"
namespace tfs
{
  namespace message
  {
    RenameFileMessage::RenameFileMessage() :
      option_flag_(0), version_(0), lease_id_(common::INVALID_LEASE_ID)
    {
      _packetHeader._pcode = common::RENAME_FILE_MESSAGE;
      memset(&rename_file_info_, 0, sizeof(common::RenameFileInfo));
    }

    RenameFileMessage::~RenameFileMessage()
    {
    }

    int RenameFileMessage::deserialize(common::Stream& input)
    {
      int64_t pos  = 0;
      int32_t iret = rename_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(rename_file_info_.length());
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&option_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

    int64_t RenameFileMessage::length() const
    {
      int64_t len = rename_file_info_.length() + common::Serialization::get_vint64_length(ds_) + common::INT_SIZE;
      if (has_lease())
      {
        len += common::INT64_SIZE * 3;
      }
      return len;
    }

    int RenameFileMessage::serialize(common::Stream& output) const
    {
      if (has_lease())
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_id_));
      }

      int64_t pos  = 0;
      int32_t iret = rename_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(rename_file_info_.length());
        iret = output.set_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(option_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }
  }
}
