/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: crc_error_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "crc_error_message.h"

namespace tfs
{
  namespace message
  {
    CrcErrorMessage::CrcErrorMessage() :
      block_id_(0), file_id_(0), crc_(0), error_flag_(common::CRC_DS_PATIAL_ERROR)
    {
      _packetHeader._pcode = common::CRC_ERROR_MESSAGE;
    }

    CrcErrorMessage::~CrcErrorMessage()
    {
    }

    int CrcErrorMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*>(&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*>(&file_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&crc_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&error_flag_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(fail_server_);
      }
      return iret;
    }

    int64_t CrcErrorMessage::length() const
    {
      return common::INT_SIZE * 3 + common::INT64_SIZE + common::Serialization::get_vint64_length(fail_server_);
    }

    int CrcErrorMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(file_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(crc_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(error_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint64(fail_server_);
      }
      return iret;
    }
  }
}
