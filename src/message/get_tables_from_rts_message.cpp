/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_message.cpp 384 2011-05-31 07:47:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "common/stream.h"
#include "common/serialization.h"

#include "get_tables_from_rts_message.h"

namespace tfs
{
  namespace message
  {
    GetTableFromRtsResponseMessage::GetTableFromRtsResponseMessage():
      tables_(new char[common::MAX_BUCKET_DATA_LENGTH]),
      length_(common::MAX_BUCKET_DATA_LENGTH),
      version_(common::INVALID_TABLE_VERSION)
    {
      _packetHeader._pcode = common::RSP_RT_GET_TABLE_MESSAGE;
    }

    GetTableFromRtsResponseMessage::~GetTableFromRtsResponseMessage()
    {
      tbsys::gDeleteA(tables_);
    }

    int GetTableFromRtsResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int64(&version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&length_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_bytes(tables_, length_);
      }
      return iret;
    }

    int GetTableFromRtsResponseMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int64(version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(length_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_bytes(tables_, length_);
      }
      return iret;
    }

    int64_t GetTableFromRtsResponseMessage::length() const
    {
      return common::INT64_SIZE * 2 + length_;
    }

    GetTableFromRtsMessage::GetTableFromRtsMessage():
      reserve_(0)
    {
      _packetHeader._pcode = common::REQ_RT_GET_TABLE_MESSAGE;
    }

    GetTableFromRtsMessage::~GetTableFromRtsMessage()
    {

    }

    int GetTableFromRtsMessage::deserialize(common::Stream& input)
    {
      return input.get_int8(&reserve_);
    }

    int GetTableFromRtsMessage::serialize(common::Stream& output) const 
    {
      return output.set_int8(reserve_);
    }

    int64_t GetTableFromRtsMessage::length() const
    {
      return common::INT8_SIZE;
    }
  }/** message **/
}/** tfs **/
