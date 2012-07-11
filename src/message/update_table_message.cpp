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

#include "update_table_message.h"

namespace tfs
{
  namespace message
  {
    UpdateTableMessage::UpdateTableMessage():
      tables_(NULL),
      length_(-1),
      version_(common::INVALID_TABLE_VERSION),
      phase_(-1),
      alloc_(false)
    {
      _packetHeader._pcode = common::REQ_RT_UPDATE_TABLE_MESSAGE;
    }

    UpdateTableMessage::~UpdateTableMessage()
    {
      if (alloc_)
        tbsys::gDeleteA(tables_);
    }

    char* UpdateTableMessage::alloc(const int64_t length)
    {
      length_ = length < 0 ?  -1 : length == 0 ? 0 : length;
      tbsys::gDeleteA(tables_);
      if (length > 0)
      {
        tables_ = new char[length_]; 
      }
      alloc_ = true;
      return tables_;
    }

    int UpdateTableMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int64(&version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(&phase_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&length_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (length_ > 0
          && length_ <= common::MAX_BUCKET_ITEM_DEFAULT * common::INT64_SIZE)
        {
          char* data = alloc(length_);
          iret = input.get_bytes(data, length_);
        }
      }
      return iret;
    }

    int UpdateTableMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int64(version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(phase_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(length_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (length_ > 0
          && length_ <= common::MAX_BUCKET_ITEM_DEFAULT * common::INT64_SIZE
          && NULL != tables_)
        {
          iret = output.set_bytes(tables_, length_);
        }
      }
      return iret;
    }

    int64_t UpdateTableMessage::length() const
    {
      return common::INT64_SIZE * 2 + common::INT8_SIZE + length_;
    }

    UpdateTableResponseMessage::UpdateTableResponseMessage():
      version_(common::INVALID_TABLE_VERSION),
      phase_(-1),
      status_(common::TFS_ERROR)
    {
      _packetHeader._pcode = common::RSP_RT_UPDATE_TABLE_MESSAGE;
    }

    UpdateTableResponseMessage::~UpdateTableResponseMessage()
    {

    }

    int UpdateTableResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int64(&version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(&phase_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int8(&status_);
      }
      return iret;
    }

    int UpdateTableResponseMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int64(version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(phase_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(status_);
      }
      return iret;
    }

    int64_t UpdateTableResponseMessage::length() const
    {
      return common::INT64_SIZE + common::INT8_SIZE * 2;
    }
  }/** message **/
}/** tfs **/
