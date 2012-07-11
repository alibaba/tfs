/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: create_filename_message.cpp 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "create_filename_message.h"
namespace tfs
{
  namespace message
  {
    CreateFilenameMessage::CreateFilenameMessage() :
      block_id_(0), file_id_(0)
    {
      _packetHeader._pcode = common::CREATE_FILENAME_MESSAGE;
    }

    CreateFilenameMessage::~CreateFilenameMessage()
    {

    }

    int CreateFilenameMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*> (&file_id_));
      }
      return iret;
    }

    int64_t CreateFilenameMessage::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE;
    }

    int CreateFilenameMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(file_id_);
      }
      return iret;
    }

    RespCreateFilenameMessage::RespCreateFilenameMessage() :
      block_id_(0), file_id_(0), file_number_(0)
    {
      _packetHeader._pcode = common::RESP_CREATE_FILENAME_MESSAGE;
    }

    RespCreateFilenameMessage::~RespCreateFilenameMessage()
    {
    }

    int RespCreateFilenameMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*> (&file_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*> (&file_number_));
      }
      return iret;
    }

    int64_t RespCreateFilenameMessage::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE * 2;
    }

    int RespCreateFilenameMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(file_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(file_number_);
      }
      return iret;
    }
  }
}
