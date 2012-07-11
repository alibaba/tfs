/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compact_block_message.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "compact_block_message.h"

namespace tfs
{
  namespace message
  {
    CompactBlockMessage::CompactBlockMessage() :
      preserve_time_(0), block_id_(0)
    {
      _packetHeader._pcode = common::COMPACT_BLOCK_MESSAGE;
    }

    CompactBlockMessage::~CompactBlockMessage()
    {

    }

    int CompactBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&preserve_time_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&is_owner_);
      }
      if (common::TFS_SUCCESS == iret
        && input.get_data_length() > 0)
      {
        iret = input.get_int64(&seqno_);
      }
      return iret;
    }


    int64_t CompactBlockMessage::length() const
    {
      return common::INT_SIZE * 3 + common::INT64_SIZE;
    }

    int CompactBlockMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(preserve_time_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(block_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(is_owner_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(seqno_);
      }
      return iret;
    }

    CompactBlockCompleteMessage::CompactBlockCompleteMessage() :
      block_id_(0), success_(common::PLAN_STATUS_END), server_id_(0), flag_(0)
    {
      memset(&block_info_, 0, sizeof(block_info_));
      _packetHeader._pcode = common::BLOCK_COMPACT_COMPLETE_MESSAGE;
    }

    CompactBlockCompleteMessage::~CompactBlockCompleteMessage()
    {
    }

    int CompactBlockCompleteMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&success_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&flag_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(ds_list_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(block_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret
        && input.get_data_length() > 0)
      {
        iret = input.get_int64(&seqno_);
      }
      return iret;
    }

    int CompactBlockCompleteMessage::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &success_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&server_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&flag_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_vint64(data, data_len, pos, ds_list_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = block_info_.deserialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == iret
          && pos + common::INT64_SIZE <= data_len)
      {
        iret = common::Serialization::get_int64(data, data_len, pos, &seqno_);
      }
      return iret;
    }

    int64_t CompactBlockCompleteMessage::length() const
    {
      return  common::INT_SIZE * 3 + common::INT64_SIZE * 2 + block_info_.length() + common::Serialization::get_vint64_length(ds_list_);
    }

    int CompactBlockCompleteMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(success_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(server_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint64(ds_list_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(block_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        output.set_int64(seqno_);
      }
      return iret;
    }
    void CompactBlockCompleteMessage::dump(void) const
    {
      std::string ipstr;
      common::VUINT64::const_iterator iter = ds_list_.begin();
      for (; iter != ds_list_.end(); ++iter)
      {
        ipstr += tbsys::CNetUtil::addrToString((*iter));
        ipstr += "/";
      }
      TBSYS_LOG(INFO, "compact block: %u, seqno: %"PRI64_PREFIX"d, success: %d, serverid: %"PRI64_PREFIX"u, flag: %d, block: %u, version: %u"
          "file_count: %u, size: %u, delfile_count: %u, del_size: %u, seqno: %u, ds_list: %zd, servers: %s",
          block_id_, seqno_, success_, server_id_, flag_, block_info_.block_id_, block_info_.version_, block_info_.file_count_, block_info_.size_,
          block_info_.del_file_count_, block_info_.del_size_, block_info_.seq_no_, ds_list_.size(), ipstr.c_str());
    }
  }
}
