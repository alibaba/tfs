/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: write_data_message.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "write_data_message.h"

namespace tfs
{
  namespace message
  {
    WriteDataMessage::WriteDataMessage() :
      data_(NULL), version_(0), lease_id_(common::INVALID_LEASE_ID)
    {
      _packetHeader._pcode = common::WRITE_DATA_MESSAGE;
      memset(&write_data_info_, 0, sizeof(write_data_info_));
      ds_.clear();
    }

    WriteDataMessage::~WriteDataMessage()
    {

    }

    int WriteDataMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = write_data_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain( write_data_info_.length());
        iret = input.get_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret
        && write_data_info_.length_ > 0)
      {
        data_ = input.get_data();
        input.drain(write_data_info_.length_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

    int64_t WriteDataMessage::length() const
    {
      int64_t len = write_data_info_.length() + common::Serialization::get_vint64_length(ds_);
      if (write_data_info_.length_ > 0)
      {
        len += write_data_info_.length_;
      }
      if (has_lease())
      {
        len += common::INT64_SIZE * 3;
      }
      return len;
    }

    int WriteDataMessage::serialize(common::Stream& output) const
    {
      if (has_lease())
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t>(version_));
        ds_.push_back(static_cast<uint64_t>(lease_id_));
      }
      int64_t pos = 0;
      int32_t iret = write_data_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(write_data_info_.length());
        iret = output.set_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret
          && write_data_info_.length_ > 0)
      {
        iret = output.set_bytes(data_, write_data_info_.length_);
      }
      // reparse, avoid push verion&lease again when clone twice;
      if (common::TFS_SUCCESS == iret)
      {
        parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

#ifdef _DEL_001_
    RespWriteDataMessage::RespWriteDataMessage():
      length_(0)
    {
      _packetHeader._pcode = common::RESP_WRITE_DATA_MESSAGE;
    }

    RespWriteDataMessage::~RespWriteDataMessage()
    {
    }

    int RespWriteDataMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&length_);
    }

    int64_t RespWriteDataMessage::length() const
    {
      return common::INT_SIZE;
    }

    int RespWriteDataMessage::serialize(common::Stream& output) const
    {
      return output.set_int32(lenght_);
    }

#endif

    WriteRawDataMessage::WriteRawDataMessage() :
      data_(NULL), flag_(0)
    {
      _packetHeader._pcode = common::WRITE_RAW_DATA_MESSAGE;
      memset(&write_data_info_, 0, sizeof(write_data_info_));
    }

    WriteRawDataMessage::~WriteRawDataMessage()
    {

    }

    int WriteRawDataMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = write_data_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(write_data_info_.length());
        if (write_data_info_.length_ > 0)
        {
          data_ = input.get_data();
          input.drain( write_data_info_.length_);
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&flag_);
      }
      return iret;
    }

    int64_t WriteRawDataMessage::length() const
    {
      int64_t len = write_data_info_.length() + common::INT_SIZE;
      if (write_data_info_.length_ > 0)
      {
        len += write_data_info_.length_;
      }
      return len;
    }

    int WriteRawDataMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = write_data_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(write_data_info_.length());
        if (write_data_info_.length_ > 0)
        {
          iret = output.set_bytes(data_, write_data_info_.length_);
        }
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(flag_);
      }
      return iret;
    }

    WriteInfoBatchMessage::WriteInfoBatchMessage() :
      cluster_(0)
    {
      _packetHeader._pcode = common::WRITE_INFO_BATCH_MESSAGE;
      memset(&write_data_info_, 0, sizeof(write_data_info_));
      memset(&block_info_, 0, sizeof(block_info_));
      meta_list_.clear();
    }

    WriteInfoBatchMessage::~WriteInfoBatchMessage()
    {

    }

    int WriteInfoBatchMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = write_data_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(write_data_info_.length());
      }
      int32_t have_block = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&have_block);
      }

      if (common::TFS_SUCCESS == iret)
      {
        if (1 == have_block)
        {
          pos = 0;
          iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(block_info_.length());
          }
        }
      }

      int32_t size = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }
      if (common::TFS_SUCCESS == iret)
      {
        for (int32_t i = 0; i < size; ++i)
        {
          pos = 0;
          common::RawMeta raw_meta;
          iret = raw_meta.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            meta_list_.push_back(raw_meta);
            input.drain(raw_meta.length());
          }
          else
          {
            break;
          }
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&cluster_);
      }
      return iret;
    }

    int64_t WriteInfoBatchMessage::length() const
    {
      int64_t len = write_data_info_.length() + common::INT_SIZE * 2;
      if (block_info_.block_id_ > 0)
      {
        len += block_info_.length();
      }
      len += common::INT_SIZE;
      common::RawMeta raw_data;
      len += meta_list_.size() * raw_data.length();
      return len;
    }

    int WriteInfoBatchMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t have_block = (block_info_.block_id_ > 0) ? 1 : 0;
      int32_t iret = write_data_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(write_data_info_.length());
        iret = output.set_int32(have_block);
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (1 == have_block)
        {
          pos = 0;
          iret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(block_info_.length());
          }
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(meta_list_.size());
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::RawMetaVec::const_iterator iter = meta_list_.begin();
        for (; iter != meta_list_.end(); ++iter)
        {
          pos = 0;
          iret = const_cast<common::RawMeta*>((&(*iter)))->serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour((*iter).length());
          }
          else
          {
            break;
          }
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(cluster_);
      }
      return iret;
    }
  }
}
