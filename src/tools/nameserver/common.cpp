/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: common.cpp 447 2011-06-08 09:32:19Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "common.h"
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    ServerBase::ServerBase():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
#ifdef TFS_NS_DEBUG
      total_elect_num_ = 0;
#endif
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }
    ServerBase::~ServerBase()
    {
    }

    int ServerBase::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t type)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
#ifdef TFS_NS_DEBUG
      total_elect_num_ = input.readInt64();
#endif
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      current_time_ = input.readInt64();
      status_ = (DataServerLiveStatus)input.readInt32();
      offset += (len - input.getDataLen());

      if (type & SERVER_TYPE_BLOCK_LIST)
      {
        len = input.getDataLen();
        int32_t hold_size = input.readInt32();
        while (hold_size > 0)
        {
          hold_.insert(input.readInt32());
          hold_size--;
        }
        offset += (len - input.getDataLen());
      }

      if (type & SERVER_TYPE_BLOCK_WRITABLE)
      {
        len = input.getDataLen();
        int32_t writable_size = input.readInt32();
        while (writable_size > 0)
        {
          writable_.insert(input.readInt32());
          writable_size--;
        }
        offset += (len - input.getDataLen());
      }

      if (type & SERVER_TYPE_BLOCK_MASTER)
      {
        len = input.getDataLen();
        int32_t master_size = input.readInt32();
        while (master_size > 0)
        {
          master_.insert(input.readInt32());
          master_size--;
        }
        offset += (len - input.getDataLen());
      }
      return TFS_SUCCESS;
    }

    void ServerBase::dump() const
    {
      TBSYS_LOG(INFO, "server_id: %"PRI64_PREFIX"d, use_capacity: %"PRI64_PREFIX"d, total_capacity: %"PRI64_PREFIX"d, current_load: %d, block_count: %d, last_update_time: %s, startup_time: %s, write_byte: %"PRI64_PREFIX"d, write_file_count: %"PRI64_PREFIX"d, read_byte: %"PRI64_PREFIX"d, read_file_count: %"PRI64_PREFIX"d current_time: %s, status: %d, hold_size: %zd, writable size: %zd, master size: %zd",
          id_,
          use_capacity_,
          total_capacity_,
          current_load_,
          block_count_,
          Func::time_to_str(last_update_time_).c_str(),
          Func::time_to_str(startup_time_).c_str(),
          total_tp_.write_byte_,
          total_tp_.write_file_count_,
          total_tp_.read_byte_,
          total_tp_.read_file_count_,
          Func::time_to_str(current_time_).c_str(),
          status_,
          hold_.size(),
          writable_.size(),
          master_.size()
          );
    }

    //************************BLOCK**************************
    BlockBase::BlockBase()
    {
      memset(&info_, 0, sizeof(info_));
      server_list_.clear();
    }
    BlockBase::~BlockBase()
    {
    }
    int32_t BlockBase::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
      input.readBytes(&info_, sizeof(info_));

      int8_t server_size = input.readInt8();
      while (server_size > 0)
      {
        ServerInfo server_info;
        server_info.server_id_ = input.readInt64();
        server_list_.push_back(server_info);
        server_size--;
      }
      offset += (len - input.getDataLen());
      return TFS_SUCCESS;
    }

    void BlockBase::dump() const
    {
      TBSYS_LOG(INFO, "block_id: %u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %u, copys: %Zd",
          info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, info_.seq_no_, server_list_.size());
    }
  }
}
