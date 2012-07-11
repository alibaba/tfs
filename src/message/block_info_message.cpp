/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_info_message.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "block_info_message.h"

namespace tfs
{
  namespace message
  {
    int SdbmStat::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &startup_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &fetch_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &miss_fetch_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &store_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &delete_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &overflow_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &page_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, &item_count_);
      }
      return iret;
    }
    int SdbmStat::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, startup_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, fetch_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, miss_fetch_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, store_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, delete_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, overflow_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, page_count_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, item_count_);
      }
      return iret;
    }
    int64_t SdbmStat::length() const
    {
      return common::INT_SIZE * 8;
    }

    GetBlockInfoMessage::GetBlockInfoMessage(const int32_t mode) :
      block_id_(0), mode_(mode)
    {
      _packetHeader._pcode = common::GET_BLOCK_INFO_MESSAGE;
    }

    GetBlockInfoMessage::~GetBlockInfoMessage()
    {
    }

    int GetBlockInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&mode_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(fail_server_);
      }
      return iret;
    }

    int64_t GetBlockInfoMessage::length() const
    {
      return common::INT_SIZE * 2 + common::Serialization::get_vint64_length(fail_server_);
    }

    int GetBlockInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(mode_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(block_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint64(fail_server_);
      }
      return iret;
    }

    SetBlockInfoMessage::SetBlockInfoMessage() :
      block_id_(0), version_(0), lease_id_(common::INVALID_LEASE_ID)
    {
      _packetHeader._pcode = common::SET_BLOCK_INFO_MESSAGE;
      ds_.clear();
    }

    SetBlockInfoMessage::~SetBlockInfoMessage()
    {
    }

    //block_id, server_count, server_id1, server_id2, ..., filename_len, filename
    int SetBlockInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

    int64_t SetBlockInfoMessage::length() const
    {
      int64_t len = common::INT_SIZE + common::Serialization::get_vint64_length(ds_);
      if (has_lease()
        && !ds_.empty())
      {
        len += common::INT64_SIZE * 3;
      }
      return len;
    }

    int SetBlockInfoMessage::serialize(common::Stream& output)  const
    {
      if (has_lease()
        && !ds_.empty())
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_id_));
      }
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint64(ds_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        // reparse, avoid push verion&lease again when clone twice;
        common::BasePacket::parse_special_ds(ds_, version_, lease_id_);
      }
      return iret;
    }

    void SetBlockInfoMessage::set(const uint32_t block_id, const int32_t version, const uint32_t lease_id)
    {
      block_id_ = block_id;
      if (lease_id != common::INVALID_LEASE_ID)
      {
        version_ = version;
        lease_id_ = lease_id;
      }
    }

    BatchGetBlockInfoMessage::BatchGetBlockInfoMessage(int32_t mode) :
      mode_(mode), block_count_(0)
    {
      _packetHeader._pcode = common::BATCH_GET_BLOCK_INFO_MESSAGE;
      block_ids_.clear();
    }

    BatchGetBlockInfoMessage::~BatchGetBlockInfoMessage()
    {
    }

    int BatchGetBlockInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&mode_);
      if (common::TFS_SUCCESS == iret)
      {
        if (mode_ & common::T_READ)
        {
          iret = input.get_vint32(block_ids_);
        }
        else
        {
          iret = input.get_int32(&block_count_);
        }
      }
      return iret;
    }

    int64_t BatchGetBlockInfoMessage::length() const
    {
      int64_t len = common::INT_SIZE;
      return (mode_ & common::T_READ) ? len + common::Serialization::get_vint32_length(block_ids_) : len + common::INT_SIZE;
    }

    int BatchGetBlockInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(mode_);
      if (common::TFS_SUCCESS == iret)
      {
        if (mode_ & common::T_READ)
        {
          iret = output.set_vint32(block_ids_);
        }
        else
        {
          iret = output.set_int32(block_count_);
        }
      }
      return iret;
    }

    BatchSetBlockInfoMessage::BatchSetBlockInfoMessage()
    {
      _packetHeader._pcode = common::BATCH_SET_BLOCK_INFO_MESSAGE;
    }

    BatchSetBlockInfoMessage::~BatchSetBlockInfoMessage()
    {
    }

    // count, blockid, server_count, server_id1, server_id2, ..., blockid, server_count, server_id1 ...
    int BatchSetBlockInfoMessage::deserialize(common::Stream& input)
    {
      int32_t count = 0;
      int32_t iret = input.get_int32(&count);
      if (common::TFS_SUCCESS == iret)
      {
        uint32_t block_id = 0;
        for (int32_t i = 0; i < count; ++i)
        {
          common::BlockInfoSeg block_info;
          iret = input.get_int32(reinterpret_cast<int32_t*>(&block_id));
          if (common::TFS_SUCCESS != iret)
              break;
          iret = input.get_vint64(block_info.ds_);
          if (common::TFS_SUCCESS != iret)
              break;
          common::BasePacket::parse_special_ds(block_info.ds_, block_info.version_, block_info.lease_id_);
          block_infos_[block_id] = block_info;
        }
      }
      return iret;
    }

    int64_t BatchSetBlockInfoMessage::length() const
    {
      int64_t len = common::INT_SIZE * block_infos_.size();
      // just test first has lease, then all has lease, maybe add mode test
      if (!block_infos_.empty())
      {
        std::map<uint32_t, common::BlockInfoSeg>::const_iterator it = block_infos_.begin();
        for (; it != block_infos_.end(); it++)
        {
          len += common::Serialization::get_vint64_length(it->second.ds_);
        }
        if (block_infos_.begin()->second.has_lease())
        {
          // has_lease + lease + version
          len += common::INT64_SIZE * 3 * block_infos_.size();
        }
      }
      return len;
    }

    int BatchSetBlockInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(block_infos_.size());
      if (common::TFS_SUCCESS == iret)
      {
        std::map<uint32_t, common::BlockInfoSeg>::const_iterator it = block_infos_.begin();
        common::BlockInfoSeg* block_info = NULL;
        for (; it != block_infos_.end(); it++)
        {
          block_info = const_cast< common::BlockInfoSeg*>(&it->second);
          if (block_info->has_lease()
              && !block_info->ds_.empty())
          {
            block_info->ds_.push_back(ULONG_LONG_MAX);
            block_info->ds_.push_back(static_cast<uint64_t> (block_info->version_));
            block_info->ds_.push_back(static_cast<uint64_t> (block_info->lease_id_));
          }
          //block id
          iret = output.set_int32(it->first);
          if (common::TFS_SUCCESS != iret)
            break;
          // dataserver list
          iret = output.set_vint64(block_info->ds_);
          if (common::TFS_SUCCESS != iret)
            break;
          // reparse, avoid push verion&lease again when clone twice;
          common::BasePacket::parse_special_ds(block_info->ds_, block_info->version_, block_info->lease_id_);
        }
      }
      return iret;
    }

    void BatchSetBlockInfoMessage::set_read_block_ds(const uint32_t block_id, common::VUINT64& ds)
    {
        block_infos_[block_id] = common::BlockInfoSeg(ds);
    }

    void BatchSetBlockInfoMessage::set_write_block_ds(const uint32_t block_id, common::VUINT64& ds,
                                                      const int32_t version, const int32_t lease)
    {
        block_infos_[block_id] = common::BlockInfoSeg(ds, lease, version);
    }

    CarryBlockMessage::CarryBlockMessage()
    {
      _packetHeader._pcode = common::CARRY_BLOCK_MESSAGE;
      expire_blocks_.clear();
      remove_blocks_.clear();
      new_blocks_.clear();
    }

    CarryBlockMessage::~CarryBlockMessage()
    {
    }

    int CarryBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_vint32(expire_blocks_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint32(remove_blocks_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint32(new_blocks_);
      }
      return iret;
    }

    int64_t CarryBlockMessage::length() const
    {
      return common::Serialization::get_vint32_length(expire_blocks_)
              + common::Serialization::get_vint32_length(remove_blocks_)
              + common::Serialization::get_vint32_length(new_blocks_);
    }

    int CarryBlockMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_vint32(expire_blocks_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint32(remove_blocks_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint32(new_blocks_);
      }
      return iret;
    }

    void CarryBlockMessage::add_expire_id(const uint32_t block_id)
    {
      expire_blocks_.push_back(block_id);
    }
    void CarryBlockMessage::add_new_id(const uint32_t block_id)
    {
      new_blocks_.push_back(block_id);
    }
    void CarryBlockMessage::add_remove_id(const uint32_t block_id)
    {
      remove_blocks_.push_back(block_id);
    }

    NewBlockMessage::NewBlockMessage()
    {
      _packetHeader._pcode = common::NEW_BLOCK_MESSAGE;
      new_blocks_.clear();
    }

    NewBlockMessage::~NewBlockMessage()
    {
    }

    int NewBlockMessage::deserialize(common::Stream& input)
    {
      return input.get_vint32(new_blocks_);
    }

    int64_t NewBlockMessage::length() const
    {
      return  common::Serialization::get_vint32_length(new_blocks_);
    }

    int NewBlockMessage::serialize(common::Stream& output)  const
    {
      return output.set_vint32(new_blocks_);
    }

    void NewBlockMessage::add_new_id(const uint32_t block_id)
    {
      new_blocks_.push_back(block_id);
    }

    RemoveBlockMessage::RemoveBlockMessage():
      id_(0),
      response_flag_(common::REMOVE_BLOCK_RESPONSE_FLAG_NO)
    {
      _packetHeader._pcode = common::REMOVE_BLOCK_MESSAGE;
    }

    RemoveBlockMessage::~RemoveBlockMessage()
    {
    }

    int RemoveBlockMessage::deserialize(common::Stream& input)
    {
      int32_t reserve = 0;
      int32_t iret = input.get_int32(&reserve);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&id_));
      }
      if (common::TFS_SUCCESS == iret
         && input.get_data_length() > 0)
      {
        iret = input.get_int8(&response_flag_);
      }
      if (common::TFS_SUCCESS == iret
         && input.get_data_length() > 0)
      {
        iret = input.get_int64(&seqno_);
      }
      return iret;
    }

    int64_t RemoveBlockMessage::length() const
    {
      return  common::INT_SIZE * 2 + common::INT64_SIZE + common::INT8_SIZE;
    }

    int RemoveBlockMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(0);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        output.set_int8(response_flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(seqno_);
      }
      return iret;
    }

    RemoveBlockResponseMessage::RemoveBlockResponseMessage():
      block_id_(0)
    {
      _packetHeader._pcode = common::REMOVE_BLOCK_RESPONSE_MESSAGE;
    }

    RemoveBlockResponseMessage::~RemoveBlockResponseMessage()
    {

    }

    int RemoveBlockResponseMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32(reinterpret_cast<int32_t*>(&block_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&seqno_);
      }
      return ret;
    }

    int64_t RemoveBlockResponseMessage::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE;
    }

    int RemoveBlockResponseMessage::serialize(common::Stream& output)  const
    {
      int32_t ret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == ret)
      {
        output.set_int64(seqno_);
      }
      return ret;
    }

    ListBlockMessage::ListBlockMessage() :
      type_(0)
    {
      _packetHeader._pcode = common::LIST_BLOCK_MESSAGE;
    }

    ListBlockMessage::~ListBlockMessage()
    {

    }

    int ListBlockMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&type_);
    }

    int64_t ListBlockMessage::length() const
    {
      return common::INT_SIZE;
    }

    int ListBlockMessage::serialize(common::Stream& output)  const
    {
      return output.set_int32(type_);
    }

    RespListBlockMessage::RespListBlockMessage() :
      status_type_(0), need_free_(0)
    {
      _packetHeader._pcode = common::RESP_LIST_BLOCK_MESSAGE;
      blocks_.clear();
    }

    RespListBlockMessage::~RespListBlockMessage()
    {
    }

    int RespListBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&status_type_);
      if (common::TFS_SUCCESS == iret)
      {
        need_free_ = 1;
        if (status_type_ & common::LB_BLOCK)//blocks
        {
          iret = input.get_vint32(blocks_);
        }

        if (common::TFS_SUCCESS == iret
          && status_type_ & common::LB_PAIRS)//pairs
        {
          int32_t size = 0;
          iret = input.get_int32(&size);
          if (common::TFS_SUCCESS == iret)
          {
            std::vector<uint32_t> tmp;
            uint32_t block_id = 0;
            for (int32_t i = 0; i < size; ++i)
            {
              iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id));
              if (common::TFS_SUCCESS == iret)
              {
                tmp.clear();
                iret = input.get_vint32(tmp);
                if (common::TFS_SUCCESS == iret)
                {
                  block_pairs_.insert(std::map<uint32_t, std::vector<uint32_t> >::value_type(block_id, tmp));
                }
                else
                {
                  break;
                }
              }
              else
              {
                break;
              }
            }
          }
        }

        if (common::TFS_SUCCESS == iret
          && status_type_ & common::LB_INFOS)
        {
          int32_t size = 0;
          iret = input.get_int32(&size);
          if (common::TFS_SUCCESS == iret)
          {
            int64_t pos = 0;
            common::BlockInfo info;
            for (int32_t i = 0; i < size; ++i)
            {
              pos = 0;
              iret = info.deserialize(input.get_data(), input.get_data_length(), pos);
              if (common::TFS_SUCCESS == iret)
              {
                input.drain(info.length());
                block_infos_.insert(std::map<uint32_t, common::BlockInfo>::value_type(info.block_id_, info));
              }
              else
              {
                break;
              }
            }
          }
        }
      }
      return iret;
    }

    int64_t RespListBlockMessage::length() const
    {
      int64_t len = common::INT_SIZE;
      // m_Blocks
      if (status_type_ & common::LB_BLOCK)
      {
        len += common::Serialization::get_vint32_length(blocks_);
      }
      // m_BlockPairs
      if (status_type_ & common::LB_PAIRS)
      {
        len += common::INT_SIZE;
        std::map<uint32_t, std::vector<uint32_t> >::const_iterator mit = block_pairs_.begin();
        for (; mit != block_pairs_.end(); mit++)
        {
          len += common::INT_SIZE;
          len += common::Serialization::get_vint32_length(mit->second);
        }
      }
      // m_BlockInfos
      if (status_type_ & common::LB_INFOS)
      {
        len += common::INT_SIZE;
        common::BlockInfo info;
        len += block_infos_.size() * info.length();
      }
      return len;
    }

    int RespListBlockMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(status_type_);
      if (common::TFS_SUCCESS == iret)
      {
        if (status_type_ & common::LB_BLOCK)
        {
          iret = output.set_vint32(blocks_);
        }

        if (common::TFS_SUCCESS == iret
          && status_type_ & common::LB_PAIRS)
        {
          iret = output.set_int32(block_pairs_.size());
          if (common::TFS_SUCCESS == iret)
          {
            std::map<uint32_t, std::vector<uint32_t> >:: const_iterator iter =
              block_pairs_.begin();
            for (; iter != block_pairs_.end(); ++iter)
            {
              iret = output.set_int32(iter->first);
              if (common::TFS_SUCCESS != iret)
                break;
              iret = output.set_vint32(iter->second);
              if (common::TFS_SUCCESS != iret)
                break;
            }
          }
        }

        if (common::TFS_SUCCESS == iret
           && status_type_ & common::LB_INFOS)
        {
          iret = output.set_int32(block_infos_.size());
          if (common::TFS_SUCCESS == iret)
          {
            std::map<uint32_t, common::BlockInfo>::const_iterator iter =
              block_infos_.begin();
            for (; iter != block_infos_.end(); ++iter)
            {
              int64_t pos = 0;
              iret = const_cast<common::BlockInfo*>(&(iter->second))->serialize(output.get_free(), output.get_free_length(), pos);
              if (common::TFS_SUCCESS == iret)
                output.pour(iter->second.length());
              else
                break;
            }
          }
        }
      }
      return iret;
    }

    void RespListBlockMessage::add_block_id(const uint32_t block_id)
    {
      blocks_.push_back(block_id);
    }

    UpdateBlockInfoMessage::UpdateBlockInfoMessage() :
      block_id_(0), server_id_(0), repair_(0)
    {
      _packetHeader._pcode = common::UPDATE_BLOCK_INFO_MESSAGE;
      memset(&db_stat_, 0, sizeof(db_stat_));
    }

    UpdateBlockInfoMessage::~UpdateBlockInfoMessage()
    {

    }

    int UpdateBlockInfoMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*> (&server_id_));
      }
      int32_t have_block = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&have_block);
      }
      if (common::TFS_SUCCESS == iret
        && have_block)
      {
        int64_t pos = 0;
        iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(block_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&repair_);
      }
      int32_t have_sdbm = 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&have_sdbm);
      }
      if (common::TFS_SUCCESS == iret
          && have_sdbm > 0)
      {
        int64_t pos = 0;
        iret = db_stat_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(db_stat_.length());
        }
      }
      return iret;
    }

    int64_t UpdateBlockInfoMessage::length() const
    {
      int64_t len = common::INT64_SIZE + common::INT_SIZE * 4;
      if (block_info_ .block_id_ > 0)
      {
        len += block_info_.length();
      }
      if (db_stat_.item_count_ > 0)
      {
        len += db_stat_.length();
      }
      return len;
    }

    int UpdateBlockInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(server_id_);
      }
      int32_t have_block = block_info_.block_id_ > 0 ? 1 : 0;
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(have_block);
      }
      int64_t pos = 0;
      if (common::TFS_SUCCESS == iret
        && have_block)
      {
        iret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(block_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(repair_);
      }
      int32_t have_sdbm = db_stat_.item_count_ > 0 ? 0 : 1;
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(have_sdbm);
      }
      if (common::TFS_SUCCESS == iret
        && have_sdbm)
      {
        pos = 0;
        iret = db_stat_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(db_stat_.length());
        }
      }
      return iret;
    }

    ResetBlockVersionMessage::ResetBlockVersionMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = common::RESET_BLOCK_VERSION_MESSAGE;
    }

    ResetBlockVersionMessage::~ResetBlockVersionMessage()
    {
    }

    int ResetBlockVersionMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
    }

    int64_t ResetBlockVersionMessage::length() const
    {
      return common::INT_SIZE;
    }

    int ResetBlockVersionMessage::serialize(common::Stream& output)  const
    {
      return output.set_int32(block_id_);
    }

    BlockFileInfoMessage::BlockFileInfoMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = common::BLOCK_FILE_INFO_MESSAGE;
      fileinfo_list_.clear();
    }

    BlockFileInfoMessage::~BlockFileInfoMessage()
    {
    }

    int BlockFileInfoMessage::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::FileInfo info;
        for (int32_t i = 0; i < size; ++i)
        {
          int64_t pos = 0;
          iret = info.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(info.length());
            fileinfo_list_.push_back(info);
          }
          else
          {
            break;
          }
        }
      }
      return iret;
    }

    int64_t BlockFileInfoMessage::length() const
    {
      common::FileInfo info;
      return common::INT_SIZE * 2 + fileinfo_list_.size() * info.length();
    }

    int BlockFileInfoMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(fileinfo_list_.size());
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::FILE_INFO_LIST::const_iterator iter = fileinfo_list_.begin();
        for (; iter != fileinfo_list_.end(); ++iter)
        {
          int64_t pos = 0;
          iret = (*iter).serialize(output.get_free(), output.get_free_length(), pos);
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
      return iret;
    }

    BlockRawMetaMessage::BlockRawMetaMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = common::BLOCK_RAW_META_MESSAGE;
      raw_meta_list_.clear();
    }

    BlockRawMetaMessage::~BlockRawMetaMessage()
    {
    }

    int BlockRawMetaMessage::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&size);
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::RawMeta raw;
        for (int32_t i = 0; i < size; ++i)
        {
          int64_t pos = 0;
          iret = raw.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            input.drain(raw.length());
            raw_meta_list_.push_back(raw);
          }
          else
          {
            break;
          }
        }
      }
      return iret;
    }

    int64_t BlockRawMetaMessage::length() const
    {
      common::RawMeta raw;
      return  common::INT_SIZE * 2 + raw_meta_list_.size() * raw.length();
    }

    int BlockRawMetaMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(raw_meta_list_.size());
      }
      if (common::TFS_SUCCESS == iret)
      {
        common::RawMetaVec::const_iterator iter = raw_meta_list_.begin();
        for (; iter != raw_meta_list_.end(); ++iter)
        {
          int64_t pos = 0;
          iret = const_cast<common::RawMeta*>(&(*iter))->serialize(output.get_free(), output.get_free_length(), pos);
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
      return iret;
    }

    BlockWriteCompleteMessage::BlockWriteCompleteMessage() :
      server_id_(0), write_complete_status_(common::WRITE_COMPLETE_STATUS_NO), unlink_flag_(common::UNLINK_FLAG_NO)
    {
      _packetHeader._pcode = common::BLOCK_WRITE_COMPLETE_MESSAGE;
    }

    BlockWriteCompleteMessage::~BlockWriteCompleteMessage()
    {
    }

    int BlockWriteCompleteMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int64( reinterpret_cast<int64_t*> (&server_id_));
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(block_info_.length());
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*> (&lease_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*> (&write_complete_status_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*> (&unlink_flag_));
      }
      return iret;
    }

    int64_t BlockWriteCompleteMessage::length() const
    {
      return common::INT64_SIZE + block_info_.length() + common::INT_SIZE * 3;
    }

    int BlockWriteCompleteMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = block_info_.block_id_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(server_id_);
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
        iret = output.set_int32(lease_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(write_complete_status_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(unlink_flag_);
      }
      return iret;
    }

    ListBitMapMessage::ListBitMapMessage() :
      type_(0)
    {
      _packetHeader._pcode = common::LIST_BITMAP_MESSAGE;
    }

    ListBitMapMessage::~ListBitMapMessage()
    {
    }

    int ListBitMapMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&type_);
    }

    int64_t ListBitMapMessage::length() const
    {
      return common::INT_SIZE;
    }

    int ListBitMapMessage::serialize(common::Stream& output)  const
    {
      return output.set_int32(type_);
    }

    RespListBitMapMessage::RespListBitMapMessage() :
      ubitmap_len_(0), uuse_len_(0), data_(NULL), alloc_(false)
    {
      _packetHeader._pcode = common::RESP_LIST_BITMAP_MESSAGE;
    }

    RespListBitMapMessage::~RespListBitMapMessage()
    {
      if ((NULL != data_ ) && (alloc_ ))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* RespListBitMapMessage::alloc_data(const int32_t len)
    {
      if (len <= 0)
      {
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      ubitmap_len_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    int RespListBitMapMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&uuse_len_));
      if (common::TFS_SUCCESS == iret)
      {
        input.get_int32(reinterpret_cast<int32_t*> (&ubitmap_len_));
      }
      if (common::TFS_SUCCESS == iret
        && ubitmap_len_ > 0)
      {
        char* data = alloc_data(ubitmap_len_);
        iret = NULL != data ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          iret = input.get_bytes(data, ubitmap_len_);
        }
      }
      return iret;
    }

    int64_t RespListBitMapMessage::length() const
    {
      int64_t len = common::INT_SIZE * 2;
      if (ubitmap_len_ > 0)
      {
        len += ubitmap_len_;
      }
      return len;
    }

    int RespListBitMapMessage::serialize(common::Stream& output)  const
    {
      int32_t iret = output.set_int32(uuse_len_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(ubitmap_len_);
      }
      if (common::TFS_SUCCESS == iret
          && ubitmap_len_ > 0)
      {
        iret = output.set_bytes(data_, ubitmap_len_);
      }
      return iret;
    }
  }
}
