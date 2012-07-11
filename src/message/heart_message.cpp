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
#include "common/serialization.h"
#include "common/stream.h"
#include "common/parameter.h"
#include "heart_message.h"

namespace tfs
{
  namespace message
  {
    RespHeartMessage::RespHeartMessage() :
      status_(0),
      heart_interval_(common::DEFAULT_HEART_INTERVAL)
    {
      _packetHeader._pcode = common::RESP_HEART_MESSAGE;
      expire_blocks_.clear();
    }

    RespHeartMessage::~RespHeartMessage()
    {

    }

    int RespHeartMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&status_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&heart_interval_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint32(expire_blocks_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint32(new_blocks_);
      }
      return iret;
    }

    int64_t RespHeartMessage::length() const
    {
      return common::INT_SIZE * 2 +
                common::Serialization::get_vint32_length(expire_blocks_) +
                  common::Serialization::get_vint32_length(new_blocks_);
    }

    int RespHeartMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(status_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(heart_interval_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint32(expire_blocks_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint32(new_blocks_);
      }
      return iret;
    }

    int NSIdentityNetPacket::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::set_int64(data, data_len, pos, ip_port_);
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::set_int8(data, data_len, pos, role_);
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::set_int8(data, data_len, pos, status_);
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::set_int8(data, data_len, pos, flags_);
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::set_int8(data, data_len, pos, force_);
      }
      return iret;
    }

    int NSIdentityNetPacket::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&ip_port_));
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&role_));
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&status_));
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&flags_));
      }
      if (common::TFS_SUCCESS  == iret)
      {
        iret = common::Serialization::get_int8(data, data_len, pos, reinterpret_cast<int8_t*>(&force_));
      }
      return iret;
    }

    int64_t NSIdentityNetPacket::length() const
    {
      return common::INT64_SIZE + common::INT8_SIZE * 4;
    }

    MasterAndSlaveHeartMessage::MasterAndSlaveHeartMessage():
      lease_id_(common::INVALID_LEASE_ID),
      keepalive_type_(0)
    {
      _packetHeader._pcode = common::MASTER_AND_SLAVE_HEART_MESSAGE;
      memset(&ns_identity_, 0, sizeof(ns_identity_));
    }

    MasterAndSlaveHeartMessage::~MasterAndSlaveHeartMessage()
    {

    }

    int MasterAndSlaveHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = ns_identity_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(ns_identity_.length());
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (input.get_data_length() > 0)
          iret = input.get_int64(&lease_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (input.get_data_length() > 0)
          iret = input.get_int8(&keepalive_type_);
      }
      return iret;
    }

    int MasterAndSlaveHeartMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = ns_identity_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(ns_identity_.length());
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(lease_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int8(keepalive_type_);
      }
      return iret;
    }

    int64_t MasterAndSlaveHeartMessage::length() const
    {
      return ns_identity_.length() + common::INT64_SIZE + common::INT8_SIZE;
    }

    MasterAndSlaveHeartResponseMessage::MasterAndSlaveHeartResponseMessage():
      lease_id_(common::INVALID_LEASE_ID),
      lease_expired_time_(common::SYSPARAM_NAMESERVER.heart_interval_),
      renew_lease_interval_time_(common::SYSPARAM_NAMESERVER.heart_interval_)
    {
      _packetHeader._pcode = common::MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE;
      ::memset(&ns_identity_, 0, sizeof(ns_identity_));
    }

    MasterAndSlaveHeartResponseMessage::~MasterAndSlaveHeartResponseMessage()
    {

    }

    int MasterAndSlaveHeartResponseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = ns_identity_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(ns_identity_.length());
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&lease_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&lease_expired_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&renew_lease_interval_time_);
      }
      return iret;
    }

    int MasterAndSlaveHeartResponseMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = ns_identity_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(ns_identity_.length());
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(lease_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(lease_expired_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(renew_lease_interval_time_);
      }
      return iret;
    }

    int64_t MasterAndSlaveHeartResponseMessage::length() const
    {
      return ns_identity_.length() + common::INT64_SIZE + common::INT_SIZE * 2;
    }

    HeartBeatAndNSHeartMessage::HeartBeatAndNSHeartMessage() :
      flags_(0)
    {
      _packetHeader._pcode = common::HEARTBEAT_AND_NS_HEART_MESSAGE;
    }

    HeartBeatAndNSHeartMessage::~HeartBeatAndNSHeartMessage()
    {

    }

    int HeartBeatAndNSHeartMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&flags_);
    }

    int HeartBeatAndNSHeartMessage::serialize(common::Stream& output) const
    {
      return output.set_int32(flags_);
    }

    int64_t HeartBeatAndNSHeartMessage::length() const
    {
      return common::INT_SIZE;
    }

    /*OwnerCheckMessage::OwnerCheckMessage() :
      start_time_(0)
    {
      _packetHeader._pcode = common::OWNER_CHECK_MESSAGE;
    }

    OwnerCheckMessage::~OwnerCheckMessage()
    {

    }*/
  }
}
