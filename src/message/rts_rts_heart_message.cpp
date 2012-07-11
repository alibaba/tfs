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
#include "common/rts_define.h"

#include "rts_rts_heart_message.h"

namespace tfs
{
  namespace message
  {
    RtsRsHeartMessage::RtsRsHeartMessage():
      keepalive_type_(common::RTS_RS_KEEPALIVE_TYPE_LOGIN)
    {
      _packetHeader._pcode = common::REQ_RT_RS_KEEPALIVE_MESSAGE;
    }

    RtsRsHeartMessage::~RtsRsHeartMessage()
    {

    }

    int RtsRsHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = server_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(server_.length());
        iret = input.get_int8(&keepalive_type_);
      }
      return iret;
    }

    int RtsRsHeartMessage::serialize(common::Stream& output) const 
    {
      int64_t pos = 0;
      int32_t iret = server_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(server_.length());
        iret = output.set_int8(keepalive_type_);
      }
      return iret;
    }

    int64_t RtsRsHeartMessage::length() const
    {
      return server_.length() + common::INT8_SIZE;
    }

    RtsRsHeartResponseMessage::RtsRsHeartResponseMessage():
      active_table_version_(common::INVALID_TABLE_VERSION),
      lease_expired_time_(common::RTS_RS_LEASE_EXPIRED_TIME_DEFAULT),
      ret_value_(common::TFS_ERROR),
      renew_lease_interval_time_(common::RTS_RS_RENEW_LEASE_INTERVAL_TIME_DEFAULT)
    {
      _packetHeader._pcode = common::RSP_RT_RS_KEEPALIVE_MESSAGE;
    }

    RtsRsHeartResponseMessage::~RtsRsHeartResponseMessage()
    {

    }

    int RtsRsHeartResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int64(&active_table_version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&lease_expired_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&ret_value_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&renew_lease_interval_time_);
      }
      return iret;
    }

    int RtsRsHeartResponseMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int64(active_table_version_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(lease_expired_time_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(ret_value_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(renew_lease_interval_time_);
      }
      return iret;
    }

    int64_t RtsRsHeartResponseMessage::length() const
    {
      return common::INT64_SIZE * 2 + common::INT_SIZE * 2;
    }
  }/** message **/
}/** tfs **/
