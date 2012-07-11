/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dump_plan_message.cpp 1990 2010-12-24 08:41:05Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "dump_plan_message.h"
namespace tfs
{
  namespace message
  {
    DumpPlanMessage::DumpPlanMessage():
      reserve_(0)
    {
      _packetHeader._pcode= common::DUMP_PLAN_MESSAGE;
    }

    DumpPlanMessage::~DumpPlanMessage()
    {

    }

    int DumpPlanMessage::deserialize(common::Stream& input)
    {
      return input.get_int8(&reserve_);
    }

    int64_t DumpPlanMessage::length() const
    {
      return common::INT8_SIZE;
    }

    int DumpPlanMessage::serialize(common::Stream& output) const
    {
      return output.set_int8(reserve_);
    }

    DumpPlanResponseMessage::DumpPlanResponseMessage()
    {
      _packetHeader._pcode= common::DUMP_PLAN_RESPONSE_MESSAGE;
    }

    DumpPlanResponseMessage::~DumpPlanResponseMessage()
    {
    }

    int DumpPlanResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = common::TFS_SUCCESS;
      if (input.get_data_length() > 0)
      {
        data_.writeBytes(input.get_data(), input.get_data_length());
      }
      return iret;
    }

    int64_t DumpPlanResponseMessage::length() const
    {
      return data_.getDataLen();
    }

    int DumpPlanResponseMessage::serialize(common::Stream& output) const
    {
      int32_t iret = common::TFS_SUCCESS;
      if (data_.getDataLen() > 0)
      {
        iret = output.set_bytes(data_.getData(), data_.getDataLen());
      }
      return iret;
    }
  }
}
