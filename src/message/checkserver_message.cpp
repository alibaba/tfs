/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_message.cpp 706 2012-04-12 14:24:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include "checkserver_message.h"
namespace tfs
{
  namespace message
  {
    using namespace common;

    int CheckBlockRequestMessage::serialize(common::Stream& output) const
    {
      int ret = TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(check_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(check_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(last_check_time_);
      }

      return ret;
    }

    int CheckBlockRequestMessage::deserialize(common::Stream& input)
    {
      int ret = TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32((int32_t*)&check_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32((int32_t*)&block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32((int32_t*)&check_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32((int32_t*)&last_check_time_);
      }

      return ret;
    }

    int64_t CheckBlockRequestMessage::length() const
    {
      return 3 * INT_SIZE;
    }

    int CheckBlockResponseMessage::serialize(common::Stream& output) const
    {
      int ret = output.set_int32(check_result_.size());
      if (TFS_SUCCESS == ret)
      {
        for (uint32_t i = 0; i < check_result_.size(); i++)
        {
          int64_t pos = 0;
          ret = check_result_[i].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(check_result_[i].length());
          }
          else
          {
            break;
          }
        }
      }
      return ret;
   }

    int CheckBlockResponseMessage::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int ret = input.get_int32(&size);
      if (TFS_SUCCESS == ret)
      {
        CheckBlockInfo cbi;
        for (int i = 0; i < size; i++)
        {
          int64_t pos = 0;
          ret = cbi.deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            check_result_.push_back(cbi);
            input.drain(cbi.length());
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    int64_t CheckBlockResponseMessage::length() const
    {
      CheckBlockInfo cbi;
      return INT_SIZE + check_result_.size() * cbi.length();
    }

 }/** end namespace message **/
}/** end namespace tfs **/
