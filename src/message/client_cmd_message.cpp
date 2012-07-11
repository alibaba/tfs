/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: client_cmd_message.cpp 384 2011-05-31 07:47:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "client_cmd_message.h"
namespace tfs
{
  namespace message
  {
    ClientCmdMessage::ClientCmdMessage()
    {
      _packetHeader._pcode = common::CLIENT_CMD_MESSAGE;
      memset(&info_, 0, sizeof(common::ClientCmdInformation));
    }

    ClientCmdMessage::~ClientCmdMessage()
    {

    }

    int ClientCmdMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.pour(info_.length());
      }
      return iret;
    }

    int64_t ClientCmdMessage::length() const
    {
      return info_.length();
    }

    int ClientCmdMessage::serialize(common::Stream& output) const 
    {
      int64_t pos = 0;
      int32_t iret = info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(info_.length());
      }
      return iret;
    }
  }
}
