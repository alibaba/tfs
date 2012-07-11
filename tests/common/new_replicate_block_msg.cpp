/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: replicate_block_message.cpp 186 2011-04-07 06:32:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "new_replicate_block_msg.h"

using namespace tfs::common;
namespace tfs
{
  namespace tests 
  {
    NewReplicateBlockMessage::NewReplicateBlockMessage() :
      command_(0), expire_(0)
    {
      _packetHeader._pcode = REPLICATE_BLOCK_MESSAGE;
      memset(&repl_block_, 0, sizeof(ReplBlock));
    }

    NewReplicateBlockMessage::~NewReplicateBlockMessage()
    {

    }

    int NewReplicateBlockMessage::serialize(common::Stream& output)
    {
      int32_t iret = output.set_int32(command_);
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(expire_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = repl_block_.serialize(output);
      }
      return iret;
    }

    int NewReplicateBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&command_);
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&expire_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = repl_block_.deserialize(input);
      }
      return iret;
    }

    int64_t NewReplicateBlockMessage::length() const
    {
      return INT_SIZE * 2 + repl_block_.length();
    }

    tbnet::Packet* NewReplicateBlockMessage::create(const int32_t type)
    {
      NewReplicateBlockMessage* packet = new NewReplicateBlockMessage();
      packet->setPCode(type);
      return packet;
    }
  }
}
