/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: replicate_block_message.h 186 2011-04-07 06:32:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TESTS_REPLICATE_BLOCK_MSG_H_
#define TFS_TESTS_REPLICATE_BLOCK_MSG_H_
#include "base_packet.h"

namespace tfs
{
  namespace tests 
  {
    class NewReplicateBlockMessage: public common::BasePacket 
    {
      public:
        NewReplicateBlockMessage();
        virtual ~NewReplicateBlockMessage();
        int serialize(common::Stream& output);                                                               
        int deserialize(common::Stream& input);
        int64_t length() const;
        static tbnet::Packet* create(const int32_t type);

        inline void set_command(const int32_t command) { command_ = command;}
        inline int32_t get_command() const { return command_;}
        inline void set_expire(const int32_t expire) {  expire_ = expire; }
        inline int32_t get_expire() const { return expire_; }
        inline common::ReplicateBlockMoveFlag get_move_flag() const
        {
          return static_cast<common::ReplicateBlockMoveFlag>(repl_block_.is_move_);
        }

        inline void set_repl_block(const common::ReplBlock* repl_block)
        {
          memcpy(&repl_block_, repl_block, sizeof(common::ReplBlock));
        }
        inline const common::ReplBlock* get_repl_block() const { return &repl_block_;}

      private:
        int32_t command_;
        int32_t expire_;
        common::ReplBlock repl_block_;
    };
  }
}
#endif
