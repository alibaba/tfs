/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: checkserver_message.h 706 2012-04-12 14:24:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CHECKSERVERMESSAGE_H_
#define TFS_MESSAGE_CHECKSERVERMESSAGE_H_

#include "common/base_packet.h"
#include "common/internal.h"
#include <vector>

namespace tfs
{
  namespace message
  {
    class CheckBlockRequestMessage: public common::BasePacket
    {
      public:
        CheckBlockRequestMessage(): check_flag_(0), block_id_(0),
              check_time_(0), last_check_time_(0)
        {
          _packetHeader._pcode = common::REQ_CHECK_BLOCK_MESSAGE;
        }
        virtual ~CheckBlockRequestMessage()
        {
        }
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_check_flag(const int32_t check_flag)
        {
          check_flag_ = check_flag;
        }

        int32_t get_check_flag()
        {
          return check_flag_;
        }

        void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }

        uint32_t get_block_id()
        {
          return block_id_;
        }

        void set_check_time(const uint32_t check_time)
        {
          check_time_ = check_time;
        }

        uint32_t get_check_time()
        {
          return check_time_;
        }

        void set_last_check_time(const uint32_t last_check_time)
        {
          last_check_time_ = last_check_time;
        }

        uint32_t get_last_check_time()
        {
          return last_check_time_;
        }

      private:
        int32_t check_flag_;  // for scalability
        uint32_t block_id_;
        uint32_t check_time_;
        uint32_t last_check_time_;
    };

    class CheckBlockResponseMessage: public common::BasePacket
    {
      public:
        CheckBlockResponseMessage()
        {
          _packetHeader._pcode = common::RSP_CHECK_BLOCK_MESSAGE;
        }
        virtual ~CheckBlockResponseMessage()
        {
        }
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        common::CheckBlockInfoVec& get_result_ref()
        {
          return check_result_;
        }

      private:
        std::vector<common::CheckBlockInfo> check_result_;
    };

 }/** end namespace message **/
}/** end namespace tfs **/
#endif
