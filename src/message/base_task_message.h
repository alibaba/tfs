/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_task_message.h 477 2011-06-10 08:15:48Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_BASE_TASK_MESSAGE_H_
#define TFS_MESSAGE_BASE_TASK_MESSAGE_H_
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class BaseTaskMessage: public common::BasePacket
    {
      public:
        BaseTaskMessage(){}
        virtual ~BaseTaskMessage(){}
        virtual int serialize(common::Stream& output) const = 0;
        virtual int deserialize(common::Stream& input) = 0;
        virtual int64_t length() const = 0;
        virtual void dump(void) const {};
        int64_t get_seqno() const { return seqno_;}
        void set_seqno(const int64_t seqno) { seqno_ = seqno;}
      protected:
        int64_t seqno_;
    };
  }
}
#endif
