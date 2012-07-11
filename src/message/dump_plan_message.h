/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dump_plan_message.h 2135 2011-03-25 06:57:01Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_DUMP_PLAN_MESSAGE_H_
#define TFS_MESSAGE_DUMP_PLAN_MESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class DumpPlanMessage: public common::BasePacket 
    {
      public:
        DumpPlanMessage();
        virtual ~DumpPlanMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
      private:
        int8_t reserve_;
    };

    class DumpPlanResponseMessage: public common::BasePacket 
    {
      public:
        DumpPlanResponseMessage();
        virtual ~DumpPlanResponseMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline tbnet::DataBuffer& get_data()
        {
          return data_;
        }
      private:
        mutable tbnet::DataBuffer data_;
    };
  }
}

#endif
