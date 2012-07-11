/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog_sync_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_OPLOGSYNCMESSAGE_H_
#define TFS_MESSAGE_OPLOGSYNCMESSAGE_H_ 
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class OpLogSyncMessage: public common::BasePacket 
    {
      public:
        OpLogSyncMessage();
        virtual ~OpLogSyncMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_data(const char* data, const int64_t length);
        inline int64_t get_length() const { return length_;}
        inline const char* get_data() const { return data_;}
      private:
        bool alloc_;
        int32_t length_;
        char* data_;
    };

    enum OpLogSyncMsgCompleteFlag
    {
      OPLOG_SYNC_MSG_COMPLETE_YES = 0x00,
      OPLOG_SYNC_MSG_COMPLETE_NO
    };

    class OpLogSyncResponeMessage: public common::BasePacket 
    {
      public:
        OpLogSyncResponeMessage();
        virtual ~OpLogSyncResponeMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline uint8_t get_complete_flag() const { return complete_flag_;}
        inline void set_complete_flag(uint8_t flag = OPLOG_SYNC_MSG_COMPLETE_YES) { complete_flag_ = flag;}
      private:
        uint8_t complete_flag_;
    };
  }
}
#endif
