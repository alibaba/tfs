/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rts_ms_heart_message.h 439 2011-09-05 08:35:08Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_RTS_GET_TABLES_MESSAGE_H_
#define TFS_MESSAGE_RTS_GET_TABLES_MESSAGE_H_ 
#include "common/rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class GetTableFromRtsResponseMessage: public common::BasePacket 
    {
      public:
        GetTableFromRtsResponseMessage();
        virtual ~GetTableFromRtsResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        char* alloc(const int64_t length);
        inline char* get_table() { return tables_;}
        inline int64_t& get_table_length() { return length_;}
        inline int64_t& get_version() { return version_;}
      private:
        char* tables_;
        int64_t length_;
        int64_t version_;
    };

    class GetTableFromRtsMessage: public common::BasePacket 
    {
      public:
        GetTableFromRtsMessage();
        virtual ~GetTableFromRtsMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
      private:
        int8_t  reserve_;
    };
  }/** message **/
}/** tfs **/
#endif
