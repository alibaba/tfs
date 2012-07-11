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
#ifndef TFS_MESSAGE_RTS_UPDATE_TABLE_MESSAGE_H_
#define TFS_MESSAGE_RTS_UPDATE_TABLE_MESSAGE_H_

#include "common/rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class UpdateTableMessage: public common::BasePacket 
    {
      public:
        UpdateTableMessage();
        virtual ~UpdateTableMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        char* alloc(const int64_t length);
        inline char* get_table() { return tables_;}
        inline int64_t get_table_length() { return length_;}
        inline int64_t get_version() const { return version_;}
        inline void set_version(const int64_t version){ version_ = version;}
        inline int8_t get_phase(void) const { return phase_;}
        inline void set_phase(const int8_t phase) { phase_ = phase;}
      private:
        char* tables_;
        int64_t length_;
        int64_t version_;
        int8_t  phase_;
        bool alloc_;
    };

    class UpdateTableResponseMessage: public common::BasePacket 
    {
      public:
        UpdateTableResponseMessage();
        virtual ~UpdateTableResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int64_t get_version() const { return version_;}
        inline void set_version(const int64_t version){ version_ = version;}
        inline int8_t get_phase(void) const { return phase_;}
        inline void set_phase(const int8_t phase) { phase_ = phase;}
        inline int8_t get_status() const { return status_;}
        inline void set_status(const int8_t status) { status_ = status;}
      private:
        int64_t version_;
        int8_t  phase_;
        int8_t  status_;
    };
  }/** message **/
}/** tfs **/
#endif
