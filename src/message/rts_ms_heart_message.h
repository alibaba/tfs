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
#ifndef TFS_MESSAGE_RTS_MS_HEARTMESSAGE_H_
#define TFS_MESSAGE_RTS_MS_HEARTMESSAGE_H_

#include "common/rts_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class RtsMsHeartMessage: public common::BasePacket 
    {
      public:
        RtsMsHeartMessage();
        virtual ~RtsMsHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline common::MetaServer& get_ms(void) { return server_;}
        inline void set_ms(const common::MetaServer& server){ memcpy(&server_, &server, sizeof(common::MetaServer));}
        inline int8_t get_type(void) const { return keepalive_type_;}
        inline void set_type(const int8_t type) { keepalive_type_ = type;}
      private:
        common::MetaServer server_;
        int8_t keepalive_type_;
    };

    class RtsMsHeartResponseMessage: public common::BasePacket 
    {
      public:
        RtsMsHeartResponseMessage();
        virtual ~RtsMsHeartResponseMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_active_table_version(const int64_t version) { active_table_version_ = version;}
        inline int64_t get_active_table_version(void) const { return active_table_version_;}
        inline void set_lease_expired_time(const int64_t time) { lease_expired_time_ = time;}
        inline int64_t get_lease_expired_time(void) const { return lease_expired_time_;}
        inline void set_ret_value(const int32_t value) { ret_value_ = value;}
        inline int32_t get_ret_value(void) const { return ret_value_;}
        inline void set_renew_lease_interval_time(const int32_t time) { renew_lease_interval_time_ = time;}
        inline int32_t get_renew_lease_interval_time(void) const { return renew_lease_interval_time_;}
      private:
        int64_t active_table_version_;
        int64_t lease_expired_time_;
        int32_t ret_value_;
        int32_t renew_lease_interval_time_;
    };
  }
}
#endif
