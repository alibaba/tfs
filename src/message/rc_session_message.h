/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rc_session_message.h 378 2011-05-30 07:16:34Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_RCSESSION_MESSAGE_H_
#define TFS_MESSAGE_RCSESSION_MESSAGE_H_
#include "common/base_packet.h"
#include "common/rc_define.h"

namespace tfs
{
  namespace message
  {
    class ReqRcLoginMessage: public common::BasePacket 
    {
    public:
      ReqRcLoginMessage();
      virtual ~ReqRcLoginMessage();

      int set_app_key(const char* app_key);
      void set_app_ip(const int64_t app_ip);

      int serialize(common::Stream& output) const ;
      int deserialize(common::Stream& input);
      int64_t length() const;

      const char* get_app_key() const;
      int64_t get_app_ip() const;
      void dump() const;

    private:
      char app_key_[common::MAX_PATH_LENGTH];
      int64_t app_ip_;
      int32_t length_;
    };

    class RspRcLoginMessage: public common::BasePacket 
    {
    public:
      RspRcLoginMessage();
      virtual ~RspRcLoginMessage();

      int set_session_id(const char* session_id);
      void set_base_info(const common::BaseInfo& base_info);

      int serialize(common::Stream& output) const ;
      int deserialize(common::Stream& input);
      int64_t length() const;

      const char* get_session_id() const;
      const common::BaseInfo& get_base_info() const;
      void dump() const;

    private:
      char session_id_[common::MAX_PATH_LENGTH];
      common::BaseInfo base_info_;
      int32_t length_;
    };

    class ReqRcKeepAliveMessage: public common::BasePacket 
    {
    public:
      ReqRcKeepAliveMessage();
      virtual ~ReqRcKeepAliveMessage();

      void set_ka_info(const common::KeepAliveInfo& ka_info);

      int serialize(common::Stream& output) const ;
      int deserialize(common::Stream& input);
      int64_t length() const;

      const common::KeepAliveInfo& get_ka_info() const;
      void dump() const;

    protected:
      common::KeepAliveInfo ka_info_;
    };

    class RspRcKeepAliveMessage: public common::BasePacket 
    {
    public:
      RspRcKeepAliveMessage();
      virtual ~RspRcKeepAliveMessage();

      void set_update_flag(const bool update_flag = common::KA_FLAG);
      void set_base_info(const common::BaseInfo& base_info);

      int serialize(common::Stream& output) const ;
      int deserialize(common::Stream& input);
      int64_t length() const;

      bool get_update_flag() const;
      const common::BaseInfo& get_base_info() const;
      void dump() const;

    private:
      bool update_flag_;
      common::BaseInfo base_info_;
    };

    class ReqRcLogoutMessage: public ReqRcKeepAliveMessage
    {
    public:
      ReqRcLogoutMessage();
      virtual ~ReqRcLogoutMessage();
    };

    //Rsp use status msg 
    class ReqRcReloadMessage: public common::BasePacket 
    {
      public:
        ReqRcReloadMessage();
        virtual ~ReqRcReloadMessage();

        void set_reload_type(const common::ReloadType type);

        int serialize(common::Stream& output) const ;
        int deserialize(common::Stream& input);
        int64_t length() const;

        common::ReloadType get_reload_type() const;
        void dump() const;

      protected:
        common::ReloadType reload_type_;
    };
  }
}
#endif //TFS_MESSAGE_RCSESSION_MESSAGE_H_
