/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: client_cmd_message.h 384 2011-05-31 07:47:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CLIENTCMDMESSAGE_H_
#define TFS_MESSAGE_CLIENTCMDMESSAGE_H_
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    // Client Command
    class ClientCmdMessage: public common::BasePacket 
    {
    public:
      ClientCmdMessage();
      virtual ~ClientCmdMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;
      inline void set_value1(const uint64_t value)
      {
        info_.value1_ = value;
      }
      inline uint64_t get_value1() const
      {
        return info_.value1_;
      }
      inline void set_value2(const uint64_t value)
      {
        info_.value2_ = value;
      }
      inline uint64_t get_value2() const
      {
        return info_.value2_;
      }
      inline void set_value3(const uint32_t value)
      {
        info_.value3_ = value;
      }
      inline uint32_t get_value3() const
      {
        return info_.value3_;
      }
      inline void set_value4(const uint32_t value)
      {
        info_.value4_ = value;
      }
      inline uint32_t get_value4() const
      {
        return info_.value4_;
      }
      inline void set_cmd(const int32_t cmd)
      {
        info_.cmd_ = cmd;
      }
      inline int32_t get_cmd() const
      {
        return info_.cmd_;
      }
      inline const common::ClientCmdInformation& get_cmd_info() const
      {
        return info_;
      }
    protected:
      common::ClientCmdInformation info_;
    };
  }
}
#endif
