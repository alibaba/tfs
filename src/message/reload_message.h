/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: reload_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_RELOADMESSAGE_H_
#define TFS_MESSAGE_RELOADMESSAGE_H_
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class ReloadConfigMessage: public common::BasePacket 
    {
    public:
      ReloadConfigMessage();
      virtual ~ReloadConfigMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;

      void set_switch_cluster_flag(const int32_t flag);
      int32_t get_switch_cluster_flag() const;
    protected:
      int32_t flag_;
    };
  }
}
#endif
