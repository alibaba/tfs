/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: message_factory.h 186 2011-04-22 13:47:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_PACKET_FACTORY_H_
#define TFS_COMMON_BASE_PACKET_FACTORY_H_
#include <tbnet.h>
#include "base_packet.h"
namespace tfs
{
  namespace common 
  {
    class BasePacketFactory: public tbnet::IPacketFactory
    {
    public:
      BasePacketFactory(){}
      virtual ~BasePacketFactory(){}
      virtual tbnet::Packet* createPacket(int pcode);
      virtual tbnet::Packet* clone_packet(tbnet::Packet* packet, const int32_t version, const bool deserialize); 
    };
  }
}
#endif //TFS_COMMON_BASE_PACKET_FACTORY_H_
