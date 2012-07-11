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
#include "base_packet_factory.h"
#include "local_packet.h"
#include "status_message.h"
namespace tfs
{
  namespace common 
  {
    tbnet::Packet* BasePacketFactory::createPacket(int pcode)
    {
      tbnet::Packet* packet = NULL;
      int real_pcode = (pcode & 0xFFFF);
      switch (real_pcode)
      {
      case LOCAL_PACKET:
        packet = new LocalPacket();
        break;
      case STATUS_MESSAGE:
        packet = new StatusMessage();
        break;
      }
      return packet;
    }

    tbnet::Packet* BasePacketFactory::clone_packet(tbnet::Packet* packet, const int32_t version, const bool deserialize) 
    {
      tbnet::Packet* clone_packet = NULL;
      if (NULL != packet)
      {
        clone_packet = createPacket(packet->getPCode());
        if (NULL != clone_packet)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(clone_packet);
          TBSYS_LOG(DEBUG, "pcode: %d, length: %"PRI64_PREFIX"d", bpacket->getPCode(), bpacket->length());
          bool bret = bpacket->copy(dynamic_cast<BasePacket*>(packet), version, deserialize);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "clone packet error, pcode: %d", packet->getPCode());
            //bpacket->set_auto_free();
            bpacket->free();
            clone_packet = NULL;
          }
        }
      }
      return clone_packet;
    }
  }
}
