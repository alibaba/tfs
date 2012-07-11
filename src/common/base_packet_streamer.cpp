/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet_streamer.cpp 5 2011-04-22 16:22:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "base_packet.h"
#include "base_packet_streamer.h"

namespace tfs
{
  namespace common
  {
    BasePacketStreamer::BasePacketStreamer()
    {

    }

    BasePacketStreamer::BasePacketStreamer(tbnet::IPacketFactory* factory) :
      IPacketStreamer(factory)
    {
    }

    BasePacketStreamer::~BasePacketStreamer()
    {

    }

    void BasePacketStreamer::set_packet_factory(tbnet::IPacketFactory* factory)
    {
      _factory = factory;
    }

    bool BasePacketStreamer::getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool* broken)
    {
      bool bret = NULL != input && NULL != header;
      if (bret)
      {
        bret = input->getDataLen() >= TFS_PACKET_HEADER_V0_SIZE;
        if (bret)
        {
          //Func::hex_dump(input->getData(), input->getDataLen());
          //first peek header size, check the flag , if flag == v1, need more data;
          TfsPacketNewHeaderV0 pheader;
          int64_t pos = 0;
          int32_t iret = pheader.deserialize(input->getData(), input->getDataLen(), pos);
          bret = TFS_SUCCESS == iret;
          if (!bret)
          {
            *broken = true;
            input->clear();
            TBSYS_LOG(ERROR, "header deserialize error, iret: %d", iret);
          }
          else
          {
            if ((TFS_PACKET_FLAG_V1 == pheader.flag_)
                && ((input->getDataLen() - pheader.length()) < TFS_PACKET_HEADER_DIFF_SIZE))
            {
              bret = false;
            }
            else
            {
              input->drainData(pheader.length());
              header->_dataLen = pheader.length_;
              header->_pcode   = pheader.type_;
              header->_chid = 1;
              uint64_t channel_id = 0;

              if (((TFS_PACKET_FLAG_V0 != pheader.flag_)
                    &&(TFS_PACKET_FLAG_V1 != pheader.flag_))
                  || ( 0 >= header->_dataLen)
                  || (0x4000000 < header->_dataLen))//max 64M
              {
                *broken = true;
                input->clear();
                TBSYS_LOG(ERROR, "stream error: %x<>%x,%x, dataLen: %d", pheader.flag_,
                    TFS_PACKET_FLAG_V0, TFS_PACKET_FLAG_V1, header->_dataLen);
              }
              else
              {
                if (TFS_PACKET_FLAG_V1 == pheader.flag_)
                {
                  header->_pcode |= (pheader.check_ << 16);
                  header->_dataLen += TFS_PACKET_HEADER_DIFF_SIZE;
                  pos = 0;
                  iret = Serialization::get_int64(input->getData(), input->getDataLen(), pos, reinterpret_cast<int64_t*>(&channel_id));
                  bret = TFS_SUCCESS == iret;
                  if (bret)
                  {
                    if (TFS_PACKET_VERSION_V2 == pheader.check_)
                    {
                      header->_chid = static_cast<uint32_t>((channel_id & 0xFFFFFFFF));
                    }
                    else if (TFS_PACKET_VERSION_V1 == pheader.check_)
                    {
                      //tbnet version client got origin version 1 packet
                      header->_chid = static_cast<uint32_t>((channel_id & 0xFFFFFFFF)) - 1;
                    }
                    if (header->_chid == 0)
                    {
                      TBSYS_LOG(ERROR, "get a packet chid==0, pcode: %d,channel_id:%"PRI64_PREFIX"d", header->_pcode, channel_id);
                    }
                  }
                  else
                  {
                    bret = false;
                    *broken = true;
                    input->clear();
                    TBSYS_LOG(ERROR, "channel id deserialize error, iret: %d", iret);
                  }
                }
              }
            }
          }
        }
      }
      return bret;
    }

    tbnet::Packet* BasePacketStreamer::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header)
    {
      assert(_factory != NULL);
      tbnet::Packet* packet = NULL;
      if (NULL != input && NULL != header)
      {
        packet = _factory->createPacket(header->_pcode);
        if (NULL != packet)
        {
          if (!packet->decode(input, header))
          {
            packet->free();
            packet = NULL;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "create packet: %d fail, drain data length: %d",
            header->_pcode, header->_dataLen);
          input->drainData(header->_dataLen);
        }
      }
      return packet;
    }

    bool BasePacketStreamer::encode(tbnet::Packet* packet, tbnet::DataBuffer* output)
    {
      bool bret = NULL != packet && NULL != output;
      if (bret)
      {
        tbnet::PacketHeader* header = packet->getPacketHeader();
        int32_t old_len = output->getDataLen();
        BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
        int32_t iret = TFS_SUCCESS;
        int32_t header_length = 0;
        int64_t pos  = 0;
        //v1
        if (TFS_PACKET_VERSION_V1 == bpacket->get_version())
        {
          TfsPacketNewHeaderV1 pheader;
          pheader.crc_ = bpacket->get_crc();
          pheader.flag_ = TFS_PACKET_FLAG_V1;
          pheader.id_  = bpacket->get_id();
          pheader.length_ = bpacket->get_data_length();
          pheader.type_ = header->_pcode;
          pheader.version_ = bpacket->get_version();
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }//v2 tbnet
        else if (TFS_PACKET_VERSION_V2 == bpacket->get_version())
        {
          TfsPacketNewHeaderV1 pheader;
          pheader.crc_ = bpacket->get_crc();
          pheader.flag_ = TFS_PACKET_FLAG_V1;
          pheader.id_  = bpacket->getChannelId();
          pheader.length_ = bpacket->get_data_length();
          pheader.type_ = header->_pcode;
          pheader.version_ = bpacket->get_version();
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }
        else//v0
        {
          TfsPacketNewHeaderV1 pheader;
          memset(&pheader, 0, sizeof(pheader));
          pheader.flag_ = TFS_PACKET_FLAG_V0;
          char* header_data = reinterpret_cast<char*>(&pheader);
          int32_t length = TFS_PACKET_HEADER_V0_SIZE - 2;
          for (int32_t i = 0; i < length; i++)
          {
            pheader.version_ = static_cast<uint16_t>(*(header_data + i));
          }
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }
        bret = TFS_SUCCESS == iret;
        if (bret)
        {
          //TBSYS_LOG(DEBUG, "pcode: %d, header length: %d, body length : %"PRI64_PREFIX"d", bpacket->getPCode(), header_length, bpacket->get_data_length());
          //Func::hex_dump(output->getData(), output->getDataLen());
          output->pourData(header_length);
          bret = bpacket->encode(output);
          //Func::hex_dump(output->getData(), output->getDataLen());
        }
        else
        {
          TBSYS_LOG(ERROR, "encode erorr, pcode: %d", header->_pcode);
          output->stripData(output->getDataLen() - old_len);
        }
      }
      return bret;
    }
  }
}
