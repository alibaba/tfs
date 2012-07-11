/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rootserver.h 590 2011-08-17 14:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_ROOTSERVER_NAMESERVER_H_
#define TFS_ROOTSERVER_NAMESERVER_H_

#include "message/message_factory.h"
#include "common/internal.h"
#include "common/base_service.h"

#include "meta_server_manager.h"
#include "root_server_heart_manager.h"

namespace tfs
{
  namespace rootserver
  {
    class RootServer: public common::BaseService
    {
    public:
      RootServer();
      virtual ~RootServer();
      /** initialize application data*/
      virtual int initialize(int argc, char* argv[]);

      /** destroy application data*/
      virtual int destroy_service();

      /** create the packet streamer, this is used to create packet according to packet code */
      virtual tbnet::IPacketStreamer* create_packet_streamer()
      {
        return new common::BasePacketStreamer();
      }

      /** destroy the packet streamer*/
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
      {
        tbsys::gDelete(streamer);
      }

      /** create the packet streamer, this is used to create packet*/
      virtual common::BasePacketFactory* create_packet_factory()
      {
        return new message::MessageFactory();
      }

      /** destroy packet factory*/
      virtual void destroy_packet_factory(common::BasePacketFactory* factory)
      {
        tbsys::gDelete(factory);
      }

      /** handle single packet */
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      /** handle packet*/
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

      int callback(common::NewClient* client);

      virtual int async_callback(common::NewClient* client, void* args);
    private:
      DISALLOW_COPY_AND_ASSIGN(RootServer);

      int do_master_msg_helper(common::BasePacket* packet);
      int do_slave_msg_helper(common::BasePacket* packet);
      int initialize_global_info(void);

    protected:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      /** get pid file path */
      virtual const char* get_pid_file_path() { return NULL;}

    private:
      class KeepAliveIPacketQueueHandlerHelper : public tbnet::IPacketQueueHandler
      {
      public:
        explicit KeepAliveIPacketQueueHandlerHelper(RootServer& manager): manager_(manager){ }
        virtual ~KeepAliveIPacketQueueHandlerHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHandlerHelper);
        RootServer& manager_;
      };
      class RsKeepAliveIPacketQueueHandlerHelper : public tbnet::IPacketQueueHandler
      {
        public:
          explicit RsKeepAliveIPacketQueueHandlerHelper(RootServer& manager): manager_(manager){ }
          virtual ~RsKeepAliveIPacketQueueHandlerHelper() {}
          virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
        private:
          DISALLOW_COPY_AND_ASSIGN(RsKeepAliveIPacketQueueHandlerHelper);
          RootServer& manager_;
      };
    private:
      int ms_keepalive(common::BasePacket* packet);
      int rs_keepalive(common::BasePacket* packet);
      int rs_ha_ping(common::BasePacket* packet);
      int get_tables(common::BasePacket* packet);

    private:
      tbnet::PacketQueueThread update_tables_workers_;
      tbnet::PacketQueueThread ms_rs_heartbeat_workers_;
      tbnet::PacketQueueThread rs_rs_heartbeat_workers_;
      KeepAliveIPacketQueueHandlerHelper rt_ms_heartbeat_handler_;
      RsKeepAliveIPacketQueueHandlerHelper rt_rs_heartbeat_handler_;
      MetaServerManager manager_;
      RootServerHeartManager rs_heart_manager_;
    };
  }/** rootserver **/
}/** tfs **/
#endif
