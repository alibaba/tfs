/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rc_service.h 494 2011-06-14 08:24:11Z zongdai@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_RCSERVICE_H_
#define TFS_RCSERVER_RCSERVICE_H_

#include "common/base_service.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace rcserver
  {
    class IResourceManager;
    class SessionManager;
    class RcService : public common::BaseService
    {
      public:
        RcService();
        ~RcService();

      public:
        // override
        tbnet::IPacketStreamer* create_packet_streamer()
        {
          return new common::BasePacketStreamer();
        }

        void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
        {
          tbsys::gDelete(streamer);
        }
        // override
        common::BasePacketFactory* create_packet_factory()
        {
          return new message::MessageFactory();
        }

        void destroy_packet_factory(common::BasePacketFactory* factory)
        {
          tbsys::gDelete(factory);
        }
        // override
        const char* get_log_file_path();
        const char* get_pid_file_path();
        // override
        bool handlePacketQueue(tbnet::Packet* packet, void* args);

      private:
        // override
        int initialize(int argc, char* argv[]);
        // override
        int destroy_service();

        int req_login(common::BasePacket* packet);
        int req_keep_alive(common::BasePacket* packet);
        int req_logout(common::BasePacket* packet);
        int req_reload(common::BasePacket* packet);

        // reload config
        int req_reload_config();
        // reload resource from db
        int req_reload_resource();

      private:
        DISALLOW_COPY_AND_ASSIGN(RcService);

        IResourceManager* resource_manager_;
        SessionManager* session_manager_;

        tbutil::Mutex mutex_;
    };

  }
}
#endif //TFS_RCSERVER_RCSERVICE_H_
