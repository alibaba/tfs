/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: adminserver.h 443 2011-06-08 09:07:08Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#ifndef TFS_ADMINSERVER_ADMINSERVER_H_
#define TFS_ADMINSERVER_ADMINSERVER_H_

#include <vector>
#include <string>
#include "common/func.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_service.h"
#include "message/message_factory.h"
#include "message/admin_cmd_message.h"

namespace tfs
{
  namespace adminserver
  {
    enum ServiceName
    {
      SERVICE_NONE = 0,
      SERVICE_NS,
      SERVICE_DS
    };

    struct MonitorParam
    {
      std::string index_;
      common::IpAddr adr_;
      std::string lock_file_;
      std::string script_;
      std::string description_;
      int32_t fkill_waittime_;
      int32_t active_;
    };

    typedef std::map<std::string, MonitorParam*> MSTR_PARA;
    typedef std::map<std::string, MonitorParam*>::iterator MSTR_PARA_ITER;
    typedef std::map<std::string, message::MonitorStatus*> MSTR_STAT;
    typedef std::map<std::string, message::MonitorStatus*>::iterator MSTR_STAT_ITER;

    class AdminServer : public common::BaseService
    {
    public:
      AdminServer();

      virtual ~AdminServer();
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

      void destruct();

      int start_monitor();
      int stop_monitor();
      static void* do_monitor(void* args);
      int run_monitor();

      // command response
      int cmd_reply_status(message::AdminCmdMessage* message);
      int cmd_check(message::AdminCmdMessage* message);
      int cmd_start_monitor(message::AdminCmdMessage* message);
      int cmd_restart_monitor(message::AdminCmdMessage* message);
      int cmd_stop_monitor(message::AdminCmdMessage* message);
      int cmd_start_monitor_index(message::AdminCmdMessage* message);
      int cmd_restart_monitor_index(message::AdminCmdMessage* message);
      int cmd_stop_monitor_index(message::AdminCmdMessage* message);
      int cmd_exit(message::AdminCmdMessage* message);

    private:
      void reload_config();
      void add_index(const std::string& index, const bool add_conf = false);
      void clear_index(const std::string& index, const bool del_conf = true);
      void modify_conf(const std::string& index, const int32_t type);
      void set_ds_list(const char* index_range, std::vector<std::string>& ds_index);
      int get_param(const std::string& index);
      int ping(const uint64_t ip);
      int ping_nameserver(const int status);
      int kill_process(message::MonitorStatus* status, const int32_t wait_time, const bool clear = false);

    private:

      // monitor stuff
      bool stop_;
      bool running_;
      int32_t check_interval_;
      int32_t check_count_;
      int32_t warn_dead_count_;

      MSTR_PARA monitor_param_;
      MSTR_STAT monitor_status_;

    };

  }
}
#endif //TFS_ADMINSERVER_ADMINSERVER_H_
