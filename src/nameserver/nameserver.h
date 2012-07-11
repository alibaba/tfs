/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: nameserver.h 590 2011-07-21 09:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_NAMESERVER_H_
#define TFS_NAMESERVER_NAMESERVER_H_

#include "common/internal.h"
#include "common/base_service.h"
#include "ns_define.h"
#include "layout_manager.h"
#include "heart_manager.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    /*class OwnerCheckTimerTask: public tbutil::TimerTask
    {
    public:
      explicit OwnerCheckTimerTask(NameServer& manager);
      virtual ~OwnerCheckTimerTask();
      virtual void runTimerTask();
    private:
      DISALLOW_COPY_AND_ASSIGN( OwnerCheckTimerTask);
      NameServer& manager_;
      const int64_t MAX_LOOP_TIME;
      int64_t max_owner_check_time_;
      int64_t owner_check_time_;
      int32_t main_task_queue_size_;
    };
    typedef tbutil::Handle<OwnerCheckTimerTask> OwnerCheckTimerTaskPtr;*/

    class NameServer: public common::BaseService
    {
    public:
      NameServer();
      virtual ~NameServer();
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

      LayoutManager& get_layout_manager() { return layout_manager_;}
      HeartManagement& get_heart_management() { return heart_manager_;}

   private:
      DISALLOW_COPY_AND_ASSIGN(NameServer);
      LayoutManager layout_manager_;
      NameServerHeartManager master_slave_heart_manager_;
      HeartManagement heart_manager_;
    protected:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      /** get pid file path */
      virtual const char* get_pid_file_path() { return NULL;}

    private:
      int open(common::BasePacket* msg);
      int close(common::BasePacket* msg);
      int batch_open(common::BasePacket* msg);
      int update_block_info(common::BasePacket* msg);
      int show_server_information(common::BasePacket* msg);
      //int owner_check(common::BasePacket* msg);
      int ping(common::BasePacket* msg);
      int dump_plan(common::BasePacket* msg);
      int client_control_cmd(common::BasePacket* msg);
      int do_master_msg_helper(common::BasePacket* packet);
      int do_slave_msg_helper(common::BasePacket* packet);

      int initialize_ns_global_info();
    };
  }/** nameserver **/
}/** tfs **/
#endif
