/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_service.cpp 185 2011-04-21 14:44:43Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tblog.h>
#include <libgen.h>
#include "error_msg.h"
#include "config_item.h"
#include "base_service.h"
#include "directory_op.h"
#include "local_packet.h"

namespace tfs
{
  namespace common
  {
    int BaseService::golbal_async_callback_func(NewClient* client, void* args)
    {
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      int32_t iret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = service->async_callback(client, args);
      }
      return iret;
    }

    BaseService::BaseService():
      packet_factory_(NULL),
      streamer_(NULL),
      timer_(0),
      work_queue_size_(10240)
    {

    }

    BaseService::~BaseService()
    {

    }

    bool BaseService::destroy()
    {
      if (NULL != transport_)
        transport_->stop();
      main_workers_.stop();
      destroy_service();
      NewClientManager::get_instance().destroy();
      if (0 != timer_)
      {
        timer_->destroy();
      }

      if (NULL != transport_)
        transport_->wait();
      main_workers_.wait();

      destroy_packet_factory(packet_factory_);
      destroy_packet_streamer(streamer_);
      tbsys::gDelete(transport_);
      return true;
    }

    int BaseService::async_callback(NewClient* client, void*)
    {
      assert(NULL != client);
      LocalPacket* packet = dynamic_cast<LocalPacket*>(packet_factory_->createPacket(LOCAL_PACKET));
      assert(NULL != packet);
      packet->set_new_client(client);
      bool bret = main_workers_.push(packet, 0/*no limit*/, false/*no block*/);
      assert(true == bret);
      return TFS_SUCCESS;
    }

    bool BaseService::push(BasePacket* packet, bool block)
    {
      return main_workers_.push(packet, work_queue_size_, block);
    }

    tbnet::IPacketHandler::HPRetCode BaseService::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      TBSYS_LOG(DEBUG, "packet code : %d", packet->getPCode());
      bool bret = NULL != connection && NULL != packet;
      if (bret)
      {
        if (!packet->isRegularPacket())
        {
          bret = false;
          TBSYS_LOG(WARN, "control packet, pcode: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        }
        if (bret)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
          bpacket->set_connection(connection);
          bpacket->setExpireTime(MAX_RESPONSE_TIME);
          bpacket->set_direction(static_cast<DirectionStatus>(bpacket->get_direction()|DIRECTION_RECEIVE));

          if (bpacket->is_enable_dump())
          {
            bpacket->dump();
          }
          if (!main_workers_.push(bpacket, work_queue_size_, false))
          {
            bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
            bpacket->free();
          }
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    /** Note if return true, PacketQueueThread will delete this packet*/
    bool BaseService::handlePacketQueue(tbnet::Packet *packet, void *)
    {
      bool bret = true;
      if (NULL == packet)
      {
        bret = false;
        TBSYS_LOG(ERROR, "%s", "invalid packet, packet is null");
      }
      else
      {
        if (LOCAL_PACKET == packet->getPCode())
        {
          LocalPacket* local_packet = dynamic_cast<LocalPacket*>(packet);
          int32_t iret = local_packet->execute();
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, " LocalPacket execute error, iret: %d", iret);
          }
        }
      }
      return bret;
    }

    int32_t BaseService::get_port() const
    {
      int32_t port = -1;
      port = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_PORT, -1);
      if (port < 1024 || port > 65535)
      {
        port = -1;
      }
      return port;
    }

    const char* const BaseService::get_dev() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_DEV_NAME, NULL);
    }

    int32_t BaseService::get_work_thread_count() const
    {
      return TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_THREAD_COUNT, 8);
    }

    int32_t BaseService::get_work_queue_size() const
    {
      return work_queue_size_;
    }

    const char* BaseService::get_ip_addr() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_IP_ADDR, NULL);
    }

    int BaseService::run(int argc , char*argv[])
    {
      //check ip, port, starrt tbnet
      int32_t iret = initialize_network(argv[0]);
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s initialize network failed, must be exit", argv[0]);
      }

      //start workthread
      if (TFS_SUCCESS == iret)
      {
        int32_t thread_count = get_work_thread_count();
        main_workers_.setThreadParameter(thread_count, this, NULL);
        main_workers_.start();

        work_queue_size_ = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_TASK_MAX_QUEUE_SIZE, 10240);
        work_queue_size_ = std::max(work_queue_size_, 10240);
        work_queue_size_ = std::min(work_queue_size_, 40960);
        timer_ = new tbutil::Timer();
      }

      if (TFS_SUCCESS == iret)
      {
        iret = initialize(argc, argv);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s initialize user data failed, must be exit", argv[0]);
        }
      }
      return iret;
    }

    int BaseService::initialize_network(const char* app_name)
    {
      int32_t iret = TFS_SUCCESS;
      const char* ip_addr = get_ip_addr();
      if (NULL == ip_addr)//get ip addr
      {
        iret =  EXIT_CONFIG_ERROR;
        TBSYS_LOG(ERROR, "%s not set ip_addr", app_name);
      }

      int32_t port = 0;
      if (TFS_SUCCESS == iret)
      {
        port  = get_listen_port();//check port
        if (port <= 0)
        {
          iret = EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s not set port: %d or port: %d is invalid", app_name, port, port);
        }
      }

      //start transport
      if (TFS_SUCCESS == iret)
      {
        char spec[32];
        sprintf(spec, "tcp::%d", port);

        packet_factory_ = create_packet_factory();
        if (NULL == packet_factory_)
        {
          iret = EXIT_GENERAL_ERROR;
          TBSYS_LOG(ERROR, "%s create packet factory fail", app_name);
        }
        if (TFS_SUCCESS == iret)
        {
          streamer_ = dynamic_cast<BasePacketStreamer*>(create_packet_streamer());
          if (NULL == streamer_)
          {
            TBSYS_LOG(ERROR, "%s create packet streamer fail", app_name);
            iret = EXIT_GENERAL_ERROR;
          }
          else
          {
            transport_ = new tbnet::Transport();
            streamer_->set_packet_factory(packet_factory_);
            tbnet::IOComponent* com = transport_->listen(spec, streamer_, this);
            if (NULL == com)
            {
              TBSYS_LOG(ERROR, "%s listen port: %d fail", app_name, port);
              iret = EXIT_NETWORK_ERROR;
            }
            else
            {
              transport_->start();
            }
          }
        }
      }

      // start client manager
      if (TFS_SUCCESS == iret)
      {
        iret = NewClientManager::get_instance().initialize(packet_factory_, streamer_,
                transport_, &BaseService::golbal_async_callback_func, this);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s start client manager fail", app_name);
          iret = EXIT_NETWORK_ERROR;
        }
      }
      return iret;
    }
  }/** common **/
}/** tfs **/
