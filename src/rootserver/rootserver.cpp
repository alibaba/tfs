/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rootserver.cpp 590 2011-08-17 16:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 */

#include <Service.h>
#include <Memory.hpp>
#include <iterator>
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/local_packet.h"
#include "common/directory_op.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "message/heart_message.h"
#include "rootserver.h"
#include "global_factory.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace rootserver
  {
    RootServer::RootServer():
      rt_ms_heartbeat_handler_(*this),
      rt_rs_heartbeat_handler_(*this),
      rs_heart_manager_(manager_)
    {

    }

    RootServer::~RootServer()
    {

    }

    int RootServer::initialize(int /*argc*/, char* /*argv*/[])
    {
      int32_t iret =  SYSPARAM_RTSERVER.initialize();
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize rootserver parameter error, must be exit");
        iret = EXIT_GENERAL_ERROR;
      }

      if (TFS_SUCCESS == iret)
      {
        iret = GFactory::initialize();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s", "GFactory initialize error, must be exit");
        }
      }

      const char* ip_addr = get_ip_addr();
      if (TFS_SUCCESS == iret)
      {
        if (NULL == ip_addr)//get ip addr
        {
          iret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s", "rootserver not set ip_addr");
        }
      }

      if (TFS_SUCCESS == iret)
      {
        const char *dev_name = get_dev();
        if (NULL == dev_name)//get dev name
        {
          iret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s","rootserver not set dev_name");
        }
        else
        {
          uint32_t ip_addr_id = tbsys::CNetUtil::getAddr(ip_addr);
          if (0 == ip_addr_id)
          {
            iret =  EXIT_CONFIG_ERROR;
            TBSYS_LOG(ERROR, "%s", "rootserver not set ip_addr");
          }
          else
          {
            uint32_t local_ip = Func::get_local_addr(dev_name);
            RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            RWLock::Lock lock(ngi, WRITE_LOCKER);
            ngi.info_.base_info_.id_  = tbsys::CNetUtil::ipToAddr(local_ip, get_port());
            bool find_ip_in_dev = Func::is_local_addr(ip_addr_id);
            if (!find_ip_in_dev)
            {
              TBSYS_LOG(WARN, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
            }
          }
        }
      }

      if (TFS_SUCCESS == iret)
      {
        iret = initialize_global_info();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s", "initialize rootserver global information error, must be exit");
          iret = EXIT_GENERAL_ERROR;
        }
      }

      //initialize metaserver manager
      if (TFS_SUCCESS == iret)
      {
        const char* work_dir = get_work_dir();
        iret = NULL == work_dir ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          std::string table_dir(work_dir + std::string("/rootserver"));
          iret = DirectoryOp::create_full_path(table_dir.c_str()) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            std::string table_file_path(table_dir + std::string("/table"));
            iret = manager_.initialize(table_file_path);
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "meta server manager initialize error, iret: %d, must be exit", iret);
            }
          }
        }
      }

      //initialize rootserver ==> rootserver heartbeat
      if (TFS_SUCCESS == iret)
      {
        iret = rs_heart_manager_.initialize();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s", "initialize rootserver heart manager error, must be exit");
          iret = EXIT_GENERAL_ERROR;
        }
      }

      // initialize update table worker threads
      if (TFS_SUCCESS == iret)
      {
        int32_t up_table_thread_count = TBSYS_CONFIG.getInt(CONF_SN_ROOTSERVER, CONF_UPDATE_TABLE_THREAD_COUNT, 1);
        update_tables_workers_.setThreadParameter(up_table_thread_count, this, NULL);
        update_tables_workers_.start();
      }

      //initialize metaserver ==> rootserver heartbeat
      if (TFS_SUCCESS == iret)
      {
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_ROOTSERVER, CONF_HEART_THREAD_COUNT, 1);
        ms_rs_heartbeat_workers_.setThreadParameter(heart_thread_count, &rt_ms_heartbeat_handler_, this);
        ms_rs_heartbeat_workers_.start();
        rs_rs_heartbeat_workers_.setThreadParameter(1, &rt_rs_heartbeat_handler_, this);
        rs_rs_heartbeat_workers_.start();
        RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        RWLock::Lock lock(ngi, WRITE_LOCKER);
        ngi.info_.base_info_.status_ = RS_STATUS_INITIALIZED;
      }
      return iret;
    }

    int RootServer::destroy_service()
    {
      RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_NO)
      {

        {
          RWLock::Lock lock(ngi, WRITE_LOCKER);
          ngi.destroy_flag_ = NS_DESTROY_FLAGS_YES;
        }
        update_tables_workers_.stop();
        update_tables_workers_.wait();
        ms_rs_heartbeat_workers_.stop();
        ms_rs_heartbeat_workers_.wait();
        rs_rs_heartbeat_workers_.stop();
        rs_rs_heartbeat_workers_.wait();
        rs_heart_manager_.destroy();
        manager_.destroy();
      }
      return TFS_SUCCESS;
    }

    int RootServer::async_callback(NewClient* client, void*)
    {
      assert(NULL != client);
      common::LocalPacket* packet = dynamic_cast<common::LocalPacket*>(get_packet_factory()->createPacket(LOCAL_PACKET));
      assert(NULL != packet);
      packet->set_new_client(client);
      assert(NULL != client->get_source_msg());
      bool bret = false;
      int32_t pcode = client->get_source_msg()->getPCode();
      if (REQ_RT_UPDATE_TABLE_MESSAGE == pcode)
      {
        bret = update_tables_workers_.push(packet, 0 /*no limit */, false/*no block */);
      }
      else
      {
        bret = main_workers_.push(packet, 0/*no limit*/, false/*no block*/);
      }
      assert(true == bret);
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode RootServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode hret = tbnet::IPacketHandler::FREE_CHANNEL;
      bool bret = NULL != connection && NULL != packet;
      if (bret)
      {
        //TBSYS_LOG(DEBUG, "receive pcode : %d", packet->getPCode());
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
          int32_t pcode = bpacket->getPCode();
          int32_t iret = common::TFS_ERROR;
          RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          {
            RWLock::Lock lock(ngi, READ_LOCKER);
            if (ngi.info_.base_info_.role_ == RS_ROLE_MASTER)
              iret = do_master_msg_helper(bpacket);
            else
              iret = do_slave_msg_helper(bpacket);
          }
          if (common::TFS_SUCCESS == iret)
          {
            hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            switch (pcode)
            {
            case REQ_RT_MS_KEEPALIVE_MESSAGE:
              ms_rs_heartbeat_workers_.push(bpacket, 0/* no limit */, false/* no block */);
              break;
            case REQ_RT_RS_KEEPALIVE_MESSAGE:
            case HEARTBEAT_AND_NS_HEART_MESSAGE:
              rs_rs_heartbeat_workers_.push(bpacket, 0/* no limit */, false/* no block */);
              break;
            default:
              if (!main_workers_.push(bpacket, work_queue_size_))
              {
                bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
                bpacket->free();
              }
              break;
            }
          }
          else
          {
            bpacket->free();
            TBSYS_LOG(WARN, "the msg: %d will be ignored", pcode);
          }
        }
      }
      return hret;
    }

    /** handle packet*/
    bool RootServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t iret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case REQ_RT_GET_TABLE_MESSAGE:
              iret = get_tables(msg);
              break;
            default:
              iret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != iret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "execute message failed, pcode: %d", pcode);
          }
        }
      }
      return bret;
    }

    int RootServer::callback(common::NewClient* client)
    {
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
        iret = NULL != sresponse && fresponse != NULL ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          tbnet::Packet* packet = client->get_source_msg();
          assert(NULL != packet);
          int32_t pcode = packet->getPCode();
          if (REQ_RT_UPDATE_TABLE_MESSAGE == pcode)
          {
            if (!sresponse->empty())
            {
              UpdateTableResponseMessage* pmsg = NULL;
              NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
              for (; iter != sresponse->end(); ++iter)
              {
                if (RSP_RT_UPDATE_TABLE_MESSAGE == (iter->second.second->getPCode()))
                {
                  pmsg = dynamic_cast<UpdateTableResponseMessage*>(iter->second.second);
                  assert(NULL != pmsg);
                  manager_.update_tables_item_status(iter->second.first,
                    pmsg->get_version(), pmsg->get_status(), pmsg->get_phase());
                }
              }
            }
          }
        }
      }
      return iret;
    }

    int RootServer::initialize_global_info(void)
    {
      int32_t iret = TFS_SUCCESS;
      const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_ROOTSERVER, CONF_IP_ADDR_LIST);
      if (NULL == ns_ip)
      {
        iret = EXIT_GENERAL_ERROR;
        TBSYS_LOG(ERROR, "%s", "initialize rootserver ip is null, must be exit");
      }
      if (TFS_SUCCESS == iret)
      {
        std::vector < uint32_t > ip_list;
        char buffer[256];
        strncpy(buffer, ns_ip, 256);
        char *t = NULL;
        char *s = buffer;
        while ((t = strsep(&s, "|")) != NULL)
        {
          ip_list.push_back(tbsys::CNetUtil::getAddr(t));
        }

        if (2U != ip_list.size())
        {
          TBSYS_LOG(DEBUG, "%s", "must have two ns,check your ns' list");
          iret = EXIT_GENERAL_ERROR;
        }
        else
        {
          uint32_t local_ip = 0;
          bool bfind_flag = false;
          std::vector<uint32_t>::iterator iter = ip_list.begin();
          for (; iter != ip_list.end(); ++iter)
          {
            bfind_flag = Func::is_local_addr((*iter));
            if (bfind_flag)
            {
              local_ip = (*iter);
              break;
            }
          }
          if (!bfind_flag)
          {
            TBSYS_LOG(ERROR, "ip list: %s not in %s, must be exit", ns_ip, get_dev());
            iret = EXIT_GENERAL_ERROR;
          }
          else
          {
            RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            RWLock::Lock lock(ngi, WRITE_LOCKER);
            iter = ip_list.begin();
            for (;iter != ip_list.end(); ++iter)
            {
              if (local_ip == (*iter))
                ngi.info_.base_info_.id_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
              else
                ngi.other_id_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
            }
            ngi.info_.base_info_.status_ = RS_STATUS_UNINITIALIZE;
            ngi.vip_ = Func::get_addr(get_ip_addr());
            ngi.info_.base_info_.start_time_ = time(NULL);
            ngi.info_.base_info_.last_update_time_ = time(NULL);
          }
        }
      }
      return iret;
    }

    int RootServer::do_master_msg_helper(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int32_t pcode = packet->getPCode();
        RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        iret = (ngi.info_.base_info_.status_ >= RS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.info_.base_info_.status_ <= RS_STATUS_INITIALIZED) || (pcode == RS_STATUS_UNINITIALIZE) ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          //receive all owner check message , master and slave heart message, dataserver heart message
          if (pcode != REQ_RT_RS_KEEPALIVE_MESSAGE
            && pcode != REQ_RT_MS_KEEPALIVE_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE)
          {
            iret = ngi.info_.base_info_.status_ < RS_STATUS_INITIALIZED ? common::TFS_ERROR : common::TFS_SUCCESS;
          }
        }
      }
      return iret;
    }

    int RootServer::do_slave_msg_helper(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int32_t pcode = packet->getPCode();
        RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        iret = ngi.info_.base_info_.status_ >= RS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.info_.base_info_.status_ <= RS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          //receive all owner check message , master and slave heart message, dataserver heart message
          if ((pcode != HEARTBEAT_AND_NS_HEART_MESSAGE)
            && (pcode != RSP_RT_RS_KEEPALIVE_MESSAGE))
          {
            iret = ngi.info_.base_info_.status_ < RS_STATUS_INITIALIZED ? common::TFS_ERROR : common::TFS_SUCCESS;
          }
        }
      }
      return iret;
    }

    int RootServer::ms_keepalive(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        RtsMsHeartMessage* msg = dynamic_cast<RtsMsHeartMessage*>(packet);
        common::MetaServer& server = msg->get_ms();
        iret = manager_.keepalive(msg->get_type(), server);
        RtsMsHeartResponseMessage* reply_msg = new RtsMsHeartResponseMessage();
        reply_msg->set_ret_value(iret);
        reply_msg->set_active_table_version(server.tables_.version_);
        reply_msg->set_lease_expired_time(server.lease_.lease_expired_time_);
        reply_msg->set_renew_lease_interval_time(SYSPARAM_RTSERVER.mts_rts_renew_lease_interval_);
        TBSYS_LOG(DEBUG, "%s keepalive %s, type: %d, iret: %d",
          tbsys::CNetUtil::addrToString(server.base_info_.id_).c_str(),
          iret == TFS_SUCCESS ? "successful" : "failed", msg->get_type(), iret);
        iret = packet->reply(reply_msg);
      }
      return iret;
    }

    int RootServer::rs_keepalive(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        RtsRsHeartMessage* msg  = dynamic_cast<RtsRsHeartMessage*>(packet);
        common::RootServerInformation& server = msg->get_rs();
        iret = rs_heart_manager_.keepalive(msg->get_type(), server);
        RtsRsHeartResponseMessage* reply_msg = new RtsRsHeartResponseMessage();
        reply_msg->set_ret_value(iret);
        if (TFS_SUCCESS == iret)
        {
          reply_msg->set_active_table_version(manager_.get_active_table_version());
          reply_msg->set_lease_expired_time(server.lease_.lease_expired_time_);
          reply_msg->set_renew_lease_interval_time(SYSPARAM_RTSERVER.rts_rts_renew_lease_interval_);
        }
        iret = packet->reply(reply_msg);
        TBSYS_LOG(DEBUG, "slave rootserver %s keepalive %s",
          tbsys::CNetUtil::addrToString(server.base_info_.id_).c_str(),
          iret == TFS_SUCCESS ? "successful" : "failed");
      }
      return iret;
    }

    int RootServer::get_tables(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        GetTableFromRtsResponseMessage* reply_msg = new GetTableFromRtsResponseMessage();
        iret = manager_.get_tables(reply_msg->get_table(), reply_msg->get_table_length(), reply_msg->get_version());
        if (TFS_SUCCESS == iret)
        {
          iret = packet->reply(reply_msg);
        }
        else
        {
          tbsys::gDelete(reply_msg);
        }
      }
      return iret;
    }

    int RootServer::rs_ha_ping(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        RsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        HeartBeatAndNSHeartMessage* reply_msg = new HeartBeatAndNSHeartMessage();
        RWLock::Lock lock(ngi, READ_LOCKER);
        reply_msg->set_ns_switch_flag_and_status(0 /*no use*/ , ngi.info_.base_info_.status_);
        iret = packet->reply(reply_msg);
      }
      return iret;
    }

    int rs_async_callback(common::NewClient* client)
    {
      RootServer* service = dynamic_cast<RootServer*>(BaseMain::instance());
      int32_t iret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = service->callback(client);
      }
      return iret;
    }

    bool RootServer::KeepAliveIPacketQueueHandlerHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool bret = packet != NULL;
      if (bret)
      {
        //if return TFS_SUCCESS, packet had been delete in this func
        //if handlePacketQueue return true, tbnet will delete this packet
        assert(packet->getPCode() == REQ_RT_MS_KEEPALIVE_MESSAGE);
        manager_.ms_keepalive(dynamic_cast<BasePacket*>(packet));
      }
      return bret;
    }

    bool RootServer::RsKeepAliveIPacketQueueHandlerHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool bret = packet != NULL;
      if (bret)
      {
        //if return TFS_SUCCESS, packet had been delete in this func
        //if handlePacketQueue return true, tbnet will delete this packet
        int32_t pcode = packet->getPCode();
        int32_t iret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case REQ_RT_RS_KEEPALIVE_MESSAGE:
            case RSP_RT_RS_KEEPALIVE_MESSAGE:
              iret = manager_.rs_keepalive(msg);
              break;
            case HEARTBEAT_AND_NS_HEART_MESSAGE:
              iret = manager_.rs_ha_ping(msg);
              break;
            default:
              iret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != iret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "execute message failed");
          }
        }
      }
      return bret;
    }
  } /** rootserver **/
}/** tfs **/

