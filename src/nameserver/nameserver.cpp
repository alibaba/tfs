/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: nameserver.cpp 983 2011-10-31 09:59:33Z duanfei $
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
#include "nameserver.h"

#include <Service.h>
#include <Memory.hpp>
#include <iterator>
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "global_factory.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    /*OwnerCheckTimerTask::OwnerCheckTimerTask(NameServer& manager) :
      manager_(manager),
      MAX_LOOP_TIME(SYSPARAM_NAMESERVER.heart_interval_ * 1000 * 1000 / 2)
    {
      int32_t percent_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 200);
      owner_check_time_ = (manager_.get_work_queue_size() * percent_size) * 1000;//us
      max_owner_check_time_ = owner_check_time_ * 4;//us
      TBSYS_LOG(INFO, "owner_check_time: %"PRI64_PREFIX"d(us), max_owner_check_time: %"PRI64_PREFIX"u(us)",
          owner_check_time_, max_owner_check_time_);
    }

    OwnerCheckTimerTask::~OwnerCheckTimerTask()
    {

    }

    void OwnerCheckTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.owner_status_ >= NS_STATUS_INITIALIZED)
      {
        bool bret = false;
        tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
        tbutil::Time end = now + MAX_LOOP_TIME;
        if (ngi.last_owner_check_time_ >= ngi.last_push_owner_check_packet_time_)
        {
          int64_t current = 0;
          OwnerCheckMessage* message = new OwnerCheckMessage();
          while(!bret
              && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
              && current < end.toMicroSeconds())
          {
            bret = manager_.push(message, false);
            if (!bret)
            {
              usleep(500);
              current = tbutil::Time::now(tbutil::Time::Monotonic).toMicroSeconds();
            }
            else
            {
              ngi.last_push_owner_check_packet_time_ = now.toMicroSeconds();
            }
          }
          if (!bret)
          {
            message->free();
          }
        }
        else
        {
          int64_t diff = now.toMicroSeconds() - ngi.last_owner_check_time_;
          if (diff >= max_owner_check_time_)
          {
            TBSYS_LOG(INFO,"last push owner check packet time: %"PRI64_PREFIX"d(us) > max owner check time: %"PRI64_PREFIX"d(us), nameserver dead, modify owner status(uninitialize)",
                ngi.last_push_owner_check_packet_time_, now.toMicroSeconds());
            ngi.owner_status_ = NS_STATUS_UNINITIALIZE;//modif owner status
          }
        }
      }
      return;
    }*/

    NameServer::NameServer() :
      layout_manager_(*this),
      master_slave_heart_manager_(layout_manager_),
      heart_manager_(*this)
    {

    }

    NameServer::~NameServer()
    {

    }

    int NameServer::initialize(int /*argc*/, char* /*argv*/[])
    {
      int32_t ret =  SYSPARAM_NAMESERVER.initialize();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize nameserver parameter error, must be exit");
        ret = EXIT_GENERAL_ERROR;
      }
      const char* ip_addr = get_ip_addr();
      if (NULL == ip_addr)//get ip addr
      {
        ret =  EXIT_CONFIG_ERROR;
        TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
      }

      if (TFS_SUCCESS == ret)
      {
        const char *dev_name = get_dev();
        if (NULL == dev_name)//get dev name
        {
          ret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s","nameserver not set dev_name");
        }
        else
        {
          uint32_t ip_addr_id = tbsys::CNetUtil::getAddr(ip_addr);
          if (0 == ip_addr_id)
          {
            ret =  EXIT_CONFIG_ERROR;
            TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
          }
          else
          {
            uint32_t local_ip = Func::get_local_addr(dev_name);
            NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            ngi.owner_ip_port_  = tbsys::CNetUtil::ipToAddr(local_ip, get_port());
            bool find_ip_in_dev = Func::is_local_addr(ip_addr_id);
            if (!find_ip_in_dev)
            {
              TBSYS_LOG(WARN, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
            }
          }
        }
      }

      //start clientmanager
      if (TFS_SUCCESS == ret)
      {
        NewClientManager::get_instance().destroy();
        assert(NULL != get_packet_streamer());
        assert(NULL != get_packet_factory());
        BasePacketStreamer* packet_streamer = dynamic_cast<BasePacketStreamer*>(get_packet_streamer());
        BasePacketFactory* packet_factory   = dynamic_cast<BasePacketFactory*>(get_packet_factory());
        ret = NewClientManager::get_instance().initialize(packet_factory, packet_streamer,
                NULL, &BaseService::golbal_async_callback_func, this);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "start client manager failed, must be exit!!!");
          ret = EXIT_NETWORK_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = initialize_ns_global_info();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "%s", "initialize nameserver global information error, must be exit");
          ret = EXIT_GENERAL_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret =  layout_manager_.initialize();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize layoutmanager failed, must be exit, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_HEART_THREAD_COUNT, 1);
        int32_t report_thread_count = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_REPORT_BLOCK_THREAD_COUNT, 2);
        ret = heart_manager_.initialize(heart_thread_count, report_thread_count);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize heart manager failed, must be exit, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = master_slave_heart_manager_.initialize();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize master and slave heart manager failed, must be exit, ret: %d", ret);
        }
        else
        {
          if (GFactory::get_runtime_info().is_master())
          {
            ret = master_slave_heart_manager_.establish_peer_role_(GFactory::get_runtime_info());
            if (EXIT_ROLE_ERROR == ret)
            {
              TBSYS_LOG(INFO, "nameserve role error, must be exit, ret: %d", ret);
            }
            else
            {
              ret = TFS_SUCCESS;
            }
          }
        }
      }

      //start heartbeat loop
      if (TFS_SUCCESS == ret)
      {
        //if we're the master ns or slave ns ,we can start service now.change status to INITIALIZED.
         GFactory::get_runtime_info().owner_status_ = NS_STATUS_INITIALIZED;
        /*int32_t percent_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
        int64_t owner_check_interval = get_work_queue_size() * percent_size * 1000;
        OwnerCheckTimerTaskPtr owner_check_task = new OwnerCheckTimerTask(*this);
        ret = get_timer()->scheduleRepeated(owner_check_task, tbutil::Time::microSeconds(owner_check_interval));
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "%s", "add timer task(OwnerCheckTimerTask) error, must be exit");
          ret = EXIT_GENERAL_ERROR;
        }*/
        TBSYS_LOG(INFO, "nameserver running, listen port: %d", get_port());
      }
      return ret;
    }

    int NameServer::destroy_service()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (!ngi.is_destroyed())
      {
        GFactory::destroy();
        heart_manager_.destroy();
        master_slave_heart_manager_.destroy();
        layout_manager_.destroy();

        heart_manager_.wait_for_shut_down();
        master_slave_heart_manager_.wait_for_shut_down();
        layout_manager_.wait_for_shut_down();
        GFactory::wait_for_shut_down();
      }
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode NameServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode hret = tbnet::IPacketHandler::FREE_CHANNEL;
      bool bret = (NULL != connection) && (NULL != packet);
      if (bret)
      {
        TBSYS_LOG(DEBUG, "receive pcode : %d", packet->getPCode());
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
          int32_t ret = common::TFS_ERROR;
          if (GFactory::get_runtime_info().is_master())
            ret = do_master_msg_helper(bpacket);
          else
            ret = do_slave_msg_helper(bpacket);
          if (common::TFS_SUCCESS == ret)
          {
            hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            switch (pcode)
            {
            case SET_DATASERVER_MESSAGE:
            case REQ_REPORT_BLOCKS_TO_NS_MESSAGE:
              heart_manager_.push(bpacket);
              break;
            case MASTER_AND_SLAVE_HEART_MESSAGE:
            case HEARTBEAT_AND_NS_HEART_MESSAGE:
              master_slave_heart_manager_.push(bpacket, 0, false);
              break;
            case OPLOG_SYNC_MESSAGE:
              layout_manager_.get_oplog_sync_mgr().push(bpacket, 0, false);
              break;
            default:
              if (!main_workers_.push(bpacket, work_queue_size_, false))
              {
                hret = tbnet::IPacketHandler::FREE_CHANNEL;
                bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
                bpacket->free();
              }
              break;
            }
          }
          else
          {
            bpacket->free();
            GFactory::get_runtime_info().dump(TBSYS_LOG_LEVEL_WARN);
            TBSYS_LOG(WARN, "the msg: %d will be ignored", pcode);
          }
        }
      }
      return hret;
    }

    /** handle packet*/
    bool NameServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t ret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          //TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case GET_BLOCK_INFO_MESSAGE:
              ret = open(msg);
              break;
            case BATCH_GET_BLOCK_INFO_MESSAGE:
              ret = batch_open(msg);
              break;
            case BLOCK_WRITE_COMPLETE_MESSAGE:
              ret = close(msg);
              break;
            case REPLICATE_BLOCK_MESSAGE:
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
            case REMOVE_BLOCK_RESPONSE_MESSAGE:
              ret = layout_manager_.get_client_request_server().handle(msg);
              break;
            case UPDATE_BLOCK_INFO_MESSAGE:
              ret = update_block_info(msg);
              break;
            case SHOW_SERVER_INFORMATION_MESSAGE:
              ret = show_server_information(msg);
              break;
            /*case OWNER_CHECK_MESSAGE:
              ret = owner_check(msg);
              break;*/
            case STATUS_MESSAGE:
              ret = ping(msg);
              break;
            case DUMP_PLAN_MESSAGE:
              ret = dump_plan(msg);
              break;
            case CLIENT_CMD_MESSAGE:
              ret = client_control_cmd(msg);
              break;
            default:
              ret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != ret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed, pcode: %d", pcode);
          }
        }
      }
      return bret;
    }

    int NameServer::callback(common::NewClient* client)
    {
      int32_t ret = (NULL != client) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
        ret = ((NULL != sresponse) && (fresponse != NULL)) ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
        if (TFS_SUCCESS == ret)
        {
          tbnet::Packet* packet = client->get_source_msg();
          assert(NULL != packet);
          int32_t pcode = packet->getPCode();
          if (REMOVE_BLOCK_MESSAGE == pcode)
          {
            if (!sresponse->empty())
            {
              NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
              for (; iter != sresponse->end(); ++iter)
              {
                if (iter->second.second->getPCode() == STATUS_MESSAGE)
                {
                  RemoveBlockMessage* msg = dynamic_cast<RemoveBlockMessage*>(packet);
                  StatusMessage* sm = dynamic_cast<StatusMessage*>(iter->second.second);
                  TBSYS_LOG(INFO, "remove block: %u %s", msg->get(),
                    STATUS_MESSAGE_OK == sm->get_status() ? "successful" : "failure");
                }
                //layout_manager_.get_client_request_server().handle(dynamic_cast<BasePacket*>(iter->second.second));
              }
           }
          }
        }
      }
      return ret;
    }

    int NameServer::open(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == GET_BLOCK_INFO_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        GetBlockInfoMessage* message = dynamic_cast<GetBlockInfoMessage*> (msg);
        SetBlockInfoMessage* result_msg = new SetBlockInfoMessage();
        uint32_t block_id = message->get_block_id();
        uint32_t lease_id = common::INVALID_LEASE_ID;
        int32_t  mode     = message->get_mode();
        int32_t  version  = 0;
        time_t now = Func::get_monotonic_time();
        uint64_t ipport = msg->get_connection()->getServerId();

        ret = layout_manager_.get_client_request_server().open(block_id, lease_id, version, result_msg->get_block_ds(), mode, now);
        if (TFS_SUCCESS == ret)
        {
          result_msg->set(block_id, version, lease_id);
          ret = message->reply(result_msg);
        }
        else
        {
          result_msg->free();
          if(EXIT_NO_DATASERVER == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NO_DATASERVER,
                  "got error, when get block: %u mode: %d, result: %d information, %s",
                  block_id, mode, ret, tbsys::CNetUtil::addrToString(ipport).c_str());
          }
          else if (EXIT_ACCESS_PERMISSION_ERROR == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NAMESERVER_ONLY_READ,
                  "current nameserver only read, %s", tbsys::CNetUtil::addrToString(ipport).c_str());
          }
          else
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
                  "got error, when get block: %u mode: %d, result: %d information, %s",
                  block_id, mode, ret,tbsys::CNetUtil::addrToString(ipport).c_str());
          }
        }
      }
      return ret;
    }

    /**
     * a write operation completed, commit to nameserver and update block's verion
     */
    int NameServer::close(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == BLOCK_WRITE_COMPLETE_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        BlockWriteCompleteMessage* message = dynamic_cast<BlockWriteCompleteMessage*> (msg);
        CloseParameter param;
        memset(&param, 0, sizeof(param));
        param.need_new_ = false;
        param.block_info_ = (*message->get_block());
        param.id_ = message->get_server_id();
        param.lease_id_ = message->get_lease_id();
        param.status_ = message->get_success();
        param.unlink_flag_ = message->get_unlink_flag();
        ret = layout_manager_.get_client_request_server().close(param);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "%s, ret: %d", param.error_msg_, ret);
        }
        TBSYS_LOG(DEBUG, "close, block: %u, server: %s, status: %d, lease_id: %u, ret: %d",
          param.block_info_.block_id_, tbsys::CNetUtil::addrToString(param.id_).c_str(), param.status_, param.lease_id_, ret);
        ret = message->reply(new StatusMessage(ret, param.error_msg_));
      }
      return ret;
    }

    int NameServer::batch_open(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == BATCH_GET_BLOCK_INFO_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BatchGetBlockInfoMessage* message = dynamic_cast<BatchGetBlockInfoMessage*>(msg);
        int32_t block_count = message->get_block_count();
        int32_t  mode     = message->get_mode();
        common::VUINT32& blocks = message->get_block_id();
        BatchSetBlockInfoMessage* reply = new (std::nothrow)BatchSetBlockInfoMessage();
        assert(NULL != reply);
        ret = layout_manager_.get_client_request_server().batch_open(blocks, mode, block_count, reply->get_infos());
        if (TFS_SUCCESS == ret)
        {
          ret = message->reply(reply);
        }
        else
        {
          reply->free();
          if(EXIT_NO_DATASERVER == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NO_DATASERVER,
                "not found dataserver, dataserver size equal 0");
          }
          else if (EXIT_ACCESS_PERMISSION_ERROR == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NAMESERVER_ONLY_READ,
                "current nameserver only read");
          }
          else
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
                "batch get get block information error, mode: %d, ret: %d", mode, ret);
          }
        }
      }
      return ret;
    }

    int NameServer::update_block_info(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == UPDATE_BLOCK_INFO_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        UpdateBlockInfoMessage* message = dynamic_cast<UpdateBlockInfoMessage*>(msg);
        uint32_t block = message->get_block_id();
        if (0 == block)
        {
          ret = msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_BLOCK_NOT_FOUND,
              "repair block: %u, block object not found", block);
        }
        else
        {
          time_t now = Func::get_monotonic_time();
          int32_t repair_flag = message->get_repair();
          uint64_t dest_server = message->get_server_id();
          if (UPDATE_BLOCK_NORMAL == repair_flag)
          {
            const BlockInfo* info = message->get_block();
            if (NULL == info)
            {
              ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_BLOCK_NOT_FOUND,
                  "repair block: %u, blockinfo object is null", block);
            }
            else
            {
              bool addnew = true;
              ret = layout_manager_.update_block_info(*info, dest_server, now, addnew);
              if (TFS_SUCCESS == ret)
                ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK, "update block info successful"));
              else
                ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
                  "block: %u, new block info version lower than meta, cannot update", block);
            }
          }
          else
          {
            char error_msg[512] = {'\0'};
            ret = layout_manager_.repair(error_msg, 512, block, dest_server, repair_flag, now);
            ret = message->reply(new StatusMessage(ret, error_msg));
          }
        }
      }
      return ret;
    }

    int NameServer::show_server_information(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == SHOW_SERVER_INFORMATION_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(msg);
        ShowServerInformationMessage* resp = new (std::nothrow)ShowServerInformationMessage();
        assert(NULL != resp);
        SSMScanParameter& param = resp->get_param();
        param.addition_param1_ = message->get_param().addition_param1_;
        param.addition_param2_ = message->get_param().addition_param2_;
        param.start_next_position_ = message->get_param().start_next_position_;
        param.should_actual_count_ = message->get_param().should_actual_count_;
        param.child_type_ = message->get_param().child_type_;
        param.type_ = message->get_param().type_;
        param.end_flag_ = message->get_param().end_flag_;
        ret = layout_manager_.scan(param);
        if (TFS_SUCCESS == ret)
          ret = message->reply(resp);
        else
          resp->free();
      }
      return ret;
    }

    /*int NameServer::owner_check(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ngi.last_owner_check_time_ = tbutil::Time::now(tbutil::Time::Monotonic).toMicroSeconds();//us
      }
      return ret;
    }*/

    int NameServer::ping(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == STATUS_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        StatusMessage* stmsg = dynamic_cast<StatusMessage*>(msg);
        if (STATUS_MESSAGE_PING == stmsg->get_status())
        {
          StatusMessage* reply_msg = dynamic_cast<StatusMessage*>(get_packet_factory()->createPacket(STATUS_MESSAGE));
          reply_msg->set_message(STATUS_MESSAGE_PING);
          ret = msg->reply(reply_msg);
        }
        else
        {
          ret = EXIT_GENERAL_ERROR;
        }
      }
      return ret;
   }

    int NameServer::dump_plan(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == DUMP_PLAN_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DumpPlanResponseMessage* rmsg = new DumpPlanResponseMessage();
        layout_manager_.get_client_request_server().dump_plan(rmsg->get_data());
        ret = msg->reply(rmsg);
      }
      return ret;
    }

    int NameServer::client_control_cmd(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == CLIENT_CMD_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char buf[256] = {'\0'};
        ClientCmdMessage* message = dynamic_cast<ClientCmdMessage*>(msg);
        ret = layout_manager_.get_client_request_server().handle_control_cmd(message->get_cmd_info(), msg, 256, buf);
        if (TFS_SUCCESS == ret)
          ret = msg->reply(new StatusMessage(STATUS_MESSAGE_OK, buf));
        else
          ret = msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), STATUS_MESSAGE_ERROR, buf);
      }
      return ret;
    }

    int NameServer::initialize_ns_global_info()
    {
      int32_t ret = GFactory::initialize(get_timer());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "%s", "GFactory initialize error, must be exit");
      }
      else
      {
        const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_IP_ADDR_LIST);
        ret = NULL == ns_ip ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "%s", "initialize ns ip is null or ns port <= 0, must be exit");
        }

        if (TFS_SUCCESS == ret)
        {
          std::vector < uint32_t > ns_ip_list;
          char buffer[256];
          strncpy(buffer, ns_ip, 256);
          char *t = NULL;
          char *s = buffer;
          while ((t = strsep(&s, "|")) != NULL)
          {
            ns_ip_list.push_back(tbsys::CNetUtil::getAddr(t));
          }
          ret = 2U != ns_ip_list.size() ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "%s", "must have two ns,check your ns' list");
          }
          else
          {
            bool flag = false;
            uint32_t local_ip = 0;
            std::vector<uint32_t>::iterator iter = ns_ip_list.begin();
            for (; iter != ns_ip_list.end() && !flag; ++iter)
            {
              if ((flag = Func::is_local_addr((*iter))))
                local_ip = (*iter);
            }
            ret = flag ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "ip list: %s not in %s, must be exit", ns_ip, get_dev());
            }
            else
            {
              NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
              ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
              ngi.peer_status_ = NS_STATUS_UNINITIALIZE;
              ngi.vip_ = Func::get_addr(get_ip_addr());
              for (iter = ns_ip_list.begin();iter != ns_ip_list.end(); ++iter)
              {
                if (local_ip == (*iter))
                  ngi.owner_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
                else
                  ngi.peer_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
              }
              ngi.switch_role(true);
            }
          }
        }
      }
      return ret;
    }

    /*int NameServer::wait_for_ds_report()
    {
      bool complete = false;
      int32_t ret = TFS_SUCCESS;
      tbnet::Packet* ret_msg = NULL;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      MasterAndSlaveHeartMessage master_slave_msg;
      master_slave_msg.set_ip_port(ngi.owner_ip_port_);
      master_slave_msg.set_role(ngi.owner_role_);
      master_slave_msg.set_status(ngi.owner_status_);
      master_slave_msg.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_YES);
      while (!stop_ && !complete && !ngi.is_destroyed())
      {
        ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ngi.peer_ip_port_, client, &master_slave_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          ret = ret_msg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == ret)
          {
            MasterAndSlaveHeartResponseMessage *response = NULL;
            response = dynamic_cast<MasterAndSlaveHeartResponseMessage *> (ret_msg);
            std::vector<uint64_t>& peer_list = response->get_servers();
            std::vector<uint64_t> local_list;
            layout_manager_.get_server_manager().get_alive_servers(local_list);
            TBSYS_LOG(DEBUG, "local size: %zd, peer size: %zd", local_list.size(), peer_list.size());
            complete = peer_list.size() == 0U;
            if (!complete)
            {
              time_t now = Func::get_monotonic_time();
              int32_t percent = (int) (((double) local_list.size() / (double) peer_list.size()) * 100);
              complete = percent > 90 || !ngi.in_safe_mode_time(now);
            }
          }
        }
        NewClientManager::get_instance().destroy_client(client);
        if (!complete)
        {
          usleep(500000); //500ms
        }
      }
      return TFS_SUCCESS;
    }*/

    int NameServer::do_master_msg_helper(common::BasePacket* packet)
    {
      int32_t ret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          //receive all owner check message , master and slave heart message, dataserver heart message
          //if (pcode != OWNER_CHECK_MESSAGE
            if (pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != SET_DATASERVER_MESSAGE
            && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            ret = ngi.owner_status_ < NS_STATUS_INITIALIZED? common::TFS_ERROR : common::TFS_SUCCESS;
          }
        }
      }
      return ret;
    }

    int NameServer::do_slave_msg_helper(common::BasePacket* packet)
    {
      int32_t ret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          //if (pcode != OWNER_CHECK_MESSAGE
            if (pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != SET_DATASERVER_MESSAGE
            && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            if (ngi.owner_status_ < NS_STATUS_INITIALIZED)
            {
              ret = common::TFS_ERROR;
            }
            else
            {
              if (pcode != REPLICATE_BLOCK_MESSAGE
                && pcode != BLOCK_COMPACT_COMPLETE_MESSAGE
                && pcode != OPLOG_SYNC_MESSAGE
                && pcode != GET_BLOCK_INFO_MESSAGE
                && pcode != SET_DATASERVER_MESSAGE
                && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
                && pcode != BATCH_GET_BLOCK_INFO_MESSAGE
                && pcode != SHOW_SERVER_INFORMATION_MESSAGE)
              {
                ret = common::TFS_ERROR;
              }
            }
          }
        }
      }
      return ret;
    }

    int ns_async_callback(common::NewClient* client)
    {
      NameServer* service = dynamic_cast<NameServer*>(BaseMain::instance());
      int32_t ret = (NULL != service) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = service->callback(client);
      }
      return ret;
    }
  } /** nameserver **/
}/** tfs **/

