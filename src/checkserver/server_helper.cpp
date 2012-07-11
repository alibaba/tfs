/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: server_helper.cpp 868 2012-04-13 22:07:38Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include "server_helper.h"
#include "common/status_message.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace checkserver
  {
    ServerStat::ServerStat():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
#ifdef TFS_NS_DEBUG
      total_elect_num_ = 0;
#endif
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }

    ServerStat::~ServerStat()
    {
    }

    int ServerStat::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
#ifdef TFS_NS_DEBUG
      total_elect_num_ = input.readInt64();
#endif
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      current_time_ = input.readInt64();
      status_ = (DataServerLiveStatus)input.readInt32();
      offset += (len - input.getDataLen());

      return TFS_SUCCESS;
    }

    int ServerHelper::get_ds_list(const uint64_t ns_ip, VUINT64& ds_list)
    {
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (100 << 16);  // get 100 ds every turn
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

      if (false == NewClientManager::get_instance().is_init())
      {
        TBSYS_LOG(ERROR, "new client manager not init.");
        return TFS_ERROR;
      }

      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        int ret = send_msg_to_server(ns_ip, client, &msg, ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get server info error, ret: %d", ret);
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }
        if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();

        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        while (data_len > offset)
        {
          ServerStat server;
          if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset))
          {
            ds_list.push_back(server.id_);
            std::string ip_port = Func::addr_to_str(server.id_, true);
            TBSYS_LOG(DEBUG, "get dataserver %s", ip_port.c_str());
          }
        }
        param.addition_param1_ = ret_param.addition_param1_;
        param.addition_param2_ = ret_param.addition_param2_;
        param.end_flag_ = ret_param.end_flag_;
        NewClientManager::get_instance().destroy_client(client);
      }

      return TFS_SUCCESS;
    }

    int ServerHelper::check_ds(const uint64_t ds_id, const uint32_t check_time,
        const uint32_t last_check_time, CheckBlockInfoVec& check_result)
    {
      TBSYS_LOG(DEBUG, "checking dataserver %s", Func::addr_to_str(ds_id, true).c_str());
      int ret =  0;
      if (false == NewClientManager::get_instance().is_init())
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "new client manager not init.");
      }
      else
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        if(NULL == client)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "create client fail, ret: %d", ret);
        }
        else
        {
          CheckBlockRequestMessage req_cb_msg;
          tbnet::Packet* ret_msg = NULL;
          req_cb_msg.set_check_time(check_time);
          req_cb_msg.set_last_check_time(last_check_time);
          ret = send_msg_to_server(ds_id, client, &req_cb_msg, ret_msg);
          if (NULL != ret_msg)
          {
            if (ret_msg->getPCode() == RSP_CHECK_BLOCK_MESSAGE)
            {
              CheckBlockResponseMessage* resp_cb_msg = dynamic_cast<CheckBlockResponseMessage*>(ret_msg);
              check_result = resp_cb_msg->get_result_ref();
            }
            else
            {
              StatusMessage* status_msg = dynamic_cast<StatusMessage*>(ret_msg);
              TBSYS_LOG(ERROR, "%s %s %d", Func::addr_to_str(ds_id, true).c_str(),
                  status_msg->get_error(), status_msg->get_status());
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "dataserver %s is too old or down, ret: %d",
                Func::addr_to_str(ds_id, true).c_str(), ret);
          }
          NewClientManager::get_instance().destroy_client(client);
        }
      }
      return ret;
    }

    int ServerHelper::check_block(const uint64_t ns_id, const uint32_t block_id,
        CheckBlockInfoVec& check_result)
    {
      int ret =  0;
      if (false == NewClientManager::get_instance().is_init())
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "new client manager not init.");
      }
      else
      {
        VUINT64 ds_list;
        ret = get_block_ds_list(ns_id, block_id, ds_list);
        if (TFS_SUCCESS == ret)
        {
          for (uint32_t i = 0; i < ds_list.size(); i++)
          {
            NewClient* client = NewClientManager::get_instance().create_client();
            if(NULL == client)
            {
              ret = TFS_ERROR;
              TBSYS_LOG(ERROR, "create client fail, ret: %d", ret);
            }
            else
            {
              CheckBlockRequestMessage req_cb_msg;
              req_cb_msg.set_block_id(block_id);
              tbnet::Packet* ret_msg = NULL;
              ret = send_msg_to_server(ds_list[i], client, &req_cb_msg, ret_msg);
              if (NULL != ret_msg)
              {
                if (ret_msg->getPCode() == RSP_CHECK_BLOCK_MESSAGE)
                {
                  CheckBlockResponseMessage* resp_cb_msg = dynamic_cast<CheckBlockResponseMessage*>(ret_msg);
                  CheckBlockInfoVec& result = resp_cb_msg->get_result_ref();
                  if (result.size() > 0)
                  {
                    check_result.push_back(result[0]);
                  }
                }
                else
                {
                  StatusMessage* status_msg = dynamic_cast<StatusMessage*>(ret_msg);
                  TBSYS_LOG(ERROR, "%u %s %d", block_id, status_msg->get_error(), status_msg->get_status());
                }
              }
              else
              {
                TBSYS_LOG(WARN, "check block %u, dataserver %s request fail, ret: %d",
                    block_id, Func::addr_to_str(ds_list[i], true).c_str(), ret);
              }
              NewClientManager::get_instance().destroy_client(client);
            }
          }
        }
      }

      return check_result.size() > 0? TFS_SUCCESS: ret;
    }

    int ServerHelper::get_block_ds_list(const uint64_t server_id, const uint32_t block_id, VUINT64& ds_list)
    {
      int ret = TFS_SUCCESS;
      if (false == NewClientManager::get_instance().is_init())
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "client manager not init.");
      }
      else if (0 == server_id)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "server is is invalid: %"PRI64_PREFIX"u", server_id);
      }
      else
      {
        GetBlockInfoMessage gbi_message(common::T_READ);
        gbi_message.set_block_id(block_id);

        tbnet::Packet* rsp = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(server_id, client, &gbi_message, rsp);

        if (rsp != NULL)
        {
          if (rsp->getPCode() == SET_BLOCK_INFO_MESSAGE)
          {
            ds_list = dynamic_cast<SetBlockInfoMessage*>(rsp)->get_block_ds();
          }
          else if (rsp->getPCode() == STATUS_MESSAGE)
          {
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
            TBSYS_LOG(ERROR, "get block info fail, error: %s\n,", dynamic_cast<StatusMessage*>(rsp)->get_error());
            ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "get NULL response message, ret: %d\n", ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }
  }
}


