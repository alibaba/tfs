/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "metacmp.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    CmpInfo::CmpInfo()
    {
    }

    CmpInfo::~CmpInfo()
    {
    }
    int CmpInfo::set_ns_ip(const std::string& master_ip_port, const std::string& slave_ip_port)
    {
      master_ns_ip_ = get_addr(master_ip_port);
      slave_ns_ip_ = get_addr(slave_ip_port);

      return TFS_SUCCESS;
    }
    int CmpInfo::process_data(common::SSMScanParameter& param, tbnet::DataBuffer& data, const int8_t role, int32_t& map_count)
    {
      int32_t data_len = data.getDataLen();
      int32_t offset = 0;
      while (data_len > offset)
      {
        if (param.type_ == SSM_TYPE_SERVER)
        {
          ServerCmp server;
          if (TFS_SUCCESS == server.deserialize(data, data_len, offset, param.child_type_))
          {
            //server.dump();
            if (role == Master_Server_Role)
            {
              master_server_map_[server.id_] = server;
              map_count++;
            }
            else if (role == Slave_Server_Role)
            {
              slave_server_map_[server.id_] = server;
            }
          }
        }
        else if (param.type_ == SSM_TYPE_BLOCK)
        {
          BlockCmp block;
          if (TFS_SUCCESS == block.deserialize(data, data_len, offset, param.child_type_))
          {
            //block.dump();
            if (role == Master_Server_Role)
            {
              master_block_map_[block.info_.block_id_] = block;
              map_count++;
            }
            else if (role == Slave_Server_Role)
            {
              slave_block_map_[block.info_.block_id_] = block;
            }
          }
        }
      }
      return TFS_SUCCESS;
    }
    int CmpInfo::init_param(ComType cmp_type, int8_t, int32_t num, SSMScanParameter& param)
    {
      memset(&param, 0, sizeof(SSMScanParameter));
      if (cmp_type & SERVER_TYPE)
      {
        param.type_ = SSM_TYPE_SERVER;
        param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO
                           | SSM_CHILD_SERVER_TYPE_HOLD
                           | SSM_CHILD_SERVER_TYPE_WRITABLE
                           | SSM_CHILD_SERVER_TYPE_MASTER;
        param.start_next_position_ = 0x0;
        param.should_actual_count_= (num << 16);
        param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
      }
      else if (cmp_type & BLOCK_TYPE)
      {
        param.type_ = SSM_TYPE_BLOCK;
        param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;
        param.should_actual_count_ = (num << 16);
        param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
      }
      return TFS_SUCCESS;
    }
    int CmpInfo::get_cmp_map(SSMScanParameter& param, int8_t role, bool& need_loop, int32_t& map_count)
    {
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::DataBuffer data;
      uint64_t ns_ip = (role == Master_Server_Role) ? master_ns_ip_ : slave_ns_ip_;
      ShowServerInformationMessage msg;
      SSMScanParameter& input_param = msg.get_param();
      input_param = param;

      param.data_.clear();
      tbnet::Packet *ret_msg = NULL;
      int ret = -1;
      ret = send_msg_to_server(ns_ip, client, &msg, ret_msg);

      if (TFS_SUCCESS != ret || ret_msg == NULL)
      {
        TBSYS_LOG(ERROR, "get server info error");
        NewClientManager::get_instance().destroy_client(client);
        return TFS_ERROR;
      }
      if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
      {
        TBSYS_LOG(ERROR, "get invalid message type");
        NewClientManager::get_instance().destroy_client(client);
        return TFS_ERROR;
      }
      ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
      SSMScanParameter& ret_param = message->get_param();
      if (ret_param.data_.getDataLen() > 0)
      {
        //TBSYS_LOG(DEBUG, "data len: %d", data.getDataLen());
        process_data(param, ret_param.data_, role, map_count);
        if (param.type_ == SSM_TYPE_SERVER)
        {
          param.addition_param1_ = ret_param.addition_param1_;
          param.addition_param2_ = ret_param.addition_param2_;
          param.end_flag_ = ret_param.end_flag_;
        }
        else if (param.type_ == SSM_TYPE_BLOCK)
        {
          param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
          param.end_flag_ = ret_param.end_flag_;
          if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
          {
            param.addition_param1_ = ret_param.addition_param2_;
          }
        }
        if ((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)
        {
          need_loop = false;
        }
      }
      else
      {
        need_loop = false;
      }

      NewClientManager::get_instance().destroy_client(client);
      return TFS_SUCCESS;
    }
    int CmpInfo::compare(ComType cmp_type, int8_t type, int32_t num)
    {
      StatCmp stat;
      SSMScanParameter master_param, slave_param;
      bool master_loop = true, slave_loop = true;
      int32_t map_count = 0;
      init_param(cmp_type, type, num, master_param);
      init_param(cmp_type, type, num, slave_param);
      print_head(cmp_type, type);
      while (master_loop || slave_loop)
      {
        if (master_loop)
        {
          map_count = 0;
          get_cmp_map(master_param, Master_Server_Role, master_loop, map_count);
          stat.total_count_ += map_count;
        }
        if (slave_loop)
        {
          get_cmp_map(slave_param, Slave_Server_Role, slave_loop, map_count);
        }
        if (cmp_type & SERVER_TYPE)
        {
          do_cmp(master_server_map_, slave_server_map_, stat, type);
        }
        else if (cmp_type & BLOCK_TYPE)
        {
          do_cmp(master_block_map_, slave_block_map_, stat, type);
        }
      }
      if (cmp_type & SERVER_TYPE)
      {
        do_cmp(master_server_map_, slave_server_map_, stat, type, true);
      }
      else if (cmp_type & BLOCK_TYPE)
      {
        do_cmp(master_block_map_, slave_block_map_, stat, type, true);
      }
      stat.print_stat(cmp_type);
      return TFS_SUCCESS;
    }
    void CmpInfo::print_head(ComType cmp_type, const int8_t type) const
    {
      printf("\n\n             #######################################################################################\n");
      printf("                                       MASTER NS STATUS  VS  SLAVE NS STATUS\n");
      printf("            ########################################################################################\n");
      if (cmp_type & BLOCK_TYPE)
      {
        if (type & BLOCK_CMP_PART_INFO)
        {
          printf("\n                                   ======DIFF BLOCK SOME INFO (MasterNS VS SlaveNs)======\n\n");
          printf("BLOCK_ID   VERSION    FILECOUNT      SIZE   COPY\n");
          printf("--------   -------    --------       ----   --------\n");
        }
        if (type & BLOCK_CMP_ALL_INFO)
        {
          printf("\n                                   ======DIFF  BLOCK ALL INFO (MasterNS VS SlaveNs)======\n\n");
          printf("BLOCK_ID   VERSION    FILECOUNT     SIZE   DEL_FILE    DEL_SIZE   SEQ_NO    COPY\n");
          printf("--------   -------    --------      -----  --------    --------   -----     -----------------\n");
        }
        if (type & BLOCK_CMP_SERVER)
        {
          printf("\n                                   ======DIFF BLOCK SERVER LIST (MasterNS VS SlaveNs)======\n\n");
          printf("BLOCK_ID   SERVER_LIST\n");
          printf("---------- -----------------------------------------------------------------------------------------\n");
        }
      }
      if (cmp_type & SERVER_TYPE)
      {
        if (type & SERVER_TYPE_SERVER_INFO)
        {
          printf("\n                                   ======DIFF DATASERVER ALL INFO (MasterNs VS SlaveNs)======\n\n");
          printf("SERVER_ADDR           UCAP     TCAP    BLKCNT LOAD  TOTAL_WRITE   TOTAL_READ     STARTUP_TIME      BLOCK  WBLOCK  MASTER\n");

          printf("--------------------  ---------------  ----- -----  -----------  ------------  ------------------  ----- ------ ------\n");
        }
        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          printf("\n                                   ======DIFF WBLOCK(PWBLOCK) (MasterNs VS SlaveNs)======\n\n");
          printf("SERVER_ADDR           BLOCK\n");
          printf("--------------------- ----------------------------------------------------------------------------------------\n");
        }
        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          printf("\n                                   ======DIFF WBLOCK(PWBLOCK) (MasterNs VS SlaveNs)======\n\n");
          printf("SERVER_ADDR           WRITABLE BLOCK\n");
          printf("--------------------- ----------------------------------------------------------------------------------------\n");
        }
        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          printf("\n                                   ======DIFF WBLOCK(PWBLOCK) (MasterNs VS SlaveNs)======\n\n");
          printf("SERVER_ADDR           MASTER BLOCK\n");
          printf("--------------------- ----------------------------------------------------------------------------------------\n");
        }
      }
    }
  }
}
