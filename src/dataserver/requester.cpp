/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: requester.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "requester.h"
#include "message/block_info_message.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;

    int Requester::init(const uint64_t dataserver_id, const uint64_t ns_ip_port, DataManagement* data_management)
    {
      dataserver_id_ = dataserver_id;
      ns_ip_port_ = ns_ip_port;
      data_management_ = data_management;
      return TFS_SUCCESS;
    }

    int Requester::req_update_block_info(const uint32_t block_id, const UpdateBlockType repair)
    {
      UpdateBlockType tmp_repair = repair; 
      BlockInfo* blk = NULL;
      if (UPDATE_BLOCK_MISSING != tmp_repair)
      {
        int32_t visit_count = 0;
        int ret = data_management_->get_block_info(block_id, blk, visit_count);
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          tmp_repair = UPDATE_BLOCK_MISSING;
        }
        else
        {
          if (NULL == blk)
          {
            TBSYS_LOG(ERROR, "blockid: %u can not find block info.", block_id);
            tmp_repair = UPDATE_BLOCK_REPAIR;
          }
          else
          {
            TBSYS_LOG(
                INFO,
                "req update block info, blockid: %u, version: %d, file count: %d, size: %d, delfile count: %d, del_size: %d, seqno: %d\n",
                blk->block_id_, blk->version_, blk->file_count_, blk->size_, blk->del_file_count_, blk->del_size_, blk->seq_no_);
          }
        }
      }

      int ret = TFS_ERROR;
      UpdateBlockInfoMessage ub_msg;
      ub_msg.set_block_id(block_id);
      ub_msg.set_block(blk);
      ub_msg.set_server_id(dataserver_id_);
      ub_msg.set_repair(tmp_repair);
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* return_msg = NULL;
      ret = send_msg_to_server(ns_ip_port_, client, &ub_msg, return_msg);
      if (TFS_SUCCESS != ret)
      {
        NewClientManager::get_instance().destroy_client(client);
        return ret;
      }
      int need_expire = 0;
      if (STATUS_MESSAGE == return_msg->getPCode())
      {
        StatusMessage* sm = dynamic_cast<StatusMessage*>(return_msg);
        if (STATUS_MESSAGE_OK == sm->get_status())
        {
          ret = TFS_SUCCESS;
        }
        else if (STATUS_MESSAGE_REMOVE == sm->get_status())
        {
          need_expire = 1;
          ret = TFS_SUCCESS;
        }
        else
        {
          TBSYS_LOG(ERROR, "req update block info: %s, id: %u, tmp_repair: %d\n", sm->get_error(), block_id, tmp_repair);
        }
      }
      else
      {
        TBSYS_LOG(ERROR,"unknow packet pcode: %d", return_msg->getPCode());
      }
      NewClientManager::get_instance().destroy_client(client);

      if (need_expire)
      {
        data_management_->del_single_block(block_id);
      }

      return ret;
    }

    int Requester::req_block_write_complete(const uint32_t block_id,
        const int32_t lease_id, const int32_t success, const UnlinkFlag unlink_flag)
    {
      TBSYS_LOG(DEBUG, "req block write complete begin id: %u, lease_id: %u\n", block_id, lease_id);

      BlockInfo* blk = NULL;
      int visit_count = 0;
      int ret = data_management_->get_block_info(block_id, blk, visit_count);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "req block write complete: can not find block, id: %u, ret: %d\n", block_id, ret);
        return TFS_ERROR;
      }

      BlockInfo tmpblk;
      memcpy(&tmpblk, blk, sizeof(BlockInfo));
      ret = data_management_->get_block_curr_size(block_id, tmpblk.size_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "req block write complete: can not find block, id: %u, ret: %d\n", block_id, ret);
        return TFS_ERROR;
      }

      BlockWriteCompleteMessage bwc_msg;
      bwc_msg.set_block(&tmpblk);
      bwc_msg.set_server_id(dataserver_id_);
      bwc_msg.set_lease_id(lease_id);
      WriteCompleteStatus wc_status = WRITE_COMPLETE_STATUS_YES;
      if (TFS_SUCCESS != success)
      {
        wc_status = WRITE_COMPLETE_STATUS_NO;
      }
      bwc_msg.set_success(wc_status);
      bwc_msg.set_unlink_flag(unlink_flag);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* return_msg = NULL;
      ret = send_msg_to_server(ns_ip_port_, client, &bwc_msg, return_msg);


      if (TFS_SUCCESS != ret)
      {
        NewClientManager::get_instance().destroy_client(client);
        return ret;
      }

      if (STATUS_MESSAGE == return_msg->getPCode())
      {
        StatusMessage* sm = dynamic_cast<StatusMessage*>(return_msg);
        if (STATUS_MESSAGE_OK == sm->get_status())
        {
          ret = TFS_SUCCESS;
        }
        else
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "req block write complete, nsip: %s, error desc: %s, id: %u\n",
              tbsys::CNetUtil::addrToString(ns_ip_port_).c_str(), sm->get_error(), block_id);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "req block write complete, blockid: %u, msg type: %d error.\n", block_id,
            return_msg->getPCode());
        ret = TFS_ERROR;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }
  }
}
