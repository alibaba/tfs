/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *   duanfei <duanfei@taobao.com>
 *      -modify interface-2012.03.15
 *
 */
#include <tbsys.h>
#include <tbnetutil.h>
#include "common/status_message.h"
#include "common/base_service.h"
#include "message/client_cmd_message.h"
#include "nameserver.h"
#include "layout_manager.h"

namespace tfs
{
  namespace nameserver
  {
    using namespace common;
    using namespace tbsys;
    using namespace message;
    ClientRequestServer::ClientRequestServer(LayoutManager& manager)
      :ref_count_(0),
      manager_(manager)
    {

    }

    int ClientRequestServer::keepalive(const common::DataServerStatInfo& ds_info, const time_t now)
    {
      int32_t ret = TFS_ERROR;
      DataServerLiveStatus status = ds_info.status_;
      //check dataserver status
      if (DATASERVER_STATUS_DEAD== status)//dataserver dead
      {
        ret = manager_.get_server_manager().remove(ds_info.id_, now);
      }
      else
      {
        bool isnew = false;
        ret = manager_.get_server_manager().add(ds_info, now, isnew);
        if (ret == TFS_SUCCESS)
        {
          ServerCollect* server = manager_.get_server_manager().get(ds_info.id_);
          ret = (NULL == server) ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (isnew) //new dataserver
            {
              TBSYS_LOG(INFO, "dataserver: %s join: use capacity: %" PRI64_PREFIX "u, total capacity: %" PRI64_PREFIX "u",
                  CNetUtil::addrToString(ds_info.id_).c_str(), ds_info.use_capacity_,
                  ds_info.total_capacity_);
            }
            bool rb_expire = false;
            if (server->is_report_block(rb_expire, now, isnew))
            {
              server->set_report_block_status(REPORT_BLOCK_STATUS_IN_REPORT_QUEUE);
              TBSYS_LOG(DEBUG, "%s add report block server, now: %ld, isnew: %d, rb_expire: %d",
                tbsys::CNetUtil::addrToString(server->id()).c_str(), now, isnew, rb_expire);
              manager_.get_server_manager().add_report_block_server(server, now, rb_expire);
            }
          }
        }
      }
      return ret;
    }

    int ClientRequestServer::report_block(const uint64_t server, const time_t now,
        std::set<common::BlockInfo>& blocks)
    {
      int32_t ret = TFS_ERROR;
      ServerCollect* pserver = manager_.get_server_manager().get(server);
      ret = NULL == pserver ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        //update all relations of blocks belongs to it
        ret = manager_.update_relation(pserver, blocks, now);
        ret = TFS_SUCCESS != ret ?  EXIT_UPDATE_RELATION_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          pserver = manager_.get_server_manager().get(server);
          ret = (NULL == pserver) ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            pserver->set_report_block_status(REPORT_BLOCK_STATUS_COMPLETE);
            pserver->set_next_report_block_time(now, random() % 0xFFFFFFF, false);
            manager_.get_server_manager().del_report_block_server(pserver);
          }
        }
      }
      return ret;
    }

    int ClientRequestServer::open(uint32_t& block_id, uint32_t& lease_id, int32_t& version,
        common::VUINT64& servers, const int32_t mode, const time_t now)
    {
      servers.clear();
      int32_t ret = TFS_SUCCESS;
      if (mode & T_READ)//read mode
      {
        ret = open_read_mode_(servers, block_id);
      }
      else//write mode
      {
        ret = GFactory::get_runtime_info().is_master() ? TFS_SUCCESS : EXIT_ACCESS_PERMISSION_ERROR;
        if (TFS_SUCCESS == ret)//master
        {
          //check this block if doing any operations like replicating, moving, compacting...
          if ((block_id > 0)
              && (!(mode & T_NOLEASE)))
          {
            if (manager_.get_task_manager().exist(block_id))
            {
              TBSYS_LOG(INFO, "it's error when we'll get block information in open this block: %u with write mode because block: %u is busy.",
                  block_id,  mode);
              ret = EXIT_BLOCK_BUSY;
            }
          }

          if (TFS_SUCCESS == ret)
          {
            ret = open_write_mode_(block_id, lease_id, version, servers, mode, now);
          }
        }
      }

      std::vector<stat_int_t> stat(4,0);
      mode & T_READ ? ret == TFS_SUCCESS ? stat[0] = 1 : stat[1] = 1
        : ret == TFS_SUCCESS ? stat[2] = 1 : stat[3] = 1;
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return ret;
    }

    int ClientRequestServer::open_read_mode_(common::VUINT64& servers, const uint32_t block) const
    {
      servers.clear();
      return 0 == block ? EXIT_BLOCK_NOT_FOUND :  manager_.get_block_manager().get_servers(servers, block);
    }

    int ClientRequestServer::batch_open(const common::VUINT32& blocks, const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out)
    {
      int32_t ret =  mode & T_READ ? batch_open_read_mode_(out, blocks) : batch_open_write_mode_(out,mode, block_count);
      std::vector<stat_int_t> stat(4, 0);
      if (mode & T_READ)
      {
        if (TFS_SUCCESS == ret)
          stat[0] = out.size();
        else
          stat[1] = __gnu_cxx::abs(out.size() - blocks.size());
      }
      else
      {
        if (TFS_SUCCESS == ret)
          stat[2] = out.size();
        else
          stat[3] = __gnu_cxx::abs(out.size() - block_count);
      }
      GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
      return ret;
    }

    /**
     * Write commit operation, nameserver confirm this write op and update meta info.
     * @param [inout] parameter: information.
     * @return: success or failure
     */
    int ClientRequestServer::close(CloseParameter& parameter)
    {
      int32_t ret = TFS_SUCCESS;
      uint32_t block_id = parameter.block_info_.block_id_;
      if (parameter.unlink_flag_ == UNLINK_FLAG_YES)//unlink file
      {
        //这里暂时先这么做， 后面如果我们将Block每一次写操作(包括删除)都加版本的话
        //这里就可以和普通的写处理方式一样
        BlockCollect* block = manager_.get_block_manager().get(block_id);
        ret = (NULL == block) ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          block->update(parameter.block_info_);
          std::vector<stat_int_t> stat(6,0);
          stat[4] = 0x01;
          GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_, stat);
        }
      }
      else //write file
      {
        if (WRITE_COMPLETE_STATUS_YES !=  parameter.status_)
        {
          TBSYS_LOG(INFO, "close block: %u successful, but cleint write operation error,lease: %u",
              block_id, parameter.lease_id_);
        }
        else
        {
          BlockCollect* block = manager_.get_block_manager().get(block_id);
          ret = (NULL == block) ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)//check version
          {
            ret = block->version() >= parameter.block_info_.version_ ? EXIT_COMMIT_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              snprintf(parameter.error_msg_, 256, "close block: %u failed, version error: %d:%d",
                block_id, block->version(),parameter.block_info_.version_);
            }
          }
          else
          {
            snprintf(parameter.error_msg_, 256, "close block: %u failed, block not exist, ret: %d", block_id, ret);
          }

          if (TFS_SUCCESS == ret)
          {
            //update block information
            ret = manager_.update_block_info(parameter.block_info_, parameter.id_, Func::get_monotonic_time(), false);
            if (TFS_SUCCESS != ret)
            {
              snprintf(parameter.error_msg_,256,"close block: %u successful, but update block information failed, ret: %d", block_id, ret);
            }
          }
        }
      }
      return ret;
    }

    /**
     * client read: get block 's location of DataServerStatInfo
     * client write: get new block and location of DataServerStatInfo
     * @param [in] block_id: query block id, in write mode
     * can be set to zero for assign a new write block id.
     * @param [in] mode: read | write
     * @param [out] lease_id: write transaction id only for write mode.
     * @param [out] ds_list: block location of DataServerStatInfos.
     * @return: success or failure
     */
    int ClientRequestServer::open_write_mode_(uint32_t& block_id, uint32_t& lease_id,
        int32_t& version, VUINT64& servers, const int32_t mode, const time_t now)
    {
      int32_t ret = (mode & T_WRITE) ? TFS_SUCCESS : EXIT_ACCESS_MODE_ERROR;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "access mode: %d error", mode);
      }
      else
      {
        //nameserver assign a new write block
        BlockCollect* block = NULL;
        if (mode & T_CREATE)
        {
          if (0 == block_id)
          {
            //choolse a writable block
            manager_.get_server_manager().choose_writable_block(block);
            ret = (NULL == block) ? EXIT_NO_BLOCK : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
              block_id = block->id();
            else
              TBSYS_LOG(INFO, "%s", "it's failed when choose a write block");
          }
        }

        if ((mode & T_NEWBLK)
            && (NULL == block))
        {
          ret = 0 == block_id ? EXIT_BLOCK_ID_INVALID_ERROR: TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            time_t now = Func::get_monotonic_time();
            NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            ret = ngi.in_discard_newblk_safe_mode_time(now) || is_discard() ? EXIT_DISCARD_NEWBLK_ERROR: TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              /*block =  manager_.get_block_manager().get(block_id);
              ret = NULL != block ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
              if ((TFS_SUCCESS == ret)
                  && (block->get_servers_size() <= 0)
                  && (!block->is_creating())
                  && (block->get_last_update_time() + SYSPARAM_NAMESERVER.replicate_wait_time_ <= now))
              {
                GCObject* pobject = NULL;
                manager_.get_block_manager().remove(pobject,block_id);
                if (NULL != pobject)
                  manager_.get_gc_manager().add(pobject, now);
              }*/
              //create new block by block_id
              ret = manager_.open_helper_create_new_block_by_id(block_id);
              if (TFS_SUCCESS != ret)
                TBSYS_LOG(INFO, "create new block by block id: %u failed", block_id);
            }
          }
        }

        if (TFS_SUCCESS == ret
            || EXIT_DISCARD_NEWBLK_ERROR == ret)
        {
          ret = block_id == 0 ? EXIT_NO_BLOCK : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            block =  manager_.get_block_manager().get(block_id);
            // now check blockcollect object if has any dataserver.
            ret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              version = block->version();
              manager_.get_block_manager().get_servers(servers, block->id());
              ret = !servers.empty() ? TFS_SUCCESS : EXIT_NO_DATASERVER;
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          if (!(mode & T_NOLEASE))
          {
            manager_.get_block_manager().update_block_last_wirte_time(lease_id, block_id, now);
          }
        }
      }
      return ret;
    }
    /**
     * client read: get block 's location of dataserver
     * only choose dataserver with normal load
     * @param [in] block_id: query block id, must be a valid blockid
     * @param [out] ds_list: block location of dataserver.
     * @return: success or failure
     */

    int ClientRequestServer::batch_open_read_mode_(std::map<uint32_t, common::BlockInfoSeg>& out, const common::VUINT32& blocks) const
    {
      std::pair<std::map<uint32_t, common::BlockInfoSeg>::iterator, bool> res;
      VUINT32::const_iterator iter = blocks.begin();
      for (; iter != blocks.end(); ++iter)
      {
        res = out.insert(std::make_pair((*iter), common::BlockInfoSeg()));
        open_read_mode_(res.first->second.ds_, (*iter));
      }
      return TFS_SUCCESS;
    }

    int ClientRequestServer::batch_open_write_mode_(std::map<uint32_t, common::BlockInfoSeg>& out,const int32_t mode, const int32_t block_count)
    {
      int32_t ret =  (mode & T_WRITE) ? TFS_SUCCESS : EXIT_ACCESS_MODE_ERROR;
      if (TFS_SUCCESS == ret)
      {
        time_t now = Func::get_monotonic_time();
        uint32_t block_id = 0;
        BlockInfoSeg seg;
        for (int32_t i = 0; i < block_count; ++i, block_id = 0)
        {
          seg.ds_.clear();
          ret = open_write_mode_(block_id, seg.lease_id_, seg.version_, seg.ds_, mode, now);
          if (TFS_SUCCESS == ret)
            out.insert(std::make_pair(block_id, seg));
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_load_block(const time_t now, const common::ClientCmdInformation& info, common::BasePacket* message, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control load block: %u, server: %s",
          info.value3_, CNetUtil::addrToString(info.value1_).c_str());
      int32_t ret = ((NULL != buf) && (buf_length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        bool new_create_block_collect  = false;
        BlockCollect* block = manager_.get_block_manager().get(info.value3_);
        ret  = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          new_create_block_collect = true;
          block = manager_.get_block_manager().insert(info.value3_, now, true);
        }
        else
        {
          ret = manager_.get_block_manager().get_servers_size(block) <= 0 ? TFS_SUCCESS : EXIT_BLOCK_REPLICATE_EXIST;
        }
        ret  = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          snprintf(buf, buf_length, " block: %u no exist", info.value3_);
        }
        else
        {
          ServerCollect* server = manager_.get_server_manager().get(info.value1_);
          ret = NULL == server ? EIXT_SERVER_OBJECT_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, " server: %s no exist", CNetUtil::addrToString(info.value1_).c_str());
          }
          else
          {
            int32_t status = STATUS_MESSAGE_ERROR;
            ret = send_msg_to_server(info.value1_, message, status);
            if ((STATUS_MESSAGE_OK != status)
                || (TFS_SUCCESS != ret))
            {
              snprintf(buf, buf_length, "send load block: %u  message to server: %s failed",
                  info.value3_, CNetUtil::addrToString(info.value1_).c_str());
            }
            if (TFS_SUCCESS == ret)
            {
              ret = manager_.build_relation(block, server, now, true) ? TFS_SUCCESS : EXIT_BUILD_RELATION_ERROR;
            }
          }
          GCObject* pobject = NULL;
          if (manager_.get_block_manager().get_servers_size(block) <= 0 && new_create_block_collect)
            manager_.get_block_manager().remove(pobject, info.value3_);
          if (NULL != pobject)
            manager_.get_gc_manager().add(pobject, now);
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_delete_block(const time_t now, const common::ClientCmdInformation& info,const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control remove block: %u, flag: %u, server: %s",
          info.value3_, info.value4_, CNetUtil::addrToString(info.value1_).c_str());
      int32_t ret = ((NULL != buf) && (buf_length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        GCObject* pobject = NULL;
        if (info.value4_ & HANDLE_DELETE_BLOCK_FLAG_BOTH)
        {
          std::vector<ServerCollect*> runer;
          ret = manager_.get_block_manager().get_servers(runer, info.value3_);
          if (TFS_SUCCESS != ret)
          {
            manager_.get_block_manager().remove(pobject, info.value3_);
            snprintf(buf, buf_length, " block: %u no exist or dataserver not found, ret: %d", info.value3_, ret);
          }
          else
          {
            BlockCollect* block = manager_.get_block_manager().get(info.value3_);
            ret = NULL == block ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, " block: %u no exist, ret: %d", info.value3_, ret);
            }
            else
            {
              std::vector<ServerCollect*>::const_iterator iter = runer.begin();
              for (; iter != runer.end(); ++iter)
              {
                manager_.relieve_relation(block, (*iter), now,BLOCK_COMPARE_SERVER_BY_ID);
                manager_.get_task_manager().remove_block_from_dataserver((*iter)->id(), info.value3_, 0, now);
              }
              manager_.get_block_manager().remove(pobject, info.value3_);
            }
          }
        }
        else if (info.value4_ & HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION)
        {
          manager_.get_block_manager().remove(pobject, info.value3_);
        }
        else
        {
          BlockCollect* block = manager_.get_block_manager().get(info.value3_);
          ret = NULL != block ? TFS_SUCCESS : EXIT_NO_BLOCK;
          if (TFS_SUCCESS == ret)
          {
            ServerCollect* server = manager_.get_server_manager().get(info.value1_);
            ret = NULL != server ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
            if (TFS_SUCCESS == ret)
              manager_.relieve_relation(block, server, now, BLOCK_COMPARE_SERVER_BY_ID);
            if (manager_.get_block_manager().get_servers_size(block) <= 0)
              manager_.get_block_manager().remove(pobject, info.value3_);
          }
          else
          {
            snprintf(buf, buf_length, " block: %u no exist", info.value3_);
          }
        }
        if (NULL != pobject)
          manager_.get_gc_manager().add(pobject, now);
      }
      return ret;
    }

    int ClientRequestServer::handle_control_compact_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control compact block: %u", info.value3_);
      int32_t ret = ((NULL == buf) || (buf_length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        std::vector<ServerCollect*> runer;
        ret = manager_.get_block_manager().get_servers(runer, info.value3_);
        if (TFS_SUCCESS != ret)
        {
          snprintf(buf, buf_length, " block: %u no exist or dataserver not found, ret: %d", info.value3_, ret);
        }
        else
        {
          ret = manager_.get_task_manager().add(info.value3_, runer, PLAN_TYPE_COMPACT, now);
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, " add task(compact) failed, block: %u", info.value3_);
          }
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_immediately_replicate_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      TBSYS_LOG(INFO, "handle control %s block: %u, source: %s, target: %s",
          REPLICATE_BLOCK_MOVE_FLAG_NO == info.value4_ ? "replicate" : "move", info.value3_,
          CNetUtil::addrToString(info.value1_).c_str(), CNetUtil::addrToString(info.value2_).c_str());
      int32_t ret = ((NULL == buf) || (buf_length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        bool new_create_block_collect = false;
        BlockCollect* block = manager_.get_block_manager().get(info.value3_);
        ret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          ret = 0 == info.value3_ ? EXIT_BLOCK_ID_INVALID_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            new_create_block_collect = true;
            block = manager_.get_block_manager().insert(info.value3_, now, true);
          }
          ret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* source = NULL;
          ServerCollect* target = NULL;
          ServerCollect* servers[SYSPARAM_NAMESERVER.max_replication_ + 1];
          ArrayHelper<ServerCollect*> helper(SYSPARAM_NAMESERVER.max_replication_ + 1, servers);
          manager_.get_block_manager().get_servers(helper, info.value3_);
          if (0 != info.value1_)
            source = manager_.get_server_manager().get(info.value1_);
          else
            manager_.get_server_manager().choose_replicate_source_server(source, helper);
          ret = NULL != source ? TFS_SUCCESS : EXIT_NO_DATASERVER;
          if (TFS_SUCCESS != ret)
          {
            snprintf(buf, buf_length, "immediately %s block: %u fail, cannot found source dataserver",
                info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
          }
          else
          {
            if (!helper.exist(source))
              helper.push_back(source);
            if (0 != info.value2_)
              target = manager_.get_server_manager().get(info.value2_);
            else//这里复制和均衡都采用同样的策略，如果迁移时直接采用迁移策略代价太大
              manager_.get_server_manager().choose_replicate_target_server(target, helper);
            ret = NULL != target ? TFS_SUCCESS : EXIT_NO_DATASERVER;
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, "immediately %s block: %u fail, cannot found target dataserver",
                  info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
            }
          }
          if (TFS_SUCCESS == ret)//NULL != source && NULL != target
          {
            std::vector<ServerCollect*> runer;
            runer.push_back(source);
            runer.push_back(target);
            PlanType type = (info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO) ? PLAN_TYPE_REPLICATE : PLAN_TYPE_MOVE;
            PlanPriority priority = (info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO) ? PLAN_PRIORITY_EMERGENCY : PLAN_PRIORITY_NORMAL;
            ret = manager_.get_task_manager().add(info.value3_, runer, type, now,priority);
            if (TFS_SUCCESS != ret)
            {
              snprintf(buf, buf_length, "add task(%s) failed, block: %u",
                  info.value4_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", info.value3_);
            }
          }
          GCObject* pobject = NULL;
          if (manager_.get_block_manager().get_servers_size(block) <= 0 && new_create_block_collect)
            manager_.get_block_manager().remove(pobject, info.value3_);
          if (NULL != pobject)
            manager_.get_gc_manager().add(pobject, now);
        }
      }
      return ret;
    }

    int ClientRequestServer::handle_control_rotate_log(void)
    {
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      TBSYS_LOGGER.rotateLog(service->get_log_path());
      return TFS_SUCCESS;
    }

    int ClientRequestServer::handle_control_set_runtime_param(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      return manager_.set_runtime_param(info.value3_, info.value4_, buf_length, buf);
    }

    int ClientRequestServer::handle_control_get_balance_percent(const int64_t buf_length, char* buf)
    {
      snprintf(buf, buf_length, "%.6f", SYSPARAM_NAMESERVER.balance_percent_);
      return TFS_SUCCESS;
    }

    int ClientRequestServer::handle_control_set_balance_percent(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      int32_t ret = (info.value3_ > 1 || info.value3_ < 0 || info.value4_ < 0) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        snprintf(buf, buf_length, "parameter is invalid, value3: %d, value4: %d", info.value3_, info.value4_);
      }
      else
      {
        char data[32] = {'\0'};
        snprintf(data, 32, "%d.%06d", info.value3_, info.value4_);
        SYSPARAM_NAMESERVER.balance_percent_ = strtod(data, NULL);
      }
      return ret;
    }

    int ClientRequestServer::handle_control_clear_system_table(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf)
    {
      int32_t ret = (info.value3_ <= 0) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        snprintf(buf, buf_length, "parameter is invalid, value3: %d", info.value3_);
      }
      else
      {
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_TASK)
            manager_.get_task_manager().clear();
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_WRITE_BLOCK)
            manager_.get_block_manager().clear_write_block();
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_REPORT_SERVER)
            manager_.get_server_manager().clear_report_block_server_table();
        if (info.value3_ & CLEAR_SYSTEM_TABLE_FLAG_DELETE_QUEUE)
            manager_.get_block_manager().clear_delete_queue();
      }
      return ret;
    }

    int ClientRequestServer::handle_control_cmd(const ClientCmdInformation& info, common::BasePacket* msg, const int64_t buf_length, char* buf)
    {
      time_t now = Func::get_monotonic_time();
      int32_t ret = TFS_ERROR;
      switch (info.cmd_)
      {
        case CLIENT_CMD_LOADBLK:
          ret = handle_control_load_block(now, info, msg, buf_length, buf);
          break;
        case CLIENT_CMD_EXPBLK:
          ret = handle_control_delete_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_COMPACT:
          ret = handle_control_compact_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_IMMEDIATELY_REPL:  //immediately replicate
          ret = handle_control_immediately_replicate_block(now, info, buf_length, buf);
          break;
        case CLIENT_CMD_REPAIR_GROUP:
          //TODO
          break;
        case CLIENT_CMD_SET_PARAM:
          ret = handle_control_set_runtime_param(info, buf_length, buf);
          break;
        case CLIENT_CMD_ROTATE_LOG:
          ret = handle_control_rotate_log();
          break;
        case CLIENT_CMD_GET_BALANCE_PERCENT:
          ret = handle_control_get_balance_percent(buf_length, buf);
          break;
        case CLIENT_CMD_SET_BALANCE_PERCENT:
          ret = handle_control_set_balance_percent(info, buf_length, buf);
          break;
        case CLIENT_CMD_CLEAR_SYSTEM_TABLE:
          ret = handle_control_clear_system_table(info, buf_length, buf);
          break;
        default:
          snprintf(buf, buf_length, "unknow client cmd: %d", info.cmd_);
          ret = EXIT_UNKNOWN_MSGTYPE;
          break;
      }
      return ret;
    }

    int ClientRequestServer::handle(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t pcode = msg->getPCode();
        switch (pcode)
        {
          case REPLICATE_BLOCK_MESSAGE:
          case BLOCK_COMPACT_COMPLETE_MESSAGE:
          case REMOVE_BLOCK_RESPONSE_MESSAGE:
            ret = manager_.handle_task_complete(msg);
            break;
          default:
            TBSYS_LOG(WARN, "unkonw message PCode = %d", pcode);
            break;
        }
      }
      return ret;
    }

    void ClientRequestServer::dump_plan(tbnet::DataBuffer& output)
    {
      return manager_.get_task_manager().dump(output);
    }

    bool ClientRequestServer::is_discard(void)
    {
      bool ret = SYSPARAM_NAMESERVER.discard_max_count_ > 0;
      if (ret)
      {
        ret = (atomic_inc(&ref_count_) >= static_cast<uint32_t>(SYSPARAM_NAMESERVER.discard_max_count_));
        if (ret)
        {
          atomic_exchange(&ref_count_, 0);
        }
      }
      return ret;
    }
  }/** nameserver **/
}/** tfs **/
