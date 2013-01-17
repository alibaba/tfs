/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task.cpp 2139 2011-03-28 09:15:26Z duanfei $
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
#include <tbsys.h>
#include <bitset>
#include "common/client_manager.h"
#include "message/compact_block_message.h"
#include "common/status_message.h"
#include "message/replicate_block_message.h"
#include "message/block_info_message.h"

#include "task_manager.h"
#include "global_factory.h"
#include "server_collect.h"
#include "block_manager.h"
#include "server_manager.h"
#include "common/array_helper.h"
#include "layout_manager.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    const int8_t CompactTask::INVALID_SERVER_ID = 0;
    const int8_t CompactTask::INVALID_BLOCK_ID = 0;
    Task::Task(TaskManager& manager, const PlanType type,
        const PlanPriority priority, uint32_t block_id,
        const std::vector<ServerCollect*>& runer):
      GCObject(0xFFFFFFFF),
      runer_(runer),
      block_id_(block_id),
      type_(type),
      status_(PLAN_STATUS_NONE),
      priority_(priority),
      seqno_(0),
      manager_(manager)
    {

    }

    bool Task::operator < (const Task& task) const
    {
      return block_id_ < task.block_id_;
    }

    bool Task::need_add_server_to_map() const
    {
      return type_ == PLAN_TYPE_MOVE || type_ == PLAN_TYPE_REPLICATE || PLAN_TYPE_COMPACT;
    }

    bool Task::timeout(const time_t now) const
    {
      return now > last_update_time_;
    }

    void Task::runTimerTask()
    {
      dump(TBSYS_LOG_LEVEL_INFO, "task expired");
      status_ = PLAN_STATUS_TIMEOUT;
    }

    void Task::dump(tbnet::DataBuffer& stream)
    {
      stream.writeInt8(type_);
      stream.writeInt8(status_);
      stream.writeInt8(priority_);
      stream.writeInt32(block_id_);
      stream.writeInt64(last_update_time_);
      stream.writeInt64(seqno_);
      stream.writeInt8(runer_.size());
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter)
      {
        stream.writeInt64((*iter)->id());
      }
    }

    void Task::dump(int32_t level, const char* const format)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        std::string runer;
        std::vector<ServerCollect*>::iterator iter = runer_.begin();
        for (; iter != runer_.end(); ++iter)
        {
          runer += tbsys::CNetUtil::addrToString((*iter)->id());
          runer += "/";
        }
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__, "%s plan seqno: %"PRI64_PREFIX"d, type: %s ,status: %s, priority: %s , block_id: %u, expired_time: %"PRI64_PREFIX"d,runer: %s",
            format == NULL ? "" : format, seqno_,
            type_ == PLAN_TYPE_REPLICATE ? "replicate" : type_ == PLAN_TYPE_MOVE ? "move" : type_ == PLAN_TYPE_COMPACT
            ? "compact" : type_ == PLAN_TYPE_DELETE ? "delete" : "unknow",
            status_ == PLAN_STATUS_BEGIN ? "begin" : status_ == PLAN_STATUS_TIMEOUT ? "timeout" : status_ == PLAN_STATUS_END
            ? "finish" : status_ == PLAN_STATUS_FAILURE ? "failure": "unknow",
            priority_ == PLAN_PRIORITY_NORMAL ? "normal" : priority_ == PLAN_PRIORITY_EMERGENCY ? "emergency": "unknow",
            block_id_, last_update_time_, runer.c_str());
      }
    }

    CompactTask::CompactTask(TaskManager& manager, const PlanPriority priority,
      uint32_t block_id, const std::vector<ServerCollect*>& runer):
      Task(manager, PLAN_TYPE_COMPACT, priority, block_id, runer)
    {
      complete_status_.clear();
      memset(&block_info_, 0, sizeof(block_info_));
    }

    void CompactTask::runTimerTask()
    {
      status_ = PLAN_STATUS_TIMEOUT;
      {
        tbutil::Mutex::Lock lock(mutex_);
        std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
        for (; iter != complete_status_.end(); ++iter)
        {
          std::pair<uint64_t, PlanStatus>& status = (*iter);
          if (status.second != PLAN_STATUS_END
              && (status.second !=  PLAN_STATUS_FAILURE))
          {
            status.second = PLAN_STATUS_TIMEOUT;
          }
        }
      }
      CompactComplete value(INVALID_SERVER_ID, INVALID_SERVER_ID, PLAN_STATUS_NONE);
      value.block_info_ = block_info_;
      VUINT64 servers;
      check_complete(value, servers);

      do_complete(value, servers);
    }

    void CompactTask::dump(tbnet::DataBuffer& stream)
    {
      Task::dump(stream);
      tbutil::Mutex::Lock lock(mutex_);
      stream.writeInt8(complete_status_.size());
      std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
      for (; iter != complete_status_.end(); ++iter)
      {
        stream.writeInt64((*iter).first);
        stream.writeInt8((*iter).second);
      }
    }

    void CompactTask::dump(const int32_t level, const char* const format)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        std::string runer;
        std::vector<ServerCollect*>::iterator iter = runer_.begin();
        for (; iter != runer_.end(); ++iter)
        {
          runer += tbsys::CNetUtil::addrToString((*iter)->id());
          runer += "/";
        }
        PlanStatus plan_status = PLAN_STATUS_NONE;
        std::string status;

        {
          tbutil::Mutex::Lock lock(mutex_);
          std::vector< std::pair <uint64_t, PlanStatus> >::iterator it= complete_status_.begin();
          for (; it != complete_status_.end(); ++it)
          {
            status += tbsys::CNetUtil::addrToString((*it).first);
            status += ":";
            plan_status = (*it).second;
            status += plan_status == PLAN_STATUS_BEGIN ? "begin" : plan_status == PLAN_STATUS_TIMEOUT ? "timeout" : plan_status == PLAN_STATUS_END
              ? "finish" : plan_status == PLAN_STATUS_FAILURE ? "failure": "unknow",
              status += "/";
          }
        }
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__, "pointer: %p, %s plan seqno: %"PRI64_PREFIX"d, type: %s ,status: %s, priority: %s , block_id: %u, expired_time: %"PRI64_PREFIX"d, runer: %s, complete status: %s",
            this,
            format == NULL ? "" : format,
            seqno_,
            type_ == PLAN_TYPE_REPLICATE ? "replicate" : type_ == PLAN_TYPE_MOVE ? "move" : type_ == PLAN_TYPE_COMPACT
            ? "compact" : type_ == PLAN_TYPE_DELETE ? "delete" : "unknow",
            status_ == PLAN_STATUS_BEGIN ? "begin" : status_ == PLAN_STATUS_TIMEOUT ? "timeout" : status_ == PLAN_STATUS_END
            ? "finish" : status_ == PLAN_STATUS_FAILURE ? "failure" : "unknow",
            priority_ == PLAN_PRIORITY_NORMAL ? "normal" : priority_ == PLAN_PRIORITY_EMERGENCY ? "emergency": "unknow",
            block_id_, last_update_time_, runer.c_str(), status.c_str());
      }
    }

    int CompactTask::handle()
    {
      int32_t ret = TFS_SUCCESS;
      CompactBlockMessage msg;
      msg.set_block_id(block_id_);
      msg.set_seqno(seqno_);
      msg.set_preserve_time(SYSPARAM_NAMESERVER.task_expired_time_);
      std::pair<uint64_t, PlanStatus> res;
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (int32_t index = 0; iter != runer_.end(); ++iter, ++index)
      {
        res.first = (*iter)->id();
        res.second = PLAN_STATUS_BEGIN;
        msg.set_owner(index == 0 ? 1 : 0);
        int32_t status = STATUS_MESSAGE_ERROR;
        #ifdef TFS_GTEST
          ret = TFS_SUCCESS;
          status = STATUS_MESSAGE_OK;
        #else
          ret = send_msg_to_server(res.first, &msg, status);
        #endif

        if ((TFS_SUCCESS != ret)
          || (STATUS_MESSAGE_OK != status))
        {
          res.second = PLAN_STATUS_TIMEOUT;
          TBSYS_LOG(INFO, "send compact message failed; block : %u owner: %d to server: %s, ret: %d",
              block_id_, index == 0 ? 1 : 0, tbsys::CNetUtil::addrToString(res.first).c_str(), ret);
        }
        else
        {
          TBSYS_LOG(INFO, "send compact message successful; block : %u owner: %d to server: %s, ret: %d",
              block_id_, index == 0 ? 1 : 0, tbsys::CNetUtil::addrToString(res.first).c_str(), ret);
        }
        tbutil::Mutex::Lock lock(mutex_);
        complete_status_.push_back(res);
      }
      status_ = PLAN_STATUS_BEGIN;
      last_update_time_ = Func::get_monotonic_time() +  SYSPARAM_NAMESERVER.task_expired_time_;
      return ret;
    }

    int CompactTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      int32_t ret = (NULL != msg) && (msg->getPCode() == BLOCK_COMPACT_COMPLETE_MESSAGE) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        CompactBlockCompleteMessage* message = dynamic_cast<CompactBlockCompleteMessage*>(msg);
        PlanStatus status = status_transform_compact_to_plan(static_cast<CompactStatus>(message->get_success()));
        CompactComplete value(message->get_server_id(), message->get_block_id(), status);
        value.block_info_ = message->get_block_info();
        VUINT64 servers;
        if (GFactory::get_runtime_info().is_master())//master handle
        {
          check_complete(value, servers);
          ret = do_complete(value, servers);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "block: %u compact, do compact complete fail: %d", value.block_id_, ret);
          }
          ret = message->reply(new StatusMessage(ret));
          all_complete_flag = value.is_complete_;
          if (all_complete_flag)
            status_ = PLAN_STATUS_END;
        }
        else
        {
          //slave
          std::bitset < 3 > bset(message->get_flag());
          value.all_success_ = bset[0];
          value.has_success_ = bset[1];
          value.is_complete_ = bset[2];
          TBSYS_LOG(DEBUG, "check compact complete flag: %u", message->get_flag());
          servers.clear();
          servers.assign(message->get_ds_list().begin(), message->get_ds_list().end());
          ret = do_complete(value, servers);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "block: %u compact, do compact complete fail: %d", value.block_id_, ret);
          }
        }
      }
      return ret;
    }

    void CompactTask::check_complete(CompactComplete& value, std::vector<uint64_t> & servers)
    {
      int32_t complete_count = 0;
      int32_t success_count = 0;

      tbutil::Mutex::Lock lock(mutex_);
      std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
      for (; iter != complete_status_.end(); ++iter)
      {
        std::pair<uint64_t, PlanStatus>& status = (*iter);
        if (status.first == value.id_)
        {
          status.second = value.status_;
          if (value.status_ == PLAN_STATUS_END)
          {
            block_info_ = value.block_info_;
            value.current_complete_result_ = true;
            ++success_count;
            ++complete_count;
          }
          else
          {
            ++complete_count;
            servers.push_back(status.first);
          }
        }
        else
        {
          if (status.second == PLAN_STATUS_END)
          {
            ++complete_count;
            ++success_count;
          }
          else if (status.second != PLAN_STATUS_BEGIN)
          {
            ++complete_count;
            if (status.second == PLAN_STATUS_FAILURE)
              servers.push_back(status.first);
          }
        }
      }

      TBSYS_LOG(DEBUG, "complete_count: %d, success_count: %d, complete_status size: %zd",
          complete_count, success_count, complete_status_.size());

      value.is_complete_ = complete_count == static_cast<int32_t>(complete_status_.size());
      value.has_success_ = success_count != 0;
      value.all_success_ = success_count ==  static_cast<int32_t>(complete_status_.size());
    }

    int CompactTask::do_complete(CompactComplete& value, common::VUINT64& servers)
    {
      if (value.current_complete_result_)
      {
        BlockCollect* block = manager_.get_manager().get_block_manager().get(value.block_id_);
        if (NULL != block)
        {
          block->update(block_info_);
          TBSYS_LOG(DEBUG,"check compacting complete server: %s, block: %u,copy blockinfo into metadata, block size: %d",
              tbsys::CNetUtil::addrToString(value.id_).c_str(), value.block_id_, block_info_.size_);
        }
      }

      time_t now =Func::get_monotonic_time();
      if (value.is_complete_
          && value.has_success_
          && !servers.empty())
      {
        //expire block on this failed servers.
        std::vector<uint64_t>::iterator iter = servers.begin();
        for (; iter != servers.end(); ++iter)
        {
          ServerCollect* server = manager_.get_manager().get_server_manager().get((*iter));
          BlockCollect* block = manager_.get_manager().get_block_manager().get(value.block_id_);
          if((server != NULL) && (block != NULL))
          {
            if (!manager_.get_manager().relieve_relation(block, server, now, BLOCK_COMPARE_SERVER_BY_ID))
            {
              TBSYS_LOG(INFO, "we'll get failed when relive relation between block: %u and server: %s",
                  value.block_id_, tbsys::CNetUtil::addrToString((*iter)).c_str());
            }
            if ( GFactory::get_runtime_info().is_master())
            {
              /*std::vector<stat_int_t> stat(1, 1);
              GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);*/
              manager_.get_manager().get_task_manager().remove_block_from_dataserver((*iter), value.block_id_, 0, now);
            }
          }
        }
      }

      if ((value.is_complete_)
          && (value.all_success_))
      {
        // rewritable block
        ServerCollect* servers[MAX_REPLICATION];
        ArrayHelper<ServerCollect*> helper(MAX_REPLICATION, servers);
        BlockCollect* block = manager_.get_manager().get_block_manager().get(value.block_id_);
        if (NULL != block)
        {
          manager_.get_manager().get_block_manager().get_servers(helper, block);
          for (int8_t i = 0; i < helper.get_array_index(); ++i)
          {
            ServerCollect* server = *helper.at(i);
            assert(NULL != server);
            server->add_writable(block);
          }
        }
      }

      if (((value.is_complete_)
            || (value.has_success_))
          && (GFactory::get_runtime_info().is_master()))
      {
        //transmit message.
        CompactBlockCompleteMessage msg;
        msg.set_block_id(value.block_id_);
        msg.set_server_id(value.id_);
        msg.set_block_info(block_info_);
        msg.set_success(status_transform_plan_to_compact(value.status_));
        msg.set_ds_list(servers);

        std::bitset < 3 > bset;
        bset[0] = value.all_success_;
        bset[1] = value.has_success_;
        bset[2] = value.is_complete_;
        msg.set_flag(bset.to_ulong());
        msg.set_seqno(seqno_);
        TBSYS_LOG(DEBUG, "check compact complete flag: %d", msg.get_flag());

        common::Stream stream(msg.length());
        if (TFS_SUCCESS != msg.serialize(stream))
        {
          TBSYS_LOG(INFO, "%s", "compact msg serialize error");
        }
        else
        {
          int32_t ret = manager_.get_manager().get_oplog_sync_mgr().log(
                OPLOG_TYPE_COMPACT_MSG, stream.get_data(), stream.get_data_length(), now);
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(INFO, "write oplog failed, ret: %d", ret);
        }
      }
      return TFS_SUCCESS;
    }

    CompactStatus CompactTask::status_transform_plan_to_compact(const PlanStatus status)
    {
      return status == PLAN_STATUS_END ? COMPACT_STATUS_SUCCESS :
             status == PLAN_STATUS_BEGIN ? COMPACT_STATUS_START : COMPACT_STATUS_FAILED;
    }

    PlanStatus CompactTask::status_transform_compact_to_plan(const CompactStatus status)
    {
      return status == COMPACT_STATUS_SUCCESS ? PLAN_STATUS_END :
             status == COMPACT_STATUS_START ? PLAN_STATUS_BEGIN :
             status == COMPACT_STATUS_FAILED ? PLAN_STATUS_FAILURE : PLAN_STATUS_NONE;
    }

    ReplicateTask::ReplicateTask(TaskManager& manager, const common::PlanPriority priority,
      const uint32_t block_id, const std::vector<ServerCollect*>& runer):
      Task(manager, PLAN_TYPE_REPLICATE, priority, block_id, runer),
      flag_(REPLICATE_BLOCK_MOVE_FLAG_NO)
    {

    }

    int ReplicateTask::handle()
    {
      int32_t ret = runer_.size() >= 0x2U ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "task (replicate) block: %u, type: %d, priority: %d, runer size: %zd is invalid", block_id_, type_, priority_, runer_.size());
      }
      else
      {
        ReplicateBlockMessage msg;
        ReplBlock block;
        memset(&block, 0, sizeof(block));
        block.block_id_ = block_id_;
        block.source_id_ = runer_[0]->id();
        block.destination_id_ = runer_[1]->id();
        block.start_time_ = Func::get_monotonic_time();
        block.is_move_ = flag_;
        block.server_count_ = 0;
        msg.set_repl_block(&block);
        msg.set_command(PLAN_STATUS_BEGIN);
        msg.set_seqno(seqno_);
        int32_t status = STATUS_MESSAGE_ERROR;
        ret = send_msg_to_server(block.source_id_, &msg, status);
        if (TFS_SUCCESS != ret
          || STATUS_MESSAGE_OK != status)
        {
          TBSYS_LOG(WARN, "send %s command faild, block: %u, ret: %d %s===>%s",
              flag_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
              block_id_, ret, tbsys::CNetUtil::addrToString(block.source_id_).c_str(),
              tbsys::CNetUtil::addrToString(block.destination_id_).c_str());
          ret = TFS_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "send %s command successful, block: %u, : %s===>: %s",
              flag_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
              block_id_, tbsys::CNetUtil::addrToString(block.source_id_).c_str(),
              tbsys::CNetUtil::addrToString(block.destination_id_).c_str());

          status_ = PLAN_STATUS_BEGIN;
          last_update_time_ = Func::get_monotonic_time() +  SYSPARAM_NAMESERVER.task_expired_time_;
        }
      }
      return ret;
    }

    int ReplicateTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      int32_t ret = (NULL != msg) ? STATUS_MESSAGE_OK : STATUS_MESSAGE_REMOVE;
      if (STATUS_MESSAGE_OK == ret)
      {
        ret = (msg->getPCode() == REPLICATE_BLOCK_MESSAGE) ? STATUS_MESSAGE_OK : TFS_ERROR;
        if (STATUS_MESSAGE_OK == ret)
        {
          time_t now = Func::get_monotonic_time();
          ReplicateBlockMessage* message = dynamic_cast<ReplicateBlockMessage*>(msg);
          const ReplBlock blocks = *message->get_repl_block();
          ret = message->get_command() == PLAN_STATUS_END ? TFS_SUCCESS : EXIT_MOVE_OR_REPLICATE_ERROR;
          TBSYS_LOG(DEBUG, "block: %u %s complete status: %s", blocks.block_id_,
              blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate",
              message->get_command() == PLAN_STATUS_END ? "end" :
              message->get_command() == PLAN_STATUS_TIMEOUT ? "timeout" :
              message->get_command() == PLAN_STATUS_BEGIN ? "begin" :
              message->get_command() == PLAN_STATUS_FAILURE ? "failure" : "unknow");
          if (TFS_SUCCESS == ret)
          {
            ServerCollect* dest   = manager_.get_manager().get_server_manager().get(blocks.destination_id_);// find destination dataserver
            ServerCollect* source = manager_.get_manager().get_server_manager().get(blocks.source_id_);// find source dataserver
            BlockCollect* block = manager_.get_manager().get_block_manager().get(blocks.block_id_);
            if ((NULL != block) && (NULL != dest))
            {
              if (blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES)
              {
                bool result = false;
                if ((NULL != source) && block->exist(source, false))
                  result = manager_.get_manager().relieve_relation(block, source, now,BLOCK_COMPARE_SERVER_BY_ID);
                manager_.get_manager().build_relation(block, dest, now);
                ret = manager_.get_manager().get_block_manager().get_servers_size(block) > 0 && result ?  STATUS_MESSAGE_REMOVE : STATUS_MESSAGE_OK;
                if ((manager_.get_manager().get_block_manager().get_servers_size(block) <= 0) && (NULL != source))
                {
                  manager_.get_manager().build_relation(block, source, now);
                }
              }
              else
              {
                //build relation between block and dest dataserver
                manager_.get_manager().build_relation(block, dest, now);
              }
            }

            if (GFactory::get_runtime_info().is_master())
            {
              Stream stream(message->length());
              if (TFS_SUCCESS != message->serialize(stream))
              {
                TBSYS_LOG(INFO, "%s complete msg serialize error", blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate");
              }
              else
              {
                manager_.get_manager().get_oplog_sync_mgr().log(OPLOG_TYPE_REPLICATE_MSG,
                  stream.get_data(), stream.get_data_length(), now);
              }
              message->reply(new StatusMessage(ret));
            }
          }
          else
          {
            TBSYS_LOG(WARN, "block: %u %s complete status: %s", blocks.block_id_,
                blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate",
                message->get_command() == PLAN_STATUS_END ? "end" :
                message->get_command() == PLAN_STATUS_TIMEOUT ? "timeout" :
                message->get_command() == PLAN_STATUS_BEGIN ? "begin" :
                message->get_command() == PLAN_STATUS_FAILURE ? "failure" : "unknow");
            if (GFactory::get_runtime_info().is_master())
              message->reply(new StatusMessage(STATUS_MESSAGE_OK));
          }
          all_complete_flag = true;
          status_ = PLAN_STATUS_END;
        }
      }
      return (ret == STATUS_MESSAGE_OK || ret == STATUS_MESSAGE_REMOVE) ? TFS_SUCCESS : ret;
    }

    DeleteTask::DeleteTask(TaskManager& manager,const PlanPriority priority,
      const uint32_t block_id, const std::vector<ServerCollect*> & runer):
      Task(manager, PLAN_TYPE_DELETE, priority, block_id, runer)
    {

    }

    int DeleteTask::handle()
    {
      time_t now = Func::get_monotonic_time();
      BlockCollect* block = manager_.get_manager().get_block_manager().get(block_id_);
      int32_t ret = NULL != block ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        std::vector<ServerCollect*>::iterator iter = runer_.begin();
        for (; iter != runer_.end(); ++iter)
        {
          manager_.get_manager().relieve_relation(block, (*iter), now, BLOCK_COMPARE_SERVER_BY_ID);
          ret = manager_.get_manager().get_task_manager().remove_block_from_dataserver((*iter)->id(), block_id_, seqno_, now);
          TBSYS_LOG(INFO, "send remove block: %u command on server : %s %s",
            block_id_, tbsys::CNetUtil::addrToString((*iter)->id()).c_str(), TFS_SUCCESS == ret ? "successful" : "failed");
        }

        /*std::vector<stat_int_t> stat(1, runer_.size());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);*/

        status_ = PLAN_STATUS_BEGIN;
        last_update_time_ = now +  SYSPARAM_NAMESERVER.task_expired_time_;
      }
      return ret;
    }

    int DeleteTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      int32_t ret =((NULL != msg) && (msg->getPCode() ==  REMOVE_BLOCK_RESPONSE_MESSAGE))
                    ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RemoveBlockResponseMessage* message = dynamic_cast<RemoveBlockResponseMessage*>(msg);
        all_complete_flag = true;
        status_ = PLAN_STATUS_END;
        TBSYS_LOG(INFO, "block: %u remove complete status end", message->get());
      }
      return ret;
    }

    MoveTask::MoveTask(TaskManager& manager, const common::PlanPriority priority,
      const uint32_t block_id, const std::vector<ServerCollect*>& runer):
      ReplicateTask(manager, priority, block_id, runer)
    {
      type_ = PLAN_TYPE_MOVE;
      flag_ = REPLICATE_BLOCK_MOVE_FLAG_YES;
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/

