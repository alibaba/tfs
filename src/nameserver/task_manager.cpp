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
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include "task_manager.h"
#include "server_collect.h"
#include "layout_manager.h"
#include "common/client_manager.h"
#include "message/block_info_message.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    int TaskManager::Machine::add(const uint64_t server, Task* task, const bool target)
    {
      std::pair<SERVER_TO_TASK_ITER, bool> res =
        target ? target_server_to_task_.insert(SERVER_TO_TASK::value_type(server, task))
               : source_server_to_task_.insert(SERVER_TO_TASK::value_type(server, task));
      return !res.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
    }

    int TaskManager::Machine::remove(const uint64_t server, const bool target)
    {
      int32_t ret = EXIT_TASK_NO_EXIST_ERROR;
      SERVER_TO_TASK_ITER iter;
      if (target)
      {
        iter = target_server_to_task_.find(server);
        ret = target_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
          target_server_to_task_.erase(iter);
      }
      else
      {
        iter = source_server_to_task_.find(server);
        ret = source_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
          source_server_to_task_.erase(iter);
      }
      return ret;
    }

    Task* TaskManager::Machine::get(const uint64_t server) const
    {
      Task* task = NULL;
      SERVER_TO_TASK_CONST_ITER iter = source_server_to_task_.find(server);
      int32_t ret = source_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        iter = target_server_to_task_.find(server);
        ret = (target_server_to_task_.end() == iter) ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
        task = iter->second;
      return task;
    }

    int64_t TaskManager::Machine::size(int32_t& source_size, int32_t& target_size) const
    {
      source_size = source_server_to_task_.size();
      target_size = target_server_to_task_.size();
      return source_size + target_size;
    }

    int64_t TaskManager::Machine::size() const
    {
      return source_server_to_task_.size() + target_server_to_task_.size();
    }

    TaskManager::TaskManager(LayoutManager& manager):
      manager_(manager),
      seqno_(0)
    {

    }

    TaskManager::~TaskManager()
    {
      TASKS_ITER iter = tasks_.begin();
      for (; iter != tasks_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
    }

    int TaskManager::add(const uint32_t id, const std::vector<ServerCollect*>& runer,
            const common::PlanType type, const time_t now, const common::PlanPriority priority)
    {
      //这里只判断Block是否正在写一次的原因: 因为add task到几个列表是不能被打断的，如果打断了处理的逻辑会很复杂
      //这里我们只能尽可能的保证同一个Block不同时做写操作，复制/均衡，因为目前这种做是没有办法完全保证同一个
      //Block不同时发生写，复制等，这个功能点可以放到DataServer去做，这个后期再改进
      int32_t ret = manager_.get_block_manager().has_write(id,now) ? EXIT_BLOCK_WRITING_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
        std::pair<BLOCK_TO_TASK_ITER, bool> res;
        res.first = block_to_tasks_.find(id);
        int32_t ret = block_to_tasks_.end() != res.first ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          Task* task = generation_(id, runer, type, priority);
          assert(NULL != task);
          res = block_to_tasks_.insert(BLOCK_TO_TASK::value_type(task->block_id_, task));
          std::pair<PENDING_TASK_ITER, bool> rs = pending_queue_.insert(task);
          ret = !rs.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            std::vector<std::pair<ServerCollect*, bool> > success;
            if (task->need_add_server_to_map())
            {
              int8_t index = 0;
              std::vector<ServerCollect*>::iterator it = task->runer_.begin();
              for (; it != task->runer_.end() && TFS_SUCCESS == ret; ++it, ++index)
              {
                if (task->type_ != PLAN_TYPE_COMPACT)
                {
                  std::pair<MACHINE_TO_TASK_ITER, bool> mrs;
                  uint64_t machine_ip = ((*it)->id() & 0xFFFFFFFF);
                  MACHINE_TO_TASK_ITER mit = machine_to_tasks_.find(machine_ip);
                  if (machine_to_tasks_.end() == mit)
                  {
                    mrs = machine_to_tasks_.insert(MACHINE_TO_TASK::value_type(machine_ip, Machine()));
                    mit = mrs.first;
                  }
                  ret = mit->second.add((*it)->id(), task, is_target(index));
                }
                else
                {
                  SERVER_TO_TASK_ITER mit = server_to_tasks_.find((*it)->id());
                  ret = server_to_tasks_.end() == mit ? TFS_SUCCESS : EXIT_TASK_EXIST_ERROR;
                  if (TFS_SUCCESS == ret)
                    server_to_tasks_.insert(SERVER_TO_TASK::value_type((*it)->id(), task));
                }
                if (TFS_SUCCESS == ret)
                   success.push_back(std::pair<ServerCollect*, bool>((*it), is_target(index)));
              }
            }

            //rollback
            if (TFS_SUCCESS != ret)
            {
              pending_queue_.erase(task);
              if (task->need_add_server_to_map())
              {
                std::vector<std::pair<ServerCollect*, bool> >::iterator it;
                for (it = success.begin(); it != success.end(); ++it)
                {
                  if (task->type_ != PLAN_TYPE_COMPACT)
                    remove_(it->first->id(), it->second);
                  else
                    server_to_tasks_.erase(it->first->id());
                }
              }
            }
          }

          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(DEBUG, "add task ret: %d, block: %u", ret, id);
            task->dump(TBSYS_LOG_LEVEL_WARN, "add task failed, rollback,");
            block_to_tasks_.erase(res.first);
            manager_.get_gc_manager().add(task, now);
          }
          else
          {
            task->seqno_ = ++seqno_;
            tasks_.insert(TASKS::value_type(task->seqno_, task));
          }
        }
      }
      return ret;
    }

    int TaskManager::remove(Task* task)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(task);
    }

    void TaskManager::clear()
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      TASKS_CONST_ITER iter = tasks_.begin();
      for (; iter != tasks_.end(); ++iter)
      {
        assert(NULL != iter->second);
        manager_.get_gc_manager().add(iter->second);
      }
      tasks_.clear();
      machine_to_tasks_.clear();
      block_to_tasks_.clear();
      pending_queue_.clear();
      server_to_tasks_.clear();
   }

    void TaskManager::dump(tbnet::DataBuffer& stream) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      stream.writeInt32(pending_queue_.size());
      PENDING_TASK_CONST_ITER iter = pending_queue_.begin();
      for (; iter != pending_queue_.end(); ++iter)
      {
        (*iter)->dump(stream);
      }
    }

    void TaskManager::dump(const int32_t level) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      PENDING_TASK_CONST_ITER iter = pending_queue_.begin();
      for (; iter != pending_queue_.end(); ++iter)
      {
        (*iter)->dump(level);
      }
    }

    Task* TaskManager::get_by_id(const uint64_t id) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      TASKS_CONST_ITER iter = tasks_.find(id);
      return (tasks_.end() == iter) ? NULL : iter->second;
    }

    int64_t TaskManager::get_running_server_size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return size_();
    }

    bool TaskManager::exist(const uint32_t block) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return (NULL != get_(block));
    }

    bool TaskManager::exist(const uint64_t server, bool compact) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return (NULL != get_(server, compact));
    }

    bool TaskManager::exist(const common::ArrayHelper<ServerCollect*>& servers, bool compact) const
    {
      bool ret = !servers.empty();
      if (ret)
      {
        ServerCollect* server = NULL;
        for (int32_t i = 0; i < servers.get_array_index() && ret; i++)
        {
          server = *servers.at(i);
          assert(NULL != server);
          ret = exist(server->id(), compact);
        }
      }
      return ret;
    }

    bool TaskManager::get_exist_servers(common::ArrayHelper<ServerCollect*>& result,
        const common::ArrayHelper<ServerCollect*>& servers) const
    {
      bool ret = !servers.empty();
      if (ret)
      {
        ServerCollect* server = NULL;
        for (int32_t i = 0; i < servers.get_array_index(); i++)
        {
          server = *servers.at(i);
          assert(NULL != server);
          ret = exist(server->id());
          if (ret)
            result.push_back(server);
        }
      }
      return (ret && result.get_array_index() == servers.get_array_index());
    }

    bool TaskManager::has_space_do_task_in_machine(const uint64_t server, const bool target) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t source_size = 0, target_size = 0, total = 0;
      total = get_task_size_in_machine_(source_size, target_size, server);
      TBSYS_LOG(DEBUG, "total = %d target: %d, source:%d, max_task_in_machine_nums_: %d", total ,target_size, source_size, SYSPARAM_NAMESERVER.max_task_in_machine_nums_);
      return target ? target_size < ((SYSPARAM_NAMESERVER.max_task_in_machine_nums_ / 2) + 1)
                    : source_size < ((SYSPARAM_NAMESERVER.max_task_in_machine_nums_ / 2) + 1);
    }

    bool TaskManager::has_space_do_task_in_machine(const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t source_size = 0, target_size = 0, total = 0;
      total = get_task_size_in_machine_(source_size, target_size, server);
      //TBSYS_LOG(DEBUG, "total = %d , max_task_in_machine_nums_: %d", total , SYSPARAM_NAMESERVER.max_task_in_machine_nums_);
      return total < SYSPARAM_NAMESERVER.max_task_in_machine_nums_;
    }

    int TaskManager::remove_block_from_dataserver(const uint64_t server, const uint32_t block, const int64_t seqno, const time_t now)
    {
      int32_t ret = remove_block_from_dataserver_(server, block, seqno, now);
      TBSYS_LOG(INFO, "send remove block: %u command on server : %s %s",
        block, tbsys::CNetUtil::addrToString(server).c_str(), TFS_SUCCESS == ret ? "successful" : "failed");
      return ret;
    }

    /*
     * expire blocks on dataserver only post expire message to ds, dont care result.
     * @param [in] server dataserver id the one who post to.
     * @param [in] block, the one need expired.
     * @return TFS_SUCCESS success.
     */
    int TaskManager::remove_block_from_dataserver_(const uint64_t server, const uint32_t block, const int64_t seqno, const time_t now)
    {
      RemoveBlockMessage rbmsg;
      rbmsg.set(block);
      rbmsg.set_seqno(seqno);
      //rbmsg.set_response_flag(REMOVE_BLOCK_RESPONSE_FLAG_YES);
      rbmsg.set_response_flag(REMOVE_BLOCK_RESPONSE_FLAG_NO);
      BlockInfo info;
      info.block_id_ = block;
      std::vector<uint32_t> blocks;
      std::vector<uint64_t> servers;
      blocks.push_back(block);
      servers.push_back(server);
      manager_.block_oplog_write_helper(OPLOG_RELIEVE_RELATION, info, blocks, servers, now);

      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = client->async_post_request(servers, &rbmsg, ns_async_callback);
        if (TFS_SUCCESS != ret)
          NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int64_t TaskManager::size_() const
    {
      int64_t total = server_to_tasks_.size();
      MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.begin();
      for (;iter != machine_to_tasks_.end(); ++iter)
      {
        total += iter->second.size();
      }
      return total;
    }

    int TaskManager::run()
    {
      time_t now = 0;
      Task* task = NULL;
      PENDING_TASK_ITER iter;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();
        if (ngi.in_safe_mode_time(now))
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, ngi.destroy_flag_);

        task = NULL;
        rwmutex_.wrlock();
        if (!pending_queue_.empty())
        {
          iter = pending_queue_.begin();
          task = *iter;
          pending_queue_.erase(iter);
        }
        rwmutex_.unlock();

        if (NULL != task)
        {
          if (TFS_SUCCESS != task->handle())
          {
            task->dump(TBSYS_LOG_LEVEL_WARN, "task handle failed");
            remove(task);
          }
        }
        usleep(500);
      }
      return TFS_SUCCESS;
    }

    bool TaskManager::handle(common::BasePacket* msg, const int64_t seqno)
    {
      bool all_complete_flag = true;
      bool ret = NULL != msg;
      if (ret)
      {
        Task* task = NULL;
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        bool master = ngi.is_master();
        if (master)
        {
          task = get_by_id(seqno);
        }
        else
        {
          std::vector<ServerCollect*> runer;
          if (msg->getPCode() == BLOCK_COMPACT_COMPLETE_MESSAGE)
          {
            task = new (std::nothrow)CompactTask(*this, PLAN_PRIORITY_NONE, 0, runer);
          }
          else if (msg->getPCode() == REPLICATE_BLOCK_MESSAGE)
          {
            ReplicateBlockMessage* replicate_msg = dynamic_cast<ReplicateBlockMessage*>(msg);
            task = replicate_msg->get_move_flag() == REPLICATE_BLOCK_MOVE_FLAG_NO
              ? new (std::nothrow)ReplicateTask(*this, PLAN_PRIORITY_NONE, 0, runer)
              : new (std::nothrow)MoveTask(*this, PLAN_PRIORITY_NONE, 0, runer);
          }
          assert(NULL != task);
        }
        ret = NULL != task;
        if (ret)
        {
          ret = TFS_SUCCESS == task->handle_complete(msg, all_complete_flag);
          if (master)
          {
            task->dump(TBSYS_LOG_LEVEL_INFO, "handle message complete, show result");
            if (all_complete_flag)
              remove(task);
            /*Task* complete[SYSPARAM_NAMESERVER.max_replication_];
            ArrayHelper<Task*> helper(SYSPARAM_NAMESERVER.max_replication_, complete);
            if (all_complete_flag)
              helper.push_back(task);
            remove(helper);*/
          }
          else
          {
            msg->dump();
            tbsys::gDelete(task);
          }
        }
      }
      return ret;
    }

    int TaskManager::timeout(const time_t now)
    {
      int32_t index = 0;
      Task* task = NULL;
      const int32_t MAX_COUNT = 2048;
      Task* expire_tasks[MAX_COUNT];
      ArrayHelper<Task*> helper(MAX_COUNT, expire_tasks);
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      TASKS_ITER iter = tasks_.begin();
      while (iter != tasks_.end() && index < MAX_COUNT)
      {
        ++index;
        task = iter->second;
        if (task->timeout(now))
        {
          helper.push_back(task);
          task->runTimerTask();
          tasks_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      remove_(helper);
      return helper.get_array_index();
    }

    int TaskManager::remove_(Task* task)
    {
      int32_t ret = (NULL != task)? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        TASKS_ITER titer = tasks_.find(task->seqno_);
        if (tasks_.end() != titer)
          tasks_.erase(titer);

        BLOCK_TO_TASK_ITER it = block_to_tasks_.find(task->block_id_);
        if (block_to_tasks_.end() != it)
          block_to_tasks_.erase(it);

        if (task->need_add_server_to_map())
        {
          int32_t index = 0;
          std::vector<ServerCollect*>::iterator rit = task->runer_.begin();
          for (; rit != task->runer_.end(); ++rit, ++index)
          {
            if (task->type_ != PLAN_TYPE_COMPACT)
              remove_((*rit)->id(), is_target(index));
            else
              server_to_tasks_.erase((*rit)->id());
          }
        }
        manager_.get_gc_manager().add(task);
      }
      return ret;
    }

    int TaskManager::remove_(const uint64_t server, const bool target)
    {
      uint64_t machine_ip = server & 0xFFFFFFFF;
      MACHINE_TO_TASK_ITER iter = machine_to_tasks_.find(machine_ip);
      if (machine_to_tasks_.end() != iter)
      {
        iter->second.remove(server, target);
        if (iter->second.size() <= 0)
        {
          machine_to_tasks_.erase(iter);
        }
      }
      return TFS_SUCCESS;
    }

    bool TaskManager::remove(const common::ArrayHelper<Task*>& tasks)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(tasks);
    }

    bool TaskManager::remove_(const common::ArrayHelper<Task*>& tasks)
    {
      bool all_complete = true;
      Task* task = NULL;
      for (int32_t i = 0; i < tasks.get_array_index(); i++)
      {
        task = *tasks.at(i);
        assert(NULL != task);
        int32_t ret = remove_(task);
        if (all_complete)
          all_complete = ret == TFS_SUCCESS;
      }
      return all_complete;
    }

    Task* TaskManager::get_(const uint32_t block) const
    {
      BLOCK_TO_TASK_CONST_ITER iter = block_to_tasks_.find(block);
      return (block_to_tasks_.end() == iter) ? NULL : iter->second;
    }

    Task* TaskManager::get_(const BlockCollect* block) const
    {
      return (NULL != block) ? get_(block->id()) : NULL;
    }

    Task* TaskManager::get_(const uint64_t server, bool compact) const
    {
      Task* task = NULL;
      if (!compact)
      {
        uint64_t machine_ip = server & 0xFFFFFFFF;
        MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.find(machine_ip);
        if (machine_to_tasks_.end() != iter)
          task = iter->second.get(server);
      }
      else
      {
        SERVER_TO_TASK_CONST_ITER iter = server_to_tasks_.find(server);
        if (server_to_tasks_.end() != iter)
          task = iter->second;
      }
      return task;
    }

    Task* TaskManager::get_(const ServerCollect* server, bool compact) const
    {
      return (NULL != server) ? get_(server->id(), compact) : NULL;
    }

    int64_t TaskManager::get_task_size_in_machine_(int32_t& source_size, int32_t& target_size, const uint64_t server) const
    {
      uint64_t machine_ip = server & 0xFFFFFFFF;
      MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.find(machine_ip);
      return machine_to_tasks_.end() == iter ? 0 : iter->second.size(source_size, target_size);
    }

    Task* TaskManager::generation_(const uint32_t id, const std::vector<ServerCollect*>& runer,
            const common::PlanType type, const common::PlanPriority priority)
    {
      Task* result = NULL;
      if (type == PLAN_TYPE_REPLICATE)
      {
        result = new (std::nothrow)ReplicateTask(*this, priority, id, runer);
      }
      else if (type == PLAN_TYPE_MOVE)
      {
        result = new (std::nothrow)MoveTask(*this, priority, id, runer);
      }
      else if (type == PLAN_TYPE_COMPACT)
      {
        result = new (std::nothrow)CompactTask(*this, priority, id, runer);
      }
      else if (type == PLAN_TYPE_DELETE)
      {
        result = new (std::nothrow)DeleteTask(*this, priority, id, runer);
      }
      assert(NULL != result);
      return result;
    }

    bool TaskManager::is_target(const int32_t index)
    {
      return index % 2 == 1;
    }
  }/** nameserver **/
}/** tfs **/
