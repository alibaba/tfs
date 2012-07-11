/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: integration_case.cpp 2090 2011-03-14 16:11:58Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "common/define.h"
#include "common/internal.h"
#include "common/config.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "message/message_factory.h"
#include "integration_case.h"
#include "integration_instance.h"

using namespace tfs::integration;
using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace tbnet;
using namespace tbsys;
using namespace tbutil;

IntegrationBaseCase::IntegrationBaseCase(IntegrationServiceInstance* instance, const char* name, int32_t mode_count, int32_t mode)
  :instance_(instance),
   mode_(mode),
   mode_count_(mode_count),
   name_(name)
{

}

IntegrationBaseCase::~IntegrationBaseCase()
{

}


KickCase::KickCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "kick-stat", mode_count, mode)
{

}

KickCase::~KickCase()
{

}

void KickCase::execute(const ThreadPool* pool)
{
  uint32_t block_id = 0;
  BlockCollect* block = instance_->layout_manager_.add_new_block(block_id);
  if (block == NULL)
  {
    TBSYS_LOG(DEBUG, "%s", "add new block fail");
  }
  else
  {
    bool bfind = false;
    BlockInfo info;
    std::vector<uint64_t> servers;
    {
      BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, READ_LOCKER);
      block = ptr->find(block_id);
      bfind = block != NULL;
      if (bfind)
      {
        info = block->info_;
        uint32_t count = block->get_hold_size();
        TBSYS_LOG(INFO, "block(%u) hold server size(%u)", block_id, count);
        std::vector<ServerCollect*>::iterator iter = block->get_hold().begin(); 
        for(; iter !=  block->get_hold().end(); ++iter)
        {
          if ((*iter) != NULL)
          {
            servers.push_back((*iter)->id());
          }
        }
      }
    }
    if (bfind)
    {
      instance_->add_block(info, servers);
      std::vector<int64_t> vec(1,1);
      instance_->stat_mgr_.update_entry(name(), vec);
    }
  }
}

void KickCase::destroy()
{

}

IntegrationBaseCase* KickCase::new_object(int32_t mode)
{
  return new KickCase(instance_, mode_count_, mode);
}


OpenCloseCase::OpenCloseCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "open-close-stat", mode_count, mode)
{

}

OpenCloseCase::~OpenCloseCase()
{

}

void OpenCloseCase::execute(const tbutil::ThreadPool* pool)
{
  uint32_t block_id = 0;
  uint32_t lease_id = 0;
  int32_t  mode = T_READ;
  int32_t  version = 0;
  int32_t  iret = TFS_ERROR;
  std::vector<uint64_t> servers;

  if (mode_ & 1)
  {
    servers.clear();
    mode = T_READ;
    block_id =  instance_->random_elect_block();
    if (block_id == 0)
    {
      TBSYS_LOG(ERROR, "%s", "block not found"); 
      return;
    }
    iret = instance_->layout_manager_.open(block_id, mode, lease_id, version, servers);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open block(%u) fail, mode(read)", block_id);
      return;
    }
    std::vector<int64_t> vec(2,0);
    vec[0] = 1;
    instance_->stat_mgr_.update_entry(name(), vec);
  }
  else if (mode_ & 2)
  {
    block_id = 0;
    lease_id = 0;
    servers.clear();
    mode = T_WRITE | T_CREATE;
    iret = instance_->layout_manager_.open(block_id, mode, lease_id, version, servers);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open block(%u) fail, mode(write)", block_id);
      return;
    }
    TBSYS_LOG(DEBUG, "block(%u), lease_id(%u)", block_id, lease_id);

    CloseParameter param;                                                                      
    memset(&param, 0, sizeof(param));

    BlockInfo info;
    std::vector<uint64_t> vec;
    {
      BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, READ_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL)
      {
        TBSYS_LOG(ERROR, "block(%u) not found", block_id);
        return;
      }

      info = block->info_;
      info.version_++;
      info.size_ += 1024;

      param.need_new_ = false;
      param.block_info_ = block->info_;
      param.id_ = servers[0];
      param.lease_id_ = lease_id; 
      param.status_ = random() % 100 == 0 ? WRITE_COMPLETE_STATUS_NO : WRITE_COMPLETE_STATUS_YES;
      param.unlink_flag_ = random() % 100 == 0 ? UNLINK_FLAG_NO : UNLINK_FLAG_YES;
      std::vector<ServerCollect*>::iterator iter = block->get_hold().begin(); 
      for(; iter !=  block->get_hold().end(); ++iter)
      {
        if ((*iter) != NULL)
        {
          servers.push_back((*iter)->id());
        }
      }
    }
    instance_->update_block_info(info, vec);

    instance_->layout_manager_.close(param);
    std::vector<int64_t> tmp(2,0);
    tmp[1] = 1;
    instance_->stat_mgr_.update_entry(name(), tmp);
  }
  else if (mode & 4)
  {
    block_id = 0;
    lease_id = 0;
    servers.clear();
    mode = T_WRITE | T_CREATE;
    iret = instance_->layout_manager_.open(block_id, mode, lease_id, version, servers);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open block(%u) fail, mode(write)", block_id);
      return;
    }
    TBSYS_LOG(DEBUG, "block(%u), lease_id(%u)", block_id, lease_id);
  }
}

void OpenCloseCase::destroy()
{

}

IntegrationBaseCase* OpenCloseCase::new_object(int32_t mode)
{
  return new OpenCloseCase(instance_, mode_count_, mode);
}

BuildReplicateCase::BuildReplicateCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "build-replicate-case-stat", mode_count, mode)
{

}

BuildReplicateCase::~BuildReplicateCase()
{

}

void BuildReplicateCase::execute(const ThreadPool* pool)
{
  time_t now = time(NULL);
  std::vector<uint32_t> blocks;
  std::vector<uint64_t> servers;
  int32_t count = 2;
  int64_t need = 1;
  int64_t adjust = 0;
  int64_t emergency_replicate_count = 0;
  bool bret = false;
  if (mode_ & 1)
  {
    instance_->gmutex_.lock();
    do
    {
      --count;
      bret = instance_->layout_manager_.build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks);
      if (!bret || blocks.empty())
      {
        uint32_t block_id = 0;
        block_id =  instance_->random_elect_block();
        if (block_id == 0)
        {
          continue;
        }
        BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        if (block == NULL || block->get_hold_size() <= 0)
        {
          continue;
        }
        {
          std::vector<ServerCollect*> result;
          RWLock::Lock tlock(instance_->layout_manager_.maping_mutex_, READ_LOCKER);
          bool has_valid = (!instance_->layout_manager_.find_block_in_plan(block->id()))
                           && (!instance_->layout_manager_.find_server_in_plan(block->get_hold(), false, result));
          if (!has_valid)
            continue;
        }
        block->last_update_time_ = now - 0xffffff;
        std::vector<ServerCollect*> hold =  block->get_hold();
        std::vector<ServerCollect*>::iterator iter = hold.begin();
        iter += 1;
        for (;iter != hold.end() ; ++iter)
        {
          ServerCollect* server = (*iter);
          if (instance_->layout_manager_.relieve_relation(block, server, now) == TFS_SUCCESS)
          {
            servers.push_back(server->id());
          }
        }
        std::vector<uint64_t>::iterator r_iter = servers.begin();
        for (; r_iter != servers.end(); ++r_iter)
        {
          instance_->remove_block(block_id, (*r_iter));
        }
      }
    }
    while (count > 0 && (!bret || blocks.empty()));
    instance_->gmutex_.unlock();
  }
  else if (mode_ & 2)
  {
    instance_->gmutex_.lock();
    do
    {
      --count;
      bret = instance_->layout_manager_.build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks);
      if (!bret || blocks.empty())
      {
        uint32_t block_id = 0;
        block_id =  instance_->random_elect_block();
        if (block_id == 0)
        {
          continue;
        }
        BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        if (block == NULL || block->get_hold_size() <= SYSPARAM_NAMESERVER.min_replication_)
        {
          continue;
        }
        {
          std::vector<ServerCollect*> result;
          RWLock::Lock tlock(instance_->layout_manager_.maping_mutex_, READ_LOCKER);
          bool has_valid = (!instance_->layout_manager_.find_block_in_plan(block->id()))
                           &&(!instance_->layout_manager_.find_server_in_plan(block->get_hold(), false, result));
          if (!has_valid)
            continue;
        }
 
        block->last_update_time_ = now - 0xffffff;

        std::vector<ServerCollect*> hold =  block->get_hold();
        TBSYS_LOG(DEBUG, "server size(%u)", hold.size());
        std::vector<ServerCollect*>::iterator iter = hold.begin();
        iter += SYSPARAM_NAMESERVER.min_replication_;
        for (; iter != hold.end(); ++iter)
        {
          ServerCollect* server = (*iter);
          if (instance_->layout_manager_.relieve_relation(block, server, now) == TFS_SUCCESS)
          {
            servers.push_back(server->id());
          }
        }
        std::vector<uint64_t>::iterator r_iter = servers.begin();
        for (; r_iter != servers.end(); ++r_iter)
        {
          instance_->remove_block(block_id, (*r_iter));
        }
      }
    }
    while (count > 0 && (!bret || blocks.empty()));
    instance_->gmutex_.unlock();
  }

  TBSYS_LOG(INFO, "build replicate plan %s", blocks.empty() ? "fail" : "successful");

  if (blocks.empty())
    return;

  if (random() % 100 == 0)
    return;

  usleep(100);

  std::vector<uint32_t>::iterator iter = blocks.begin();
  for (; iter != blocks.end(); ++iter)
  {
    LayoutManager::TaskPtr task = instance_->layout_manager_.find_task((*iter));
    if (task == 0)
    {
      TBSYS_LOG(ERROR, "task not found, block(%u)", (*iter));
      return;
    }

    ReplicateBlockMessage* msg = new ReplicateBlockMessage();
    msg->set_command(random() % 100 == 0 ? PLAN_STATUS_TIMEOUT : PLAN_STATUS_END);
    ReplBlock block;
    memset(&block, 0, sizeof(block));
    block.block_id_ = (*iter);
    block.source_id_ = task->runer_[0]->id();
    block.destination_id_ = task->runer_[1]->id();
    block.start_time_ = time(NULL);
    block.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_NO;
    block.server_count_ = 0;
    msg->set_repl_block(&block);

    instance_->layout_manager_.handle_task_complete(msg);
    
    tbsys::gDelete(msg);
    TBSYS_LOG(INFO, "block(%u) build replicate successful", (*iter));
  }
}

void BuildReplicateCase::destroy()
{

}

IntegrationBaseCase* BuildReplicateCase::new_object(int32_t mode)
{
  return new BuildReplicateCase(instance_, mode_count_, mode);
}

BuildBalanceCase::BuildBalanceCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "build-balance-case-stat", mode_count, mode)
{

}

BuildBalanceCase::~BuildBalanceCase()
{

}

void BuildBalanceCase::execute(const ThreadPool* pool)
{
  time_t now = time(NULL);
  std::vector<uint32_t> blocks;
  std::vector<uint64_t> servers;
  int32_t count = 2;
  int64_t need = 1;
  bool bret = false;
  instance_->gmutex_.lock();
  do
  {
    --count;
    bret = instance_->layout_manager_.build_balance_plan(now, need, blocks);
    if (!bret || blocks.empty())
    {
      uint32_t block_id = 0;
      block_id =  instance_->random_elect_block();
      if (block_id == 0)
      {
        continue;
      }
      BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL 
          || block->get_hold_size() <= SYSPARAM_NAMESERVER.min_replication_)
      {
        continue;
      }
      {
        std::vector<ServerCollect*> result;
        RWLock::Lock tlock(instance_->layout_manager_.maping_mutex_, READ_LOCKER);
        bool has_valid = (!instance_->layout_manager_.find_block_in_plan(block->id()))
          &&(!instance_->layout_manager_.find_server_in_plan(block->get_hold(), false, result));
        if (!has_valid)
          continue;
      }
    }
  }
  while (count > 0 && (!bret || blocks.empty()));
  instance_->gmutex_.unlock();

  TBSYS_LOG(INFO, "build balance plan %s", blocks.empty() ? "fail" : "successful");

  if (blocks.empty())
    return;

  if (random() % 100 == 0)
    return;

  usleep(100);

  std::vector<uint32_t>::iterator iter = blocks.begin();
  for (; iter != blocks.end(); ++iter)
  {
    LayoutManager::TaskPtr task = instance_->layout_manager_.find_task((*iter));
    if (task == 0)
    {
      TBSYS_LOG(ERROR, "task not found, block(%u)", (*iter));
      return;
    }

    ReplicateBlockMessage* msg = new ReplicateBlockMessage();
    msg->set_command(random() % 100 == 0 ? PLAN_STATUS_TIMEOUT : PLAN_STATUS_END);
    ReplBlock block;
    memset(&block, 0, sizeof(block));
    block.block_id_ = (*iter);
    block.source_id_ = task->runer_[0]->id();
    block.destination_id_ = task->runer_[1]->id();
    block.start_time_ = time(NULL);
    block.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_YES;
    block.server_count_ = 0;
    msg->set_repl_block(&block);

    instance_->layout_manager_.handle_task_complete(msg);
    
    tbsys::gDelete(msg);
    TBSYS_LOG(INFO, "block(%u) build replicate successful", (*iter));
  }
}

void BuildBalanceCase::destroy()
{

}

IntegrationBaseCase* BuildBalanceCase::new_object(int32_t mode)
{
  return new BuildBalanceCase(instance_, mode_count_, mode);
}

BuildCompactCase::BuildCompactCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "build-compact-case-stat", mode_count, mode)
{

}

BuildCompactCase::~BuildCompactCase()
{

}

void BuildCompactCase::execute(const ThreadPool* pool)
{
  time_t now = time(NULL);
  int64_t need = 1;
  int32_t count = 2;
  bool bret = false;
  std::vector<uint64_t> servers;
  std::vector<uint32_t> blocks;
  BlockInfo info;
  memset(&info, 0, sizeof(info));
  instance_->gmutex_.lock();
  do
  {
    --count;
    bret = instance_->layout_manager_.build_compact_plan(now, need, blocks);
    if (!bret || blocks.empty())
    {
      uint32_t block_id = 0;
      block_id =  instance_->random_elect_block();
      if (block_id == 0)
      {
        continue;
      }
      BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL
        || block->get_hold_size() > SYSPARAM_NAMESERVER.max_replication_
        || block->get_hold_size() < SYSPARAM_NAMESERVER.min_replication_)
      {
         continue;
      }

      {
        std::vector<ServerCollect*> result;
        RWLock::Lock tlock(instance_->layout_manager_.maping_mutex_, READ_LOCKER);
        bool has_valid = (!instance_->layout_manager_.find_block_in_plan(block->id()))
          &&(!instance_->layout_manager_.find_server_in_plan(block->get_hold(), false, result));
        if (!has_valid)
          continue;
      }

      block->info_.size_ = 0xffffff;
      block->info_.file_count_ = 0xff;
      block->info_.del_file_count_ = block->info_.file_count_ * SYSPARAM_NAMESERVER.compact_delete_ratio_ + 0xf;
      block->info_.del_size_ = block->info_.size_ * SYSPARAM_NAMESERVER.compact_delete_ratio_ + 0xfff;
      info = block->info_;
    }
  }
  while (count > 0 && (!bret || blocks.empty()));
  instance_->gmutex_.unlock();

  TBSYS_LOG(INFO, "build compact plan %s", blocks.empty() ? "fail" : "successful");

  if (blocks.empty())
    return;

  if (random() % 100 == 0)
    return;

  usleep(100);


  std::vector<uint32_t>::iterator iter = blocks.begin();
  for (; iter != blocks.end(); ++iter)
  {
    LayoutManager::TaskPtr task = instance_->layout_manager_.find_task((*iter));
    if (task == 0)
    {
      TBSYS_LOG(ERROR, "task not found, block(%u)", (*iter));
      return;
    }

    std::vector<ServerCollect*>::iterator r_iter = task->runer_.begin();
    for(; r_iter != task->runer_.end(); ++r_iter)
    {
      info.size_ = 0xffff;
      info.file_count_ = 0;
      info.del_file_count_ = 0;
      info.del_size_ = 0;
      CompactBlockCompleteMessage* msg = new CompactBlockCompleteMessage();
      PlanStatus status = random() % 100 == 0 ? PLAN_STATUS_TIMEOUT : PLAN_STATUS_END;
      msg->set_success(status);
      if (status == PLAN_STATUS_TIMEOUT)
      {
        this->instance_->remove_block((*r_iter)->id(), task->block_id_);
      }
      msg->set_block_id(task->block_id_);
      msg->set_server_id((*r_iter)->id());
      msg->set_block_info(info);
      instance_->layout_manager_.handle_task_complete(msg);
      tbsys::gDelete(msg);
    }
    TBSYS_LOG(INFO, "block(%u) build compact successful", (*iter));
  }
}

void BuildCompactCase::destroy()
{

}

IntegrationBaseCase* BuildCompactCase::new_object(int32_t mode)
{
  return new BuildCompactCase(instance_, mode_count_, mode);
}

BuildRedundantCase::BuildRedundantCase(IntegrationServiceInstance* instance, int32_t mode_count, int32_t mode)
  :IntegrationBaseCase(instance, "build-compact-case-stat", mode_count, mode)
{

}

BuildRedundantCase::~BuildRedundantCase()
{

}

void BuildRedundantCase::execute(const ThreadPool* pool)
{
  time_t now = time(NULL);
  int64_t need = 1;
  int32_t count = 2;
  bool bret = false;
  std::vector<uint64_t> servers;
  std::vector<uint32_t> blocks;
  BlockInfo info;
  memset(&info, 0, sizeof(info));
  instance_->gmutex_.lock();
  do
  {
    blocks.clear();
    --count;
    bret = instance_->layout_manager_.build_redundant_plan(now, need, blocks);
    if (blocks.empty())
    {
      uint32_t block_id = 0;
      block_id =  instance_->random_elect_block();
      if (block_id == 0)
      {
        continue;
      }
      BlockChunkPtr ptr = instance_->layout_manager_.get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL
        || block->get_hold_size() < SYSPARAM_NAMESERVER.min_replication_)
      {
         continue;
      }
      {
        std::vector<ServerCollect*> result;
        RWLock::Lock tlock(instance_->layout_manager_.maping_mutex_, READ_LOCKER);
        bool has_valid = (!instance_->layout_manager_.find_block_in_plan(block->id()))
          &&(!instance_->layout_manager_.find_server_in_plan(block->get_hold(), false, result));
        if (!has_valid)
          continue;
      }


      int32_t diff = __gnu_cxx::abs(block->get_hold_size() - SYSPARAM_NAMESERVER.max_replication_) + 1;
      
      std::vector<ServerCollect*> servers;
      for (int32_t i = 0; i < diff; ++i)
      {
        uint64_t server_id = instance_->random_elect_server();
        if (server_id != 0)
        {
          ServerCollect* tmp = instance_->layout_manager_.get_server(server_id);
          if (tmp != NULL)
          {
            servers.push_back(tmp);
          }
        }
      }

      std::vector<ServerCollect*>::iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        instance_->layout_manager_.build_relation(block, (*iter), time(NULL));
      }
    }
  }
  while (count > 0 && (blocks.empty()));
  instance_->gmutex_.unlock();

  TBSYS_LOG(INFO, "build redundant plan %s", blocks.empty() ? "fail" : "successful");
  if (blocks.empty())
    return;

  if (random() % 100 == 0) 
    return ;
  
  usleep(10);

  std::vector<uint32_t>::iterator iter = blocks.begin();
  for (; iter != blocks.end(); ++iter)
  {
    LayoutManager::TaskPtr task = instance_->layout_manager_.find_task((*iter));
    if (task == 0)
    {
      TBSYS_LOG(ERROR, "task not found, block(%u)", (*iter));
      return;
    }

    std::vector<ServerCollect*>::iterator r_iter = task->runer_.begin();
    for(; r_iter != task->runer_.end(); ++r_iter)
    {
      instance_->remove_block((*r_iter)->id(), task->block_id_);
      RemoveBlockResponseMessage * msg = new RemoveBlockResponseMessage();
      msg->set_block_id(task->block_id_);
      instance_->layout_manager_.handle_task_complete(msg);
      tbsys::gDelete(msg);
    }
    TBSYS_LOG(ERROR, "block(%u) build redundant successful", (*iter));
  }
}

void BuildRedundantCase::destroy()
{

}

IntegrationBaseCase* BuildRedundantCase::new_object(int32_t mode)
{
  return new BuildRedundantCase(instance_, mode_count_, mode);
}


