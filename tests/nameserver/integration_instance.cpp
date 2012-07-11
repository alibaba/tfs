/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: integration_instance.cpp 2090 2011-02-16 06:52:58Z duanfei $
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
#include "integration_instance.h"
#include "integration_case.h"

using namespace tfs::integration;
using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tbnet;
using namespace tbsys;
using namespace tbutil;

IntegrationServiceInstance::IntegrationServiceInstance():
  destroyed_(false),
  timer_(0),
  driver_thread_(0)
{

}

IntegrationServiceInstance::~IntegrationServiceInstance()
{

}

int IntegrationServiceInstance::initialize(int32_t min_size, int32_t max_size, const std::string& conf)
{
  srandom(time(NULL));

  TBSYS_LOG(DEBUG, "%s", "instance initialize");

  int32_t iret = TFS_SUCCESS;

  iret = initialize_all_data_server(1000);
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initailize dataserver fail, must be exit, ret(%d)", iret);
    return iret;
  }

  iret = GFactory::initialize();
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initailize gfactory fail, must be exit, ret(%d)", iret);
    return iret;
  }

  GFactory::get_runtime_info().owner_ip_port_ = 0xffffffffff;
  GFactory::get_runtime_info().last_owner_check_time_ = 0xffffffffff;
  GFactory::get_runtime_info().switch_time_ = 0xffffffffff;
  GFactory::get_runtime_info().vip_ = 0xffffffff;
  GFactory::get_runtime_info().owner_role_ = NS_ROLE_MASTER;
  GFactory::get_runtime_info().destroy_flag_ = NS_DESTROY_FLAGS_NO;
  GFactory::get_runtime_info().owner_status_ = NS_STATUS_INITIALIZED;
  
  int32_t block_chunk_num = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BLOCK_CHUNK_NUM, 32);
  iret = layout_manager_.initialize(block_chunk_num);
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize layoutmanager fail, must be exit, ret(%d)", iret);
    return iret;
  }

  TBSYS_LOG(DEBUG, "%s", "=================start==========================");

  timer_ = new Timer();

  driver_thread_ = new DriverThreadHelper(*this);

  int32_t heart_interval = CONFIG.get_int_value(CONFIG_PUBLIC,CONF_HEART_INTERVAL,2);
  KeepaliveTimerTaskPtr task = new KeepaliveTimerTask(*this);
  timer_->scheduleRepeated(task, tbutil::Time::seconds(heart_interval));

  main_work_queue_ = new tbutil::ThreadPool(min_size, max_size);

  TBSYS_LOG(INFO, "%s", "integration servcie initialize complete");

  iret = stat_mgr_.initialize(timer_);
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "%s", "initialize stat manager fail");
    return iret;
  }

  int64_t current = tbsys::CTimeUtil::getTime();
  IntegrationBaseCase* pcase = new OpenCloseCase(this, 2, 0);
  tasks_.push_back(pcase);
  StatEntry<std::string, std::string>::StatEntryPtr open_close_stat = new StatEntry<std::string, std::string>(pcase->name(), current, false);
  open_close_stat->add_sub_key("open-close-stat-read");
  open_close_stat->add_sub_key("open-close-stat-write");
  stat_mgr_.add_entry(open_close_stat, SYSPARAM_NAMESERVER.dump_stat_info_interval_);
  pcase = new KickCase(this, 1, 0);
  tasks_.push_back(pcase);
  StatEntry<std::string, std::string>::StatEntryPtr kick_stat = new StatEntry<std::string, std::string>(pcase->name(), current, false);
  kick_stat->add_sub_key("kick-success");
  stat_mgr_.add_entry(kick_stat, SYSPARAM_NAMESERVER.dump_stat_info_interval_);

  pcase = new BuildReplicateCase(this, 2, 0);
  tasks_.push_back(pcase);

  pcase = new BuildRedundantCase(this, 1, 0);
  tasks_.push_back(pcase);

  pcase = new BuildCompactCase(this, 1, 0);
  tasks_.push_back(pcase);

  pcase = new BuildBalanceCase(this, 1, 0);
  tasks_.push_back(pcase);

  return TFS_SUCCESS;
}

int IntegrationServiceInstance::wait_for_shut_down()
{
  if (driver_thread_!= 0)
  {
    driver_thread_->join();
  }

  if (main_work_queue_ != NULL)
  {
    main_work_queue_->joinWithAllThreads();
    tbsys::gDelete(main_work_queue_);
  }

  GFactory::wait_for_shut_down();

  layout_manager_.wait_for_shut_down();

  std::map<uint64_t, DataServer*>::iterator iter = servers_.begin();
  for (; iter != servers_.end(); ++iter)
  {
    tbsys::gDelete(iter->second);
  }
  servers_.clear();

  std::vector<IntegrationBaseCase*>::iterator it= tasks_.begin();
  for (; it != tasks_.end(); ++it)
  {
    tbsys::gDelete((*it));
  }
  tasks_.clear();

  return TFS_SUCCESS;
}

bool IntegrationServiceInstance::destroy()
{
  destroyed_ = true;

  GFactory::destroy();
 
  if (timer_ != 0)
  {
    timer_->destroy();
  }

  GFactory::get_runtime_info().destroy_flag_ = NS_DESTROY_FLAGS_YES;
  TBSYS_LOG(DEBUG, "%s", "============destroy==============\n");
  if (main_work_queue_ != NULL)
  {
    main_work_queue_->destroy();
  }

  GFactory::destroy();

  layout_manager_.destroy();

  TBSYS_LOG(DEBUG, "%s", "=======================================destroy");

  return true;
}

int IntegrationServiceInstance::keepalive()
{
  bool need_sent_block = false;
  int32_t iret = TFS_ERROR;
  std::map<uint64_t, DataServer*>::iterator iter = servers_.begin();
  for (;iter != servers_.end(); ++iter)
  {
    common::HasBlockFlag flag = HAS_BLOCK_FLAG_NO;
    common::BLOCK_INFO_LIST blocks;
    common::VUINT32 expires;
    {
      tbutil::Mutex::Lock rlock(iter->second->mutex_);
      iter->second->info_.block_count_ = iter->second->blocks_.size();
      if (iter->second->need_sent_block_)
      {
        iter->second->need_sent_block_ = false;
        flag = HAS_BLOCK_FLAG_YES;
        std::map<uint32_t, common::BlockInfo>::const_iterator it = iter->second->blocks_.begin();
        for (; it != iter->second->blocks_.end(); ++it)
        {
          blocks.push_back(it->second);
        }
     }
    }
    iret = layout_manager_.keepalive(iter->second->info_, flag, blocks, expires, need_sent_block);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "server(%s) keepalive fail", CNetUtil::addrToString(iter->second->info_.id_).c_str());
    }
  }
  return TFS_SUCCESS;
}

bool IntegrationServiceInstance::add_task(tbutil::ThreadPoolWorkItem* workitem)
{
  bool bret = workitem == NULL;
  if (!bret)
  {
    bret = (main_work_queue_->execute(workitem) == 0);
  }
  return bret;
}

int IntegrationServiceInstance::add_server(DataServerStatInfo& info)
{
  DataServer* server = new DataServer();
  std::pair<std::map<uint64_t, DataServer*>::iterator, bool> res = servers_.insert(std::pair<uint64_t, DataServer*>(info.id_,server)); 
  if (!res.second)
  {
    TBSYS_LOG(WARN, "server(%s) already existed", CNetUtil::addrToString(info.id_).c_str());
    tbsys::gDelete(server);
    return TFS_ERROR;
  }

  memcpy(&server->info_, &info, sizeof(info));
  server->need_sent_block_ = true;
  std::vector<uint64_t>::iterator r_iter = std::find(servers_mapping_.begin(), servers_mapping_.end(), info.id_); 
  if (r_iter != servers_mapping_.end())
  {
    servers_mapping_.erase(r_iter);
  }
  servers_mapping_.push_back(info.id_);
 
  return TFS_SUCCESS;
}

int IntegrationServiceInstance::remove_server(uint64_t id)
{
  /*std::map<uint64_t, DataServer*>::iterator iter = servers_.find(id);
  if (iter != servers_.end())
  {
    TBSYS_LOG(WARN, "server(%s) not existed", CNetUtil::addrToString(id).c_str());
    return TFS_ERROR;
  } 
  servers_.erase(iter);*/
  return TFS_SUCCESS;
}

DataServer* IntegrationServiceInstance::find_server(uint64_t id)
{
  std::map<uint64_t, DataServer*>::const_iterator iter = servers_.find(id);
  return iter == servers_.end() ? NULL : iter->second;
}

uint64_t IntegrationServiceInstance::random_elect_server()
{
  return servers_mapping_.empty() ? 0 : servers_mapping_[ random() % servers_mapping_.size()];
}

int IntegrationServiceInstance::add_block(BlockInfo& info, std::vector<uint64_t>& servers)
{
  TBSYS_LOG(INFO, "add block(%u)", info.block_id_);
  std::vector<uint64_t>::iterator iter = servers.begin();
  for (; iter != servers.end(); ++iter)
  {
    DataServer* pserver = find_server((*iter));
    if (pserver != NULL)
    {
      tbutil::Mutex::Lock lock(pserver->mutex_);
      pserver->blocks_[info.block_id_] = info;
      std::vector<uint32_t>::iterator r_iter = std::find(pserver->blocks_mapping_.begin(), pserver->blocks_mapping_.end(), info.block_id_);
      if (r_iter != pserver->blocks_mapping_.end())
      {
        pserver->blocks_mapping_.erase(r_iter);
      }
      pserver->blocks_mapping_.push_back(info.block_id_);
    }
  }
  return TFS_SUCCESS;
}

int IntegrationServiceInstance::remove_block(uint64_t id , uint32_t block_id)
{
  DataServer* pserver = find_server(id);
  if (pserver != NULL)
  {
    tbutil::Mutex::Lock lock(pserver->mutex_);
    pserver->blocks_.erase(block_id);
    std::vector<uint32_t>::iterator r_iter = std::find(pserver->blocks_mapping_.begin(), pserver->blocks_mapping_.end(), block_id);
    if (r_iter != pserver->blocks_mapping_.end())
    {
      pserver->blocks_mapping_.erase(r_iter);
    }
  }
  return TFS_SUCCESS;
}

int IntegrationServiceInstance::update_block_info(BlockInfo& info, std::vector<uint64_t>& servers)
{
  std::vector<uint64_t>::iterator iter = servers.begin();
  for (; iter != servers.end(); ++iter)
  {
    DataServer* pserver = find_server((*iter));
    if (pserver != NULL)
    {
      tbutil::Mutex::Lock lock(pserver->mutex_);
      pserver->info_.use_capacity_ += info.size_;
      pserver->blocks_[info.block_id_] = info;
    }
  }
  return TFS_SUCCESS;
}

uint32_t IntegrationServiceInstance::random_elect_block()
{
  uint64_t id = random_elect_server();
  if (id != 0)
  {
    DataServer* pserver = find_server(id);
    if (pserver != NULL)
    {
      tbutil::Mutex::Lock lock(pserver->mutex_);
      return pserver->blocks_mapping_.empty() ? 0 : pserver->blocks_mapping_[random() % pserver->blocks_mapping_.size()];
    }
  }
  return 0;
}

int IntegrationServiceInstance::initialize_all_data_server(int32_t count)
{
  int32_t iret = TFS_ERROR;
  DataServerStatInfo info;
  for (int32_t i = 0; i < count; ++i)
  {
    memset(&info, 0, sizeof(info));
    common::IpAddr* addr = reinterpret_cast<common::IpAddr*>(&info.id_);
    addr->ip_ = 0xfffff000 + i;
    addr->port_ = 0x0C80;//3200
    info.total_capacity_ = 0x4000000000000;//1TB
    info.current_load_   = random() % count;
    info.last_update_time_ = info.startup_time_ = time(NULL);
    info.status_ = common::DATASERVER_STATUS_ALIVE; 
    iret = add_server(info);
    if (iret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "initalize dataserver fail, must be exit!");
      return iret;
    }

  }
  return TFS_SUCCESS;
}

void IntegrationServiceInstance::DriverThreadHelper::run()
{
  while (!manager_.destroyed_)
  {
    if (!manager_.tasks_.empty())
    {
      IntegrationBaseCase* task = manager_.tasks_[random() % manager_.tasks_.size()];
      IntegrationBaseCase* tmp = task->new_object(random() % task->mode_count() + 1);
      if (manager_.main_work_queue_->execute(tmp) != 0)
      {
        tbsys::gDelete(tmp);
      }
      usleep(1);
    }
  }
}

KeepaliveTimerTask::KeepaliveTimerTask(IntegrationServiceInstance& instance)
  :instance_(instance)
{

}

KeepaliveTimerTask::~KeepaliveTimerTask()
{

}

void KeepaliveTimerTask::runTimerTask()
{
  instance_.keepalive();  
}


