#ifndef TFS_TESTS_NAMESERVER_INTEGRATION_INSTANCE_H
#define TFS_TESTS_NAMESERVER_INTEGRATION_INSTANCE_H
/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: integration_instance.h 2090 2011-02-16 06:52:58Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <tbnet.h>
#include <Timer.h>
#include <ThreadPool.h>
#include "common/define.h"
#include "common/internal.h"
#include "nameserver/layout_manager.h"
#include "nameserver/global_factory.h"
#include "integration_case.h"

namespace tfs
{
namespace integration 
{
struct DataServer
{
  common::DataServerStatInfo info_;
  std::map<uint32_t, common::BlockInfo> blocks_;
  bool need_sent_block_;
  std::vector<uint32_t> blocks_mapping_;
  tbutil::Mutex mutex_;
};

class IntegrationServiceInstance
{
  friend class tbutil::ThreadPoolWorkItem;
public:
  IntegrationServiceInstance();
  virtual ~IntegrationServiceInstance();

  int initialize(int32_t min_size, int32_t max_size, const std::string& conf);
  int wait_for_shut_down();
  bool destroy();

  int keepalive();

  bool add_task(tbutil::ThreadPoolWorkItem* workitem);

  int add_server(common::DataServerStatInfo& info);
  int remove_server(uint64_t id);
  DataServer* find_server(uint64_t id);
  uint64_t random_elect_server();
  int add_block(common::BlockInfo& info, std::vector<uint64_t>& servers);
  int remove_block(uint64_t id, uint32_t block_id);
  int update_block_info(common::BlockInfo& info, std::vector<uint64_t>& servers);

  uint32_t random_elect_block();

  uint32_t find_alive_block(int8_t mode);

private:
  int initialize_all_data_server(const int32_t count = 128);

   class DriverThreadHelper: public tbutil::Thread
   {
   public:
     DriverThreadHelper(IntegrationServiceInstance& manager):
       manager_(manager)
     {
      start();
     }
     virtual ~DriverThreadHelper(){}
     void run();
   private:
     IntegrationServiceInstance& manager_;
     DISALLOW_COPY_AND_ASSIGN(DriverThreadHelper);
   };
   typedef tbutil::Handle<DriverThreadHelper> DriverThreadHelperPtr;

public:
  bool destroyed_;
  tbutil::ThreadPool* main_work_queue_;
  tbutil::TimerPtr timer_;
  std::map<uint64_t, DataServer*> servers_;
  std::vector<uint64_t> servers_mapping_;
  nameserver::LayoutManager layout_manager_;
  tbutil::Mutex mutex_;
  std::vector<IntegrationBaseCase*> tasks_;
  common::StatManager<std::string, std::string, common::StatEntry > stat_mgr_;
  tbutil::Mutex gmutex_;

private:
  DriverThreadHelperPtr driver_thread_;
};

class KeepaliveTimerTask: public tbutil::TimerTask
{
public:
  KeepaliveTimerTask(IntegrationServiceInstance& instance);
  virtual ~KeepaliveTimerTask();

  void runTimerTask();
private:
  IntegrationServiceInstance& instance_;
};
typedef tbutil::Handle<KeepaliveTimerTask> KeepaliveTimerTaskPtr;

}
}

#endif

