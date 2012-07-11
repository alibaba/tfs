/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Timer.h>
#include <Memory.hpp>
#include <time.h>
#include "global_factory.h"
#include "BlockCollect.h"
#include "gc.h"
#include "ns_define.h"

using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tbutil;


enum IntervalStatus
{
  INTERVAL_STATUS_NONE = -1,
  INTERVAL_STATUS_INIT,
};

struct IntervalInformation
{
  IntervalStatus status_;
}; 

static IntervalInformation ginterval_info;

static TimerPtr gkeepalive_timer= 0;

class KeepaliveTimeTask : public TimerTask
{
public:
  virtual void runTimerTask()
  {

  }
};
typedef Handle<KeepaliveTimeTask> KeepaliveTimeTaskPtr;

class WorkThread : public Thread
{
public:

  int initilaize()
  {
    start();
    return TFS_SUCCESS;
  }
  
  void run()
  {
    while(ginterval_info.status_ > INTERVAL_STATUS_INIT)
    {
      sleep(2);
    }
  }

  void wait_for_shut_down()
  {
    join();
  }

  void destroy()
  {

  }
};
typedef Handle<WorkThread> WorkThreadPtr;

class IntervalIntegration
{
public:
  IntervalIntegration():
    work_thread_(NULL)
  {

  }
  static void SetUpTestCase()
  {
    SYSPARAM_NAMESERVER.object_dead_max_time_ = 0xffff;
    TBSYS_LOGGER.setLogLevel("debug");
    GFactory::initialize();
    gkeepalive_timer = new Timer();
    work_thread_ = new 
  }
  static void TearDownTestCase()
  {
    if (gkeepalive_timer!= 0)
      gkeepalive_timer= 0;
    tbsys::gDeleteA(gwork_thread);
    GFactory::destroy();
    GFactory::wait_for_shut_down();
  }
  IntervalIntegration(){}
  ~IntervalIntegration(){}
private:
  WorkThreadPtr* work_thread_;
};

int main(int argc, char* argv[])
{
  EXPECT_EQ(0, 1);
}
