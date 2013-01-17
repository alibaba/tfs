/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: gc.cpp 2014 2011-01-06 07:41:45Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "tbsys.h"
#include "gc.h"
#include "common/error_msg.h"
#include "common/config_item.h"

using namespace tfs::common;
namespace tfs
{
namespace dataserver
{
  GCObjectManager GCObjectManager::instance_;

  GCObjectManager::GCObjectManager():
    is_init_(false), destroy_(false)
  {

  }

  GCObjectManager::~GCObjectManager()
  {

  }

  int GCObjectManager::add(GCObject* object)
  {
    bool bret = object == NULL ? false : destroy_ ? false : true;
    if (bret)
    {
      tbutil::Mutex::Lock lock(mutex_);
      std::list<GCObject*>::const_iterator iter = std::find(object_list_.begin(), object_list_.end(), object);
      if (iter == object_list_.end())
      {
        object_list_.push_back(object);
      }
    }
    return bret ? TFS_SUCCESS : TFS_ERROR;
  }

  void GCObjectManager::run()
  {
    int64_t now = time(NULL);
    GCObject* obj = NULL;
    tbutil::Mutex::Lock lock(mutex_);
    std::list<GCObject*>::iterator iter = object_list_.begin();
    while (iter != object_list_.end() && !destroy_)
    {
      if ((*iter)->is_dead(now))
      {
        obj = (*iter);
        obj->callback();
        obj->free();
        object_list_.erase(iter++);
      }
      else
      {
        ++iter;
      }
    }
  }

  int GCObjectManager::initialize(tbutil::TimerPtr timer)
  {
    ExpireTimerTaskPtr task = new ExpireTimerTask(*this);
    int iret = timer->scheduleRepeated(task, GC_WORKER_INTERVAL);
    is_init_ = true;
    return iret < 0 ? TFS_ERROR : TFS_SUCCESS;
  }

  int GCObjectManager::wait_for_shut_down()
  {
    tbutil::Mutex::Lock lock(mutex_);
    std::list<GCObject*>::iterator iter = object_list_.begin();
    for (; iter != object_list_.end(); ++iter)
    {
      (*iter)->callback();
      (*iter)->free();
    }
    object_list_.clear();
    return TFS_SUCCESS;
  }

  void GCObjectManager::destroy()
  {
    destroy_ = true;
  }

  void GCObjectManager::ExpireTimerTask::runTimerTask()
  {
    manager_.run();
  }
}
}
