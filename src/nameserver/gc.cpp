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
#include <execinfo.h>
#include "tbsys.h"
#include "gc.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "global_factory.h"
#include "layout_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    GCObjectManager::GCObjectManager(LayoutManager& manager):
      manager_(manager),
      wait_free_list_size_(0)
    {

    }

    GCObjectManager::~GCObjectManager()
    {
      std::set<GCObject*>::iterator iter = wait_clear_list_.begin();
      for (; iter != wait_clear_list_.end(); ++iter)
      {
        (*iter)->free();
      }
      std::list<GCObject*>::iterator it = wait_free_list_.begin();
      for (; it != wait_free_list_.end(); ++it)
      {
        (*it)->free();
      }
      wait_clear_list_.clear();
      wait_free_list_.clear();
    }

    int GCObjectManager::add(GCObject* object, const time_t now)
    {
      if (NULL != object)
      {
        tbutil::Mutex::Lock lock(mutex_);
        //TBSYS_LOG(DEBUG, "gc object list size: %zd, pointer: %p", wait_clear_list_.size(), object);
        std::pair<std::set<GCObject*>::iterator, bool> res = wait_clear_list_.insert(object);
        if (!res.second)
          TBSYS_LOG(INFO, "%p %p is exist", object, (*res.first));
        else
          object->update_last_time(now);
      }
      return TFS_SUCCESS;
    }

    int GCObjectManager::gc(const time_t now)
    {
      GCObject* obj = NULL;
      const int32_t MAX_GC_COUNT = 1024;
      GCObject* cleanups[MAX_GC_COUNT];
      ArrayHelper<GCObject*> cleanups_helper(MAX_GC_COUNT, cleanups);
      GCObject* objects[MAX_GC_COUNT];
      ArrayHelper<GCObject*> helper(MAX_GC_COUNT, objects);
      mutex_.lock();
      std::set<GCObject*>::iterator iter = wait_clear_list_.begin();
      while (iter != wait_clear_list_.end() && cleanups_helper.get_array_index() < MAX_GC_COUNT)
      {
        obj = (*iter);
        assert(NULL != obj);
        if (obj->can_be_clear(now))
        {
          cleanups_helper.push_back(obj);
          wait_clear_list_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      std::list<GCObject*>::iterator it = wait_free_list_.begin();
      while (it != wait_free_list_.end() && helper.get_array_index() < MAX_GC_COUNT)
      {
        obj = (*it);
        assert(NULL != obj);
        if (obj->can_be_free(now))
        {
          helper.push_back(obj);
          wait_free_list_.erase(it++);
        }
        else
        {
          ++it;
        }
      }
      wait_free_list_size_ += cleanups_helper.get_array_index();
      wait_free_list_size_ -= helper.get_array_index();
      mutex_.unlock();
      for (int32_t j = 0; j < cleanups_helper.get_array_index(); j++)
      {
        obj = *cleanups_helper.at(j);
        assert(NULL != obj);
        obj->callback(manager_);
        wait_free_list_.push_back(obj);
      }

      for (int32_t i = 0; i < helper.get_array_index(); ++i)
      {
        obj = *helper.at(i);
        assert(NULL != obj);
        //TBSYS_LOG(DEBUG, "gc pointer: %p", obj);
        obj->free();
      }
      return helper.get_array_index();
    }

    int64_t GCObjectManager::size() const
    {
      return wait_free_list_size_ + wait_clear_list_.size();
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
