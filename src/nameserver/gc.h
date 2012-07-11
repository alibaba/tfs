/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: gc.h 2140 2011-03-29 01:42:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMESERVER_GC_H_
#define TFS_NAMESERVER_GC_H_

#include <Timer.h>
#include <Mutex.h>
#include "ns_define.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    class GCObjectManager
    {
      #ifdef TFS_GTEST
      friend class GCTest;
      friend class LayoutManager;
      FRIEND_TEST(GCTest, add);
      FRIEND_TEST(GCTest, gc);
      #endif
      public:
      explicit GCObjectManager(LayoutManager& manager);
      virtual ~GCObjectManager();
      int add(GCObject* object, const time_t now = common::Func::get_monotonic_time());
      int gc(const time_t now);
      int64_t size() const;
      private:
      DISALLOW_COPY_AND_ASSIGN(GCObjectManager);
      LayoutManager& manager_;
      std::set<GCObject*>  wait_clear_list_;
      std::list<GCObject*> wait_free_list_;
      tbutil::Mutex mutex_;
      int64_t wait_free_list_size_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
#endif
