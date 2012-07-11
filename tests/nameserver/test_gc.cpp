/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_gc.cpp 5 2012-04-01 15:44:56Z
 *
 * Authors:
 *   duanfei
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "global_factory.h"
#include "block_collect.h"
#include "gc.h"
#include "ns_define.h"
#include "nameserver.h"
#include "layout_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    class GCTest : public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          SYSPARAM_NAMESERVER.object_dead_max_time_ = 0xffff;
          SYSPARAM_NAMESERVER.object_clear_max_time_ = 0xff;
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {
          GFactory::destroy();
          GFactory::wait_for_shut_down();
        }
        GCTest():manager_(ns_){}
        ~GCTest(){}
        NameServer ns_;
        LayoutManager manager_;
    };

    TEST_F(GCTest, gcobject)
    {
      time_t now = Func::get_monotonic_time();
      GCObject object(now);
      EXPECT_FALSE(object.can_be_free(now));
      now += SYSPARAM_NAMESERVER.object_clear_max_time_ ;
      EXPECT_TRUE(object.can_be_clear(now));
    }

    TEST_F(GCTest, gc)
    {
      srand(time(NULL));
      time_t now = Func::get_monotonic_time();
      EXPECT_EQ(0U, manager_.get_gc_manager().size());
      BlockCollect* obj = new BlockCollect(100, now);
      EXPECT_EQ(TFS_SUCCESS, manager_.get_gc_manager().add(NULL));
      EXPECT_EQ(TFS_SUCCESS, manager_.get_gc_manager().add(obj));
      EXPECT_EQ(1U, manager_.get_gc_manager().size());
      EXPECT_EQ(TFS_SUCCESS, manager_.get_gc_manager().add(obj));
      EXPECT_EQ(1U, manager_.get_gc_manager().size());

      const uint32_t MAX_COUNT = random() % 512 + 256;
      for (uint32_t i = 1; i < MAX_COUNT; ++i)
      {
        obj = new BlockCollect(200 + i, now);
        EXPECT_EQ(TFS_SUCCESS, manager_.get_gc_manager().add(obj));
      }
      EXPECT_EQ(MAX_COUNT, manager_.get_gc_manager().size());
      manager_.get_gc_manager().gc(now);
      EXPECT_EQ(MAX_COUNT, manager_.get_gc_manager().size());
      now += SYSPARAM_NAMESERVER.object_clear_max_time_ ;
      manager_.get_gc_manager().gc(now);
      EXPECT_TRUE(manager_.get_gc_manager().size() != 0U);
      now += SYSPARAM_NAMESERVER.object_dead_max_time_ ;
      manager_.get_gc_manager().gc(now);
      EXPECT_EQ(0U, manager_.get_gc_manager().size());
    }
  }/** end namespace nameserver **/
}/** end tfs nameserver **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
