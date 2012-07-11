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
#include <set>
#include <Time.h>
#include <gtest/gtest.h>
#include "common/lock.h"
#include "meta_server_manager.h"
#include "parameter.h"

using namespace std;
using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace rootserver
  {
    class MetaServerManagerTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
           SYSPARAM_RTSERVER.mts_rts_lease_expired_time_ = 2;
        }
        static void TearDownTestCase(){}
        MetaServerManagerTest(){}
        virtual ~MetaServerManagerTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
    };

    TEST_F(MetaServerManagerTest, exist)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      manager.initialize_ = true;
      EXPECT_FALSE(manager.exist(info.base_info_.id_));
      int32_t iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_TRUE(manager.exist(info.base_info_.id_));
    }

    TEST_F(MetaServerManagerTest, lease_exist)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      manager.initialize_ = true;
      int32_t iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      common::MetaServer* pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 
      EXPECT_TRUE(manager.lease_exist(info.base_info_.id_));
      sleep(3);
      EXPECT_FALSE(manager.lease_exist(info.base_info_.id_));
    }
    TEST_F(MetaServerManagerTest, register_)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      int32_t iret = manager.register_(info);
      EXPECT_EQ(TFS_ERROR, iret);
      common::MetaServer* pserver = manager.get(info.base_info_.id_);
      EXPECT_EQ(0, pserver);

      manager.initialize_ = true;
      iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 
      iret = manager.register_(info);
      EXPECT_EQ(EXIT_REGISTER_EXIST_ERROR, iret);
      pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 
    }

    TEST_F(MetaServerManagerTest, unregister)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      manager.initialize_ = true;
      int32_t iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      common::MetaServer* pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 

      iret = manager.unregister(info.base_info_.id_);
      EXPECT_EQ(TFS_SUCCESS, iret);
      iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 
    }

    TEST_F(MetaServerManagerTest, renew)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      manager.initialize_ = true;
      int32_t iret = manager.renew(info);
      EXPECT_EQ(EXIT_REGISTER_NOT_EXIST_ERROR, iret);
      iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);
      common::MetaServer* pserver = manager.get(info.base_info_.id_);
      EXPECT_TRUE(NULL != pserver);
      EXPECT_EQ(now, pserver->base_info_.start_time_); 

      iret = manager.renew(info);
      EXPECT_EQ(TFS_SUCCESS, iret);

      sleep(3);
      iret = manager.renew(info);
      EXPECT_EQ(EXIT_LEASE_EXPIRED, iret);
    }

    TEST_F(MetaServerManagerTest, check_ms_lease_expired)
    {
      time_t now = time(NULL);
      common::MetaServer info;
      memset(&info, 0, sizeof(info));
      info.base_info_.id_ = 0xfff;
      info.base_info_.start_time_ =  now;
      MetaServerManager manager;
      manager.initialize_ = true;
      int32_t iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);

      info.base_info_.id_++;
      iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);

      info.base_info_.id_++;
      iret = manager.register_(info);
      EXPECT_EQ(TFS_SUCCESS, iret);

      bool interrupt = false;
      tbutil::Time current = tbutil::Time::now(tbutil::Time::Monotonic); 
      manager.check_ms_lease_expired_helper(current, interrupt);

      sleep(3);
      current = tbutil::Time::now(tbutil::Time::Monotonic); 
      manager.check_ms_lease_expired_helper(current, interrupt);
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
