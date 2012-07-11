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
#include <Memory.hpp>
#include <time.h>
#include "lease_clerk.h"
#include "global_factory.h"

using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tbsys;

class LeaseEntryTest: public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    TBSYS_LOGGER.setLogLevel("debug");
    GFactory::initialize();
  }

  static void TearDownTestCase()
  {
    GFactory::destroy();
    GFactory::wait_for_shut_down();
  }

  LeaseEntryTest()
  {

  }
  ~LeaseEntryTest()
  {

  }
  virtual void SetUp()
  {

  }
  virtual void TearDown()
  {

  }
};

class LeaseClerkTest : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    TBSYS_LOGGER.setLogLevel("debug");
    GFactory::initialize();
  }
  static void TearDownTestCase()
  {
    GFactory::destroy();
    GFactory::wait_for_shut_down();
  }
  static uint32_t g_lease_id;
  static int64_t  g_client_id;
  LeaseClerkTest(){}
  ~LeaseClerkTest(){}
};

class LeaseFactoryTest: public ::testing::Test
{
  public:
  static void SetUpTestCase()
  {
    TBSYS_LOGGER.setLogLevel("debug");
    GFactory::initialize();
  }
  static void TearDownTestCase()
  {
    GFactory::destroy();
    GFactory::wait_for_shut_down();
  }
  static uint32_t g_lease_id;
  static int64_t  g_client_id;

  LeaseFactoryTest(){}
  ~LeaseFactoryTest(){}
};

uint32_t LeaseClerkTest::g_lease_id = 0x01;
int64_t LeaseClerkTest::g_client_id = 0xffff;

TEST_F(LeaseEntryTest, lease_entry)
{
  uint32_t lease_id = 0xff; 
  int64_t client_id = 0xffff;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  LeaseEntryPtr entry = new LeaseEntry(clerk, lease_id, client_id);
  EXPECT_EQ(lease_id, entry->id());
  EXPECT_EQ(client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());
  entry->change(LEASE_STATUS_FINISH);
  EXPECT_EQ(false, entry->is_valid_lease());
  entry->change(LEASE_STATUS_FAILED);
  EXPECT_EQ(false, entry->is_valid_lease());
  entry->change(LEASE_STATUS_EXPIRED);
  EXPECT_EQ(false, entry->is_valid_lease());
  entry->change(LEASE_STATUS_CANCELED);
  EXPECT_EQ(false, entry->is_valid_lease());
  entry->change(LEASE_STATUS_OBSOLETE);
  EXPECT_EQ(false, entry->is_valid_lease());

  bool check_time = false;
  tbutil::Time now = tbutil::Time::now();
  EXPECT_EQ(true, entry->is_remove(now, check_time));
  check_time = true;
  EXPECT_EQ(false, entry->is_remove(now, check_time));
  now += tbutil::Time::milliSeconds(LeaseEntry::LEASE_EXPIRE_REMOVE_TIME_MS + 3000);
  EXPECT_EQ(true, entry->is_remove(now, check_time));

  lease_id = 0xff + 0xff;
  entry->reset(lease_id);
  EXPECT_EQ(lease_id, entry->id());
  EXPECT_EQ(client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());

  entry->change(LEASE_STATUS_OBSOLETE);
  EXPECT_EQ(false, entry->is_valid_lease());
  EXPECT_EQ(true, entry->wait_for_expire());

  entry->change(LEASE_STATUS_RUNNING);
  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, entry->wait_for_expire());

}

TEST_F(LeaseClerkTest, add)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());

  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_YES;
  EXPECT_EQ(0U, clerk->add(0xff));
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;

  //no wait
  entry->change(LEASE_STATUS_FINISH);
  EXPECT_EQ(false, entry->is_valid_lease());
  lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  EXPECT_EQ(true, entry->is_valid_lease());

  //wait for expire
  lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  EXPECT_EQ(true, entry->is_valid_lease());

  //wait for expired
  sleep((LeaseEntry::LEASE_EXPIRE_TIME_MS/1000) + 1);

  clerk = 0;
}

TEST_F(LeaseClerkTest, find)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, clerk->remove(g_client_id));
  clerk = 0;
}

TEST_F(LeaseClerkTest, remove)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());

  EXPECT_EQ(true, clerk->remove(g_client_id));
  clerk = 0;
}


TEST_F(LeaseClerkTest, has_valid_lease)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, clerk->has_valid_lease(g_client_id));
  clerk->obsolete(g_client_id);
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));

  EXPECT_EQ(true, clerk->remove(g_client_id));
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  clerk = 0;
}

TEST_F(LeaseClerkTest, exist)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());

  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, clerk->has_valid_lease(g_client_id));
  EXPECT_EQ(true, clerk->exist(g_client_id));
  clerk->obsolete(g_client_id);
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  EXPECT_EQ(true, clerk->exist(g_client_id));

  EXPECT_EQ(true, clerk->remove(g_client_id));
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  EXPECT_EQ(false, clerk->exist(g_client_id));
  clerk = 0;
}

TEST_F(LeaseClerkTest, commit)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());

  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, clerk->has_valid_lease(g_client_id));
  EXPECT_EQ(true, clerk->exist(g_client_id));

  EXPECT_EQ(true, clerk->commit(g_client_id, g_lease_id, LEASE_STATUS_FINISH));
  EXPECT_EQ(LEASE_STATUS_FINISH, entry->status());
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));

  EXPECT_EQ(false, clerk->commit(g_client_id, g_lease_id, LEASE_STATUS_FINISH));
  EXPECT_EQ(false, clerk->commit(0, g_lease_id, LEASE_STATUS_FINISH));
  EXPECT_EQ(false, clerk->commit(g_client_id, 0, LEASE_STATUS_FINISH));
  clerk = 0;
}

TEST_F(LeaseClerkTest, clear)
{
  int32_t remove_threshold = 0x2;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  const int MAX_COUNT = 5;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    ++g_client_id;
    ++g_lease_id;
    uint32_t lease_id = clerk->add(g_client_id);
    EXPECT_EQ(g_lease_id, lease_id);
    LeaseEntryPtr entry = clerk->find(g_client_id);
    EXPECT_EQ(g_client_id, entry->client());
  }

  sleep((LeaseEntry::LEASE_EXPIRE_TIME_MS/1000) + 1);
  clerk->clear(false);
  clerk = 0;
}

TEST_F(LeaseClerkTest, finish_failed)
{
  ++g_client_id;
  int32_t remove_threshold = 0xffff;
  LeaseClerkPtr clerk = new LeaseClerk(remove_threshold);
  uint32_t lease_id = clerk->add(g_client_id);
  ++g_lease_id;
  EXPECT_EQ(g_lease_id, lease_id);
  LeaseEntryPtr entry = clerk->find(g_client_id);
  EXPECT_NE(0, entry);
  EXPECT_EQ(g_client_id, entry->client());
  EXPECT_EQ(LEASE_TYPE_WRITE, entry->type());
  EXPECT_EQ(LEASE_STATUS_RUNNING, entry->status());
  EXPECT_EQ(true, entry->is_valid_lease());
  EXPECT_EQ(true, clerk->has_valid_lease(g_client_id));
  entry->dump(g_lease_id, g_client_id, 0, 0);
  clerk->obsolete(g_client_id);
  entry->dump(g_lease_id, g_client_id, 0, 0);
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  entry->dump(g_lease_id, g_client_id, 0, 0);
  clerk->finish(g_lease_id);
  entry->dump(g_lease_id, g_client_id, 0, 0);
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  entry->dump(g_lease_id, g_client_id, 0, 0);
  clerk->failed(g_lease_id);
  entry->dump(g_lease_id, g_client_id, 0, 0);
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  EXPECT_EQ(true, clerk->remove(g_client_id));
  EXPECT_EQ(false, clerk->has_valid_lease(g_client_id));
  entry->dump(g_lease_id, g_client_id, 0, 0);
  clerk = 0;
}

TEST_F(LeaseFactoryTest,new_lease_id)
{
#ifndef UINT32_MAX 
#define UINT32_MAX 4294967295U
#endif
  LeaseFactory& instance = GFactory::get_lease_factory();
  uint32_t lease_id = LeaseFactory::lease_id_factory_;
  EXPECT_EQ(lease_id + 1, instance.new_lease_id());
  uint32_t tmp = UINT32_MAX - 1;
  atomic_exchange(&LeaseFactory::lease_id_factory_, tmp);
  lease_id = LeaseFactory::lease_id_factory_;
  EXPECT_EQ((lease_id + 1), instance.new_lease_id());
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
