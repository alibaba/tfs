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
#include <gtest/gtest.h>
#include "build_table.h"

using namespace std;
using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace rootserver
  {
    class BuildTableTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase(){}
        BuildTableTest(){}
        virtual ~BuildTableTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
        static const int32_t SERVER_COUNT;
        static void get_diff(BuildTable& tables);
    };

    void BuildTableTest::get_diff(BuildTable& tables)
    {
      int64_t new_count = 0;
      int64_t old_count = 0;
      tables.get_difference(new_count, old_count);
      TBSYS_LOG(INFO, "build table complete: new count: %"PRI64_PREFIX"d, old count: %"PRI64_PREFIX"d", new_count, old_count);
    }

    const int32_t BuildTableTest::SERVER_COUNT = 35;

    /*TEST_F(BuildTableTest, load_tables)
    {
      BuildTable tables;
      std::string file_path = "server_table";
      tables.fd_ = ::open(file_path.c_str(), O_RDWR|O_LARGEFILE|O_CREAT, 0644);
      int64_t file_size = 0;
      int64_t size = 0;
      int32_t iret = tables.load_tables(file_size, size);
      EXPECT_EQ(EXIT_MMAP_FILE_ERROR, iret);
      BuildTable::TablesHeader header;
      size = header.length() + (BuildTable::BUCKET_TABLES_COUNT * common::MAX_BUCKET_ITEM_DEFAULT) * INT64_SIZE 
        + (common::MAX_SERVER_ITEM_DEFAULT* INT64_SIZE);
      iret = tables.load_tables(file_size, size);
      EXPECT_EQ(common::MAX_SERVER_ITEM_DEFAULT, tables.header_->server_item_);
      EXPECT_EQ(common::MAX_BUCKET_ITEM_DEFAULT, tables.header_->bucket_item_);
      EXPECT_EQ(common::TABLE_VERSION_MAGIC, tables.header_->build_table_version_);
      EXPECT_EQ(common::TABLE_VERSION_MAGIC, tables.header_->active_table_version_);
      EXPECT_EQ(BuildTable::MAGIC_NUMBER, tables.header_->magic_number_);
      EXPECT_EQ(TFS_SUCCESS, iret);
      tables.destroy();

      struct stat st;
      memset(&st, 0, sizeof(st));
      tables.fd_ = ::open(file_path.c_str(), O_RDWR|O_LARGEFILE|O_CREAT, 0644);
      iret = ::fstat(tables.fd_, &st);
      EXPECT_EQ(0, iret);
      iret = tables.load_tables(st.st_size, size);
      EXPECT_EQ(common::MAX_SERVER_ITEM_DEFAULT, tables.header_->server_item_);
      EXPECT_EQ(common::MAX_BUCKET_ITEM_DEFAULT, tables.header_->bucket_item_);
      EXPECT_EQ(common::TABLE_VERSION_MAGIC, tables.header_->build_table_version_);
      EXPECT_EQ(common::TABLE_VERSION_MAGIC, tables.header_->active_table_version_);
      EXPECT_EQ(BuildTable::MAGIC_NUMBER, tables.header_->magic_number_);
      EXPECT_EQ(TFS_SUCCESS, iret);
      iret = tables.load_tables(st.st_size, 0x01);
      tables.destroy();

      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, initialize)
    {
      BuildTable tables;
      std::string file_path;
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(EXIT_PARAMETER_ERROR, iret);
      file_path = "/";
      iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_ERROR, iret);
      tables.destroy();
      file_path = "server_table";
      iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      tables.destroy();

      unlink(file_path.c_str());
    }*/

    TEST_F(BuildTableTest, build_table)
    {
      common::NEW_TABLE new_table;
      BuildTable tables;
      std::string file_path = "build_table_tables";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      int32_t i = 0;
      std::set<uint64_t> servers;
      for (i = 1; i <= SERVER_COUNT; ++i)
      {
        servers.insert(i);
      }
      bool change = false;
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);

      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      servers.insert(SERVER_COUNT + 1);//add 1
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      get_diff(tables);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);

      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);

      servers.erase(1);//del 1
      servers.insert(SERVER_COUNT+2);// add  1
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      get_diff(tables);

      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      servers.insert(SERVER_COUNT+3);//add 5
      servers.insert(SERVER_COUNT+4);//del 6 
      servers.insert(SERVER_COUNT+5);
      servers.insert(SERVER_COUNT+6);
      servers.insert(SERVER_COUNT+7);
      servers.insert(SERVER_COUNT+8);
      servers.erase(12);
      servers.erase(14);
      servers.erase(15);
      servers.erase(16);
      servers.erase(17);
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      get_diff(tables);

      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      servers.insert(SERVER_COUNT + 10);//add 1 
      servers.erase(20);//del 2
      servers.erase(21);
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      get_diff(tables);

      tables.destroy();
      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, check_update_table_complete)
    {
      NEW_TABLE_ITER iter;
      common::NEW_TABLE new_tables;
      std::set<uint64_t> servers;
      std::pair<NEW_TABLE_ITER, bool> res;
      for (int32_t i = 1; i <=SERVER_COUNT; ++i)
      {
        servers.insert(i);
      }
      std::set<uint64_t>::const_iterator it = servers.begin();
      for (; it != servers.end(); ++it)
      {
        iter = new_tables.find((*it));
        if (iter == new_tables.end())
        {
          res = new_tables.insert(NEW_TABLE::value_type((*it),
                NewTableItem()));
          res.first->second.phase_  = UPDATE_TABLE_PHASE_1;
          res.first->second.status_ = NEW_TABLE_ITEM_UPDATE_STATUS_BEGIN;
        }
      }

      BuildTable tables;
      std::string file_path = "check_update_table_complete";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);

      tables.header_->build_table_version_ = 1;

      int8_t phase = UPDATE_TABLE_PHASE_1;
      bool update_complete = false;

      iret = tables.check_update_table_complete(phase, new_tables, update_complete);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_FALSE(update_complete);

      uint64_t server = 1;
      int64_t version = 0;
      int8_t status = NEW_TABLE_ITEM_UPDATE_STATUS_END;
      iret = tables.update_tables_item_status(server, version, status, phase, new_tables);
      EXPECT_EQ(TFS_ERROR, iret);
      version = 1;
      iret = tables.update_tables_item_status(server, version, status, phase, new_tables);
      EXPECT_EQ(TFS_SUCCESS, iret);

      iret = tables.check_update_table_complete(phase, new_tables, update_complete);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_FALSE(update_complete);

      for (int32_t i = 1; i <=SERVER_COUNT; ++i)
      {
        iret = tables.update_tables_item_status(i, version, status, phase, new_tables);
        EXPECT_EQ(TFS_SUCCESS, iret);
      }
      iret = tables.check_update_table_complete(phase, new_tables, update_complete);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_TRUE(update_complete);

      status = NEW_TABLE_ITEM_UPDATE_STATUS_FAILED;
      iret = tables.update_tables_item_status(2, version, status, phase, new_tables);
      EXPECT_EQ(TFS_SUCCESS, iret);
      iret = tables.check_update_table_complete(phase, new_tables, update_complete);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_FALSE(update_complete);

      tables.destroy();
      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, switch_table)
    {
      common::NEW_TABLE new_table;
      BuildTable tables;
      std::string file_path = "switch_table";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      int32_t i = 0;
      std::set<uint64_t> servers;
      for (i = 1; i <= SERVER_COUNT; ++i)
      {
        servers.insert(i);
      }
      bool change = false;
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);

      tables.switch_table();
      EXPECT_EQ(1, tables.header_->active_table_version_);
      change = false;
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      tables.switch_table();
      EXPECT_EQ(2, tables.header_->active_table_version_);
      int64_t new_count = 0;
      int64_t old_count = 0;
      tables.get_difference(new_count, old_count);
      EXPECT_EQ(common::MAX_BUCKET_ITEM_DEFAULT, old_count);
      EXPECT_EQ(0, new_count);

      servers.erase(2);
      iret = tables.build_table(change, new_table, servers);
      EXPECT_EQ(TFS_SUCCESS, iret);
      tables.get_difference(new_count, old_count);
      EXPECT_GT(new_count , 0);

      tables.destroy();
      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, get_old_servers)
    {
      BuildTable tables;
      std::string file_path = "server_table";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      int32_t i = 0;

      std::set<uint64_t> servers;
      tables.get_old_servers(servers);
      EXPECT_EQ(0U, servers.size());
      for (i = 1; i <= SERVER_COUNT; i++)
      {
        tables.active_tables_[i] = i;
      }
      tables.get_old_servers(servers);
      EXPECT_EQ(SERVER_COUNT, static_cast<int32_t>(servers.size()));

      std::set<uint64_t>::const_iterator iter = servers.begin();
      for (i = 1; i <= SERVER_COUNT && iter != servers.end(); ++i, ++iter)
      {
        EXPECT_EQ(i, static_cast<int32_t>((*iter)));
      }

      tables.destroy();
      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, get_difference)
    {
      std::set<uint64_t> old_servers;
      std::set<uint64_t> new_servers;
      int32_t i = 0;
      for (i = 0; i < SERVER_COUNT; ++i)
      {
        old_servers.insert(i);
        new_servers.insert(i);
      }

      std::vector<uint64_t> sdeads;
      new_servers.erase(0);
      new_servers.erase(1);
      sdeads.push_back(0);
      sdeads.push_back(1);
      std::vector<uint64_t> snews;
      new_servers.insert(SERVER_COUNT);
      snews.push_back(SERVER_COUNT);

      std::vector<uint64_t> news;
      std::vector<uint64_t> deads;

      BuildTable tables;
      uint64_t diff = tables.get_difference(old_servers, new_servers, news, deads);
      EXPECT_EQ(snews.size() + sdeads.size(), diff);
      EXPECT_EQ(snews.size(), news.size());
      EXPECT_EQ(sdeads.size(), deads.size());
      std::sort(news.begin(), news.end());
      std::sort(snews.begin(), snews.end());
      std::sort(deads.begin(), deads.end());
      std::sort(sdeads.begin(), sdeads.end());
      std::vector<uint64_t>::const_iterator iter = news.begin();
      std::vector<uint64_t>::const_iterator it = snews.begin();
      for (; iter != news.end() && it != snews.end() ; ++iter, ++it)
      {
        EXPECT_EQ((*it), (*iter));
      }
      iter = deads.begin();
      it = sdeads.begin();
      for (; iter != deads.end() && it != sdeads.end() ; ++iter, ++it)
      {
        EXPECT_EQ((*it), (*iter));
      }
    }

    TEST_F(BuildTableTest, fill_old_tables)
    {
      BuildTable tables;
      std::string file_path = "old_server_tables";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      int32_t i = 0;
      std::set<uint64_t> servers;
      std::vector<uint64_t> news;
      std::vector<uint64_t> deads;
      for (i = 1; i <= SERVER_COUNT; ++i)
      {
        servers.insert(i);
        news.push_back(i);
      }
      iret = tables.fill_new_tables(servers, news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);

      servers.erase(1);
      news.clear();
      deads.push_back(1);
      news.push_back(12);
      servers.insert(12);
      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      iret = tables.fill_old_tables(news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      EXPECT_EQ(0U, deads.size());
      EXPECT_EQ(0U, news.size());
      get_diff(tables);

      deads.clear();
      news.clear();
      news.push_back(13);
      news.push_back(14);
      deads.push_back(12);
      servers.erase(12);
      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,);
      iret = tables.fill_old_tables(news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      EXPECT_EQ(0U, deads.size());
      EXPECT_EQ(1U, news.size());
      get_diff(tables);

      news.clear();
      deads.clear();
      deads.push_back(2);
      deads.push_back(3);
      news.push_back(15);
      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      iret = tables.fill_old_tables(news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      EXPECT_EQ(1U, deads.size());
      EXPECT_EQ(0U, news.size());
      get_diff(tables);

      tables.destroy();

      unlink(file_path.c_str());
    }

    TEST_F(BuildTableTest, fill_new_tables)
    {
      BuildTable tables;
      std::string file_path = "new_server_tables";
      int32_t iret = tables.intialize(file_path);
      EXPECT_EQ(TFS_SUCCESS, iret);
      int32_t i = 0;
      std::set<uint64_t> servers;
      std::vector<uint64_t> news;
      std::vector<uint64_t> deads;
      for (i = 1; i <= SERVER_COUNT; ++i)
      {
        servers.insert(i);
        news.push_back(i);
      }
      memset(tables.tables_, 0, common::MAX_BUCKET_DATA_LENGTH);
      iret = tables.fill_new_tables(servers, news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);

      news.push_back(SERVER_COUNT + 1);//new 1
      servers.insert(SERVER_COUNT + 1);
      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      iret = tables.fill_new_tables(servers, news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      get_diff(tables);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);

      servers.erase(1);
      deads.push_back(1);
      news.clear();
      memcpy((unsigned char*)tables.active_tables_, (unsigned char*)tables.tables_, tables.header_->bucket_item_ * INT64_SIZE);
      iret = tables.fill_new_tables(servers, news, deads);
      EXPECT_EQ(TFS_SUCCESS, iret);
      get_diff(tables);
      //tables.dump_tables(TBSYS_LOG_LEVEL_INFO,DUMP_TABLE_TYPE_TABLE);
      tables.destroy();
      unlink(file_path.c_str());
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
