/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bucket_manager.h 381 2011-09-07 14:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_NAME_META_BUCKET_MANAGER_H_
#define TFS_NAME_META_BUCKET_MANAGER_H_

#include <tbsys.h>
#include <Monitor.h>
#include <Mutex.h>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/rts_define.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace namemetaserver
  {
    struct BucketItem
    {
      uint64_t server_;
      common::BucketStatus status_;
    };

    class BucketManager;
    class Bucket
    {
      friend class BucketManager;
      typedef std::vector<BucketItem> TABLES;
      typedef TABLES::iterator TABLES_ITERATOR;
      typedef TABLES::const_iterator TABLES_CONST_ITERATOR;
    #ifdef GTEST_INCLUDE_GTEST_GTEST_H_
      friend class BucketTest;
      FRIEND_TEST(BucketTest, initialize);
      FRIEND_TEST(BucketTest, query);
      friend class BucketManagerTest;
      FRIEND_TEST(BucketManagerTest, switch_table);
      FRIEND_TEST(BucketManagerTest, update_table);
      FRIEND_TEST(BucketManagerTest, bucket_version_valid);
      FRIEND_TEST(BucketManagerTest, update_new_table_status);
    #endif
    public:
      Bucket();
      virtual ~Bucket();
      int initialize(const char* table, const int64_t length, const int64_t version);
      int destroy(void);
      int query(common::BucketStatus& status, const int64_t bucket_id,
                const int64_t version, const uint64_t server) const;
      void dump(int32_t level, const char* file = __FILE__,
             const int32_t line = __LINE__,
             const char* function = __FUNCTION__) const;
      inline bool empty() const { return tables_.empty();}
      inline int64_t size() const { return tables_.size();}
      int get_table(std::set<int64_t>& table, const uint64_t server);
    private:
      TABLES tables_;
      int64_t version_;
    };

    class BucketManager
    {
    #ifdef GTEST_INCLUDE_GTEST_GTEST_H_
      friend class BucketManagerTest;
      FRIEND_TEST(BucketManagerTest, switch_table);
      FRIEND_TEST(BucketManagerTest, update_table);
      FRIEND_TEST(BucketManagerTest, bucket_version_valid);
      FRIEND_TEST(BucketManagerTest, update_new_table_status);
    #endif
    public:
      BucketManager();
      virtual ~BucketManager();
      void cleanup(void);
      int switch_table(std::set<int64_t>& change, const uint64_t server, const int64_t version);
      int update_table(const char* table, const int64_t length, const int64_t version, const uint64_t server);
      int query(common::BucketStatus& status, const int64_t bucket_id,
                const int64_t version, const uint64_t server) const;
      bool bucket_version_valid(const int64_t new_version) const;

      int get_table(std::set<int64_t>& table, const uint64_t server);
      int64_t get_table_size(void) const;
      void dump(int32_t level,const int8_t type = common::DUMP_TABLE_TYPE_ACTIVE_TABLE,
             const char* file = __FILE__, const int32_t line = __LINE__,
             const char* function = __FUNCTION__) const;
    private:
      int update_new_table_status(const uint64_t server);
    private:
      tbutil::Monitor<tbutil::Mutex> monitor_;
      Bucket update_table_;
      Bucket active_table_;
      std::set<int64_t> change_;
    };
  }/** namemetaserver **/
}/** tfs **/

#endif

