/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_DATABASE_POOL_H_
#define TFS_NAMEMETASERVER_DATABASE_POOL_H_
#include <tbsys.h>
#include <Mutex.h>
#include "common/internal.h"
#include "common/meta_hash_helper.h"
namespace tfs
{
  namespace namemetaserver
  {
    class DatabaseHelper;
    class DataBasePool
    {
      public:
        enum
        {
          MAX_POOL_SIZE = 20,
        };
        DataBasePool();
        ~DataBasePool();
        bool init_pool(const int32_t pool_size,
            char** conn_str, char** user_name,
            char** passwd, int32_t* hash_flag);
        bool destroy_pool(void);
        DatabaseHelper* get(const int32_t hash_flag);
        void release(DatabaseHelper* database_helper);
        static int32_t get_hash_flag(const int64_t app_id, const int64_t uid);
      private:
        struct DataBaseInfo
        {
          DataBaseInfo():database_helper_(NULL), busy_flag_(true), hash_flag_(-1) {}
          DatabaseHelper* database_helper_;
          bool busy_flag_;
          int32_t hash_flag_;
        };
        DataBaseInfo base_info_[MAX_POOL_SIZE];
        int32_t pool_size_;
        tbutil::Mutex mutex_;
      private:
        DISALLOW_COPY_AND_ASSIGN(DataBasePool);
    };
  }
}
#endif
