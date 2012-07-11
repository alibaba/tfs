/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id$
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "database_pool.h"
#include "mysql_database_helper.h"
#include "common/internal.h"
#include "common/parameter.h"
namespace tfs
{
  using namespace common;
  namespace namemetaserver
  {
    DataBasePool::DataBasePool():pool_size_(0)
    {
    }
    DataBasePool::~DataBasePool()
    {
      for (int i = 0; i < MAX_POOL_SIZE; i++)
      {
        if (NULL != base_info_[i].database_helper_)
        {
          if (base_info_[i].busy_flag_)
          {
            TBSYS_LOG(ERROR, "release not match with get");
          }
          delete base_info_[i].database_helper_;
          base_info_[i].database_helper_ = NULL;
        }
      }
      //mysql_thread_end();
    }
    bool DataBasePool::init_pool(const int32_t pool_size,
        char** conn_str,  char** user_name,
        char** passwd,  int32_t* hash_flag)
    {
      bool ret = true;
      for (int i = 0; i < pool_size_; i++)
      {
        if (NULL != base_info_[i].database_helper_)
        {
          delete base_info_[i].database_helper_;
        }
      }
      pool_size_ = pool_size;
      if (pool_size_ > MAX_POOL_SIZE)
      {
        pool_size_ = MAX_POOL_SIZE;
      } else if (pool_size_ < 1)
      {
        pool_size_ = 1;
      }
      for (int i = 0; i < pool_size_; i++)
      {
        base_info_[i].database_helper_ = new MysqlDatabaseHelper();
        if (TFS_SUCCESS != base_info_[i].database_helper_->set_conn_param(conn_str[i], user_name[i], passwd[i]))
        {
          ret = false;
          break;
        }
        TBSYS_LOG(DEBUG, "database_helper %d connn_str %s user_name %s passwd %s hashflag %d",
            i, conn_str[i], user_name[i], passwd[i], hash_flag[i]);
        base_info_[i].busy_flag_ = false;
        base_info_[i].hash_flag_ = hash_flag[i];
      }
      if (!ret)
      {
        for (int i = 0; i < pool_size_; i++)
        {
          base_info_[i].busy_flag_ = true;
        }
      }
      return ret;
    }

    bool DataBasePool::destroy_pool(void)
    {
      mysql_thread_end();
      return true;
    }
    DatabaseHelper* DataBasePool::get(const int32_t hash_flag)
    {
      DatabaseHelper* ret = NULL;
      if (0 == pool_size_)
      {
        TBSYS_LOG(ERROR, "pool size is 0");
      }
      else
      {
        while(NULL == ret)
        {
          {
            tbutil::Mutex::Lock lock(mutex_);
            for (int i = 0; i < pool_size_; i++)
            {
              if (!base_info_[i].busy_flag_ && base_info_[i].hash_flag_ == hash_flag)
              {
                ret = base_info_[i].database_helper_;
                base_info_[i].busy_flag_ = true;
                break;
              }
            }
          }
          if (NULL == ret)
          {
            usleep(500);
          }
        }
      }
      return ret;
    }
    void DataBasePool::release(DatabaseHelper* database_helper)
    {
      tbutil::Mutex::Lock lock(mutex_);
      for (int i = 0; i < pool_size_; i++)
      {
        if (base_info_[i].database_helper_ == database_helper)
        {
          if (base_info_[i].busy_flag_)
          {
            base_info_[i].busy_flag_ = false;
          }
          else
          {
            TBSYS_LOG(ERROR, "some error in your code");
          }
          break;
        }
      }
    }
    int32_t DataBasePool::get_hash_flag(const int64_t app_id, const int64_t uid)
    {
      int32_t hash_value = -1;
      if (0 == SYSPARAM_NAMEMETASERVER.db_infos_.size())
      {
        TBSYS_LOG(ERROR, "no database info");
      }
      else
      {
        HashHelper helper(app_id, uid);
        hash_value = tbsys::CStringUtil::murMurHash((const void*)&helper, sizeof(HashHelper))
          % SYSPARAM_NAMEMETASERVER.db_infos_.size();
      }
      return hash_value;
    }

  }
}
