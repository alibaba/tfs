/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bucket_manager.cpp 381 2011-09-07 14:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include "common/internal.h"
#include "meta_bucket_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace namemetaserver
  {
    Bucket::Bucket():
      version_(INVALID_TABLE_VERSION)
    {
      tables_.clear();
    }

    Bucket::~Bucket()
    {
      destroy();
    }

    int Bucket::initialize(const char* table, const int64_t length, const int64_t version)
    {
      int32_t iret = NULL == table || length <= 0 || length > MAX_BUCKET_DATA_LENGTH
        ? TFS_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = version <= version_ ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          tables_.clear();
          version_ = version;
          BucketItem item;
          std::pair<TABLES_ITERATOR, bool> res;
          const uint64_t* data = reinterpret_cast<const uint64_t*>(table);
          int64_t bucket_count = length / INT64_SIZE;
          for (int64_t index = 0; index < bucket_count; ++index)
          {
            item.server_ = data[index];
            item.status_ = BUCKET_STATUS_NONE;
            tables_.push_back(item);
          }
        }
      }
      return iret;
    }

    int Bucket::destroy(void)
    {
      tables_.clear();
      version_ = INVALID_TABLE_VERSION;
      return TFS_SUCCESS;
    }

    int Bucket::query(BucketStatus& status, const int64_t bucket_id,
          const int64_t version, const uint64_t server) const
    {
      int32_t iret = bucket_id < 0 || bucket_id >= MAX_BUCKET_ITEM_DEFAULT
          ? EXIT_BUCKET_ID_INVLAID : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = bucket_id < static_cast<int64_t>(tables_.size()) && !tables_.empty()  ? TFS_SUCCESS : EXIT_NEW_TABLE_INVALID;
        if (TFS_SUCCESS == iret)
        {
          iret = version != version_ ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            status = (TFS_SUCCESS == iret  && tables_[bucket_id].server_ == server)
                ? tables_[bucket_id].status_: BUCKET_STATUS_NONE;
          }
        }
      }
      return iret;
    }

    void Bucket::dump(int32_t level,
             const char* file, const int32_t line,
             const char* function) const
    {
      if (level >= TBSYS_LOGGER._level)
      {
        int64_t index = 0;
        std::map<uint64_t, std::vector<uint64_t> > map;
        std::map<uint64_t, std::vector<uint64_t> > ::iterator iter;
        TABLES_CONST_ITERATOR cn_it = tables_.begin();
        for (; cn_it != tables_.end(); ++cn_it, ++index)
        {
          iter = map.find((*cn_it).server_);
          if (iter == map.end())
            map.insert(std::map<uint64_t, std::vector<uint64_t> >::value_type((*cn_it).server_, std::vector<uint64_t>()));
          else
            (*iter).second.push_back(index);
        }
        iter = map.begin();
        std::vector<uint64_t>::const_iterator it;
        for (; iter != map.end(); ++iter)
        {
          int32_t count = 0;
          std::stringstream tstr;
          tstr << tbsys::CNetUtil::addrToString(iter->first) << ": " << "[ " <<iter->second.size() <<" ] ";
          for (it = iter->second.begin(); it != iter->second.end(); ++it, ++count)
          {
            tstr << " " << (*it) << " ";
          }
          tstr << std::endl;
          TBSYS_LOGGER.logMessage(level, file, line, function, "%s", tstr.str().c_str());
        }
      }
    }

    int Bucket::get_table(std::set<int64_t>& table, const uint64_t server)
    {
      int32_t index = 0;
      std::vector<BucketItem>::const_iterator iter = tables_.begin();
      for (; iter != tables_.end(); ++iter, ++index)
      {
        if ((*iter).server_ == server)
        {
          table.insert(index);
        }
      }
      return TFS_SUCCESS;
    }

    BucketManager::BucketManager()
    {

    }

    BucketManager::~BucketManager()
    {
      cleanup();
    }

    void BucketManager::cleanup(void)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      active_table_.destroy();
      update_table_.destroy();
    }

    int BucketManager::switch_table(std::set<int64_t>& change, const uint64_t server, const int64_t version)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      int32_t iret = server > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = ((version != update_table_.version_)
            || version <= active_table_.version_ ) ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          assert(!update_table_.empty());
          assert(update_table_.version_ >= active_table_.version_);
          Bucket::TABLES_ITERATOR iter;
          active_table_ = update_table_;
          iter = active_table_.tables_.begin();
          for (; iter != active_table_.tables_.end(); ++iter)
          {
            if ((*iter).server_ == server)
              (*iter).status_ = BUCKET_STATUS_RW;
            else
              (*iter).status_ = BUCKET_STATUS_NONE;
          }
          change = change_;
          change_.clear();
          update_table_.destroy();
        }
      }
      return iret;
    }

    int BucketManager::update_table(const char* table, const int64_t length, const int64_t version, const uint64_t server)
    {
      int32_t iret = NULL == table || length <= 0 || length > MAX_BUCKET_DATA_LENGTH
        ? TFS_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        iret = (version <= active_table_.version_  || version < update_table_.version_)
                   ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          iret = update_table_.initialize(table, length, version);
          if (TFS_SUCCESS == iret)
          {
            iret = update_new_table_status(server);
          }
        }
      }
      return iret;
    }

    int BucketManager::query(BucketStatus& status, const int64_t bucket_id,
          const int64_t version, const uint64_t server) const
    {
      int32_t iret = bucket_id < 0 || bucket_id >= MAX_BUCKET_ITEM_DEFAULT
          ? EXIT_BUCKET_ID_INVLAID : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        iret = version < active_table_.version_ ? EXIT_TABLE_VERSION_ERROR : TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "version: %ld, table version: %ld", version, active_table_.version_);
        if (TFS_SUCCESS == iret)
        {
          iret = active_table_.query(status, bucket_id, version, server);
          status = (TFS_SUCCESS == iret) ? status : BUCKET_STATUS_NONE;
        }
      }
      return iret;
    }

    int BucketManager::update_new_table_status(const uint64_t server)
    {
      int32_t iret = update_table_.empty() ? EXIT_NEW_TABLE_NOT_EXIST : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = update_table_.tables_.size() != active_table_.tables_.size()
               && !active_table_.tables_.empty() ? EXIT_NEW_TABLE_INVALID : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          int64_t index = 0;
          Bucket::TABLES_ITERATOR it = active_table_.tables_.begin();
          Bucket::TABLES_ITERATOR iter = update_table_.tables_.begin();
          if (!active_table_.tables_.empty())
          {
            change_.clear();
            assert(!active_table_.empty());
            assert(!update_table_.empty());
            assert(update_table_.tables_.size() == active_table_.tables_.size());
            for (; iter != update_table_.tables_.end()
                && it != active_table_.tables_.end(); ++iter, ++it, ++index)
            {
              if ((*iter).server_ == (*it).server_)
              {
                if ((*iter).server_ == server)
                  (*it).status_ = BUCKET_STATUS_RW;
              }
              else
              {
                if ((*iter).server_ == server)//move in
                {
                  (*it).status_ = BUCKET_STATUS_READ_ONLY;
                }
                else if ((*it).server_ == server)//move out
                {
                  (*it).status_ = BUCKET_STATUS_READ_ONLY;
                  change_.insert(index);
                }
                else
                {
                  (*it).status_ = BUCKET_STATUS_NONE;
                }
              }
            }
          }
        }
      }
      return iret;
    }

    bool BucketManager::bucket_version_valid(const int64_t new_version) const
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      return ((new_version != INVALID_TABLE_VERSION)
              && ((new_version == update_table_.version_ )
                  || (new_version == active_table_.version_)));
    }

    void BucketManager::dump(int32_t level,const int8_t type,
             const char* file, const int32_t line,
             const char* function) const
    {
      if (level > TBSYS_LOGGER._level)
      {
        if (DUMP_TABLE_TYPE_ACTIVE_TABLE == type)
          active_table_.dump(level, file, line, function);
        else
          update_table_.dump(level, file, line, function);
      }
    }


    int BucketManager::get_table(std::set<int64_t>& table, const uint64_t server)
    {
      return active_table_.get_table(table, server);
    }

    int64_t BucketManager::get_table_size(void) const
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      return active_table_.empty() ? update_table_.empty()
                                   ? 0 : update_table_.size() : active_table_.size();
    }
  }/** namemetaserver **/
}/** tfs **/


