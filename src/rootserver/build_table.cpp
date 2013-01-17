/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: build_table.cpp 590 2011-08-18 13:40:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

/***
 * tables file format
 * ------------------------------------------------------------------
 * |magic number(8) | active table version(8)                       |
 * ------------------------------------------------------------------
 * |server item(8)  | bucket_item(8)                                |
 * ------------------------------------------------------------------
 * |build table version(8) | compress build table length(8)         |
 * ------------------------------------------------------------------
 * |compress active table length(8) | reserve(8)                    |
 * ------------------------------------------------------------------
 * |       active bucket tables                                     |
 * ------------------------------------------------------------------
 * |       bucket tables                                            |
 * ------------------------------------------------------------------
 * |       compress build tables                                    |
 * ------------------------------------------------------------------
 * |       compress active tables                                   |
 * ------------------------------------------------------------------
 */
#include <zlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sstream>
#include <Memory.hpp>
#include "common/func.h"
#include "common/atomic.h"
#include "common/error_msg.h"
#include "common/parameter.h"
#include "common/base_packet.h"
#include "common/client_manager.h"
#include "common/serialization.h"
#include "message/update_table_message.h"
#include "build_table.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace rootserver
  {
    const int64_t BuildTable::MAX_SERVER_COUNT = 1024;
    const int64_t BuildTable::MAGIC_NUMBER = 0x4e534654;//TFSN
    const int8_t BuildTable::BUCKET_TABLES_COUNT = 4;
    const int64_t BuildTable::MIN_BUCKET_ITEM = 1024;
    const int64_t BuildTable::MAX_BUCKET_ITEM = 102400;

    /*int BuildTable::TablesHeader::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        memcpy((data + pos), &magic_number_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &active_table_version_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &server_item_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &bucket_item_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &build_table_version_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &compress_table_length_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &compress_active_table_length_, INT64_SIZE);
        pos += INT64_SIZE;
      }
      return iret;
    }

    int BuildTable::TablesHeader::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        magic_number_ = (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        active_table_version_ = (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        server_item_  = (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        bucket_item_  = (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        build_table_version_= (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        compress_table_length_= (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
        compress_active_table_length_= (*reinterpret_cast<const int64_t*>(data + pos));
        pos += INT64_SIZE;
      }
      return iret;
    }*/

    int64_t BuildTable::TablesHeader::length() const
    {
      return common::INT64_SIZE * 8;
    }

    BuildTable::BuildTable():
      tables_(NULL),
      active_tables_(NULL),
      server_tables_(NULL),
      compress_tables_(NULL),
      compress_active_tables_(NULL),
      header_(NULL),
      fd_(-1),
      file_(NULL)
    {

    }

    BuildTable::~BuildTable()
    {
      destroy();
    }

    int BuildTable::intialize(const std::string& file_path, const int64_t max_bucket_item, const int64_t max_server_item)
    {
      int32_t iret = file_path.empty() ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        fd_ = ::open(file_path.c_str(), O_RDWR|O_LARGEFILE|O_CREAT, 0644);
        if (fd_ > 0)
        {
          struct stat st;
          memset(&st, 0, sizeof(st));
          iret = ::fstat(fd_, &st);
          if (0 == iret)
          {
            int64_t size = st.st_size;
            if (size <= 0)//empty file
            {
              if ((max_bucket_item < MIN_BUCKET_ITEM) || (max_bucket_item > MAX_BUCKET_ITEM)
                  || (max_server_item <= 0) || (max_server_item > MAX_SERVER_COUNT))
              {
                iret = TFS_ERROR;
                TBSYS_LOG(ERROR, "invalid parameter: bucket_item: %"PRI64_PREFIX"d, server_item: %"PRI64_PREFIX"d",
                    max_bucket_item, max_server_item);
              }
              else
              {
                TablesHeader header;
                size = header.length() + ( BUCKET_TABLES_COUNT * max_bucket_item) * INT64_SIZE
                  + (max_server_item * INT64_SIZE);
              }
            }
            iret = load_tables(st.st_size, size, max_bucket_item, max_server_item);
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "load tables fail, iret: %d", iret);
            }
          }
          else
          {
            iret = TFS_ERROR;
            TBSYS_LOG(ERROR, "stat file: %s fail, %s", file_path.c_str(), strerror(errno));
          }
        }
        else
        {
          iret = TFS_ERROR;
          TBSYS_LOG(ERROR, "open file: %s fail, %s", file_path.c_str(), strerror(errno));
        }
      }
      return iret;
    }

    int BuildTable::load_tables(const int64_t file_size, const int64_t size, const int64_t max_bucket_item, const int64_t max_server_item)
    {
      int32_t iret = size >= 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        MMapOption mo;
        mo.max_mmap_size_ = size;
        mo.first_mmap_size_ = size;
        mo.per_mmap_size_ = size;
        tbsys::gDelete(file_);
        file_ = new MMapFile(mo, fd_);
        iret = file_->map_file(true) ? TFS_SUCCESS : EXIT_MMAP_FILE_ERROR;
        if (TFS_SUCCESS == iret)
        {
          if (file_size <= 0)
          {
            memset(file_->get_data(), 0, size);
            iret = set_tables_pointer(max_bucket_item, max_server_item);
            header_->bucket_item_ = max_bucket_item;
            header_->build_table_version_ = TABLE_VERSION_MAGIC;
            header_->active_table_version_ = TABLE_VERSION_MAGIC;
            header_->magic_number_  = MAGIC_NUMBER;
            header_->server_item_   = max_server_item;
            iret = file_->sync_file() ? TFS_SUCCESS : TFS_ERROR;
          }
          else
          {
            header_ = reinterpret_cast<TablesHeader*>(file_->get_data());
            iret = header_->magic_number_ != MAGIC_NUMBER ? TFS_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              int64_t size = header_->length() + (BUCKET_TABLES_COUNT * header_->bucket_item_) * INT64_SIZE
                  + (header_->server_item_ * INT64_SIZE);
              iret = (header_->bucket_item_ == max_bucket_item
                    && header_->server_item_ == max_server_item)
                    && file_size == size ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                iret = set_tables_pointer(header_->bucket_item_, header_->server_item_);
              }
              else
              {
                TBSYS_LOG(ERROR, "load tables error:  %"PRI64_PREFIX"u <> %"PRI64_PREFIX"u",
                    header_->bucket_item_, max_bucket_item);
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "tables magic number error:  %"PRI64_PREFIX"u <> %"PRI64_PREFIX"u",
                  header_->magic_number_, MAGIC_NUMBER);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "mmap file: %s fail", strerror(errno));
        }
      }
      return iret;
    }

    int BuildTable::destroy()
    {
      int32_t iret = TFS_SUCCESS;
      if (NULL != file_)
      {
        iret = file_->munmap_file() ? TFS_SUCCESS : TFS_ERROR;
        tbsys::gDelete(file_);
      }
      if (fd_ > 0)
      {
        ::close(fd_);
      }
      return iret;
    }

    void BuildTable::get_old_servers(std::set<uint64_t>& servers)
    {
      int64_t i = 0;
      NEW_TABLE_ITER iter;
      std::set<uint64_t>::const_iterator it;
      for (i = 0; i < header_->bucket_item_ ; ++i)
      {
        if (0 != active_tables_[i])
        {
          servers.insert(active_tables_[i]);
        }
      }
    }

   int64_t BuildTable::get_difference(std::set<uint64_t>& old_servers,
                            const std::set<uint64_t>& new_servers, std::vector<uint64_t>& news,
                            std::vector<uint64_t>& deads)
   {
     deads.resize(old_servers.size());
     news.resize(new_servers.size());
     std::vector<uint64_t>::const_iterator iter = std::set_difference(
            old_servers.begin(), old_servers.end(), new_servers.begin(), new_servers.end(),
            deads.begin());
     std::vector<uint64_t>::const_iterator it = std::set_difference(
            new_servers.begin(), new_servers.end(), old_servers.begin(), old_servers.end(),
            news.begin());
     news.resize(it - news.begin());
     deads.resize(iter - deads.begin());
     return news.size() + deads.size();
   }

   int BuildTable::fill_old_tables( std::vector<uint64_t>& news,
                                  std::vector<uint64_t>& deads)
   {
     if (!news.empty() && !deads.empty())
     {
        std::vector<uint64_t>::iterator news_end = news.end();
        std::vector<uint64_t>::iterator deads_end = deads.end();
        std::vector<uint64_t>::iterator news_iter = news.begin();
        std::vector<uint64_t>::iterator deads_iter = deads.begin();
        std::vector<uint64_t>::iterator iter;

        int32_t diff = news.size() - deads.size();
        if (diff >= 0)
          news_end = news.begin() + deads.size();
        else
          deads_end = deads.begin() + news.size();
        int64_t index = 0;
        assert((news_end - news_iter) == (deads_end - deads_iter));
        std::map<uint64_t, uint64_t> map_table;
        std::map<uint64_t, uint64_t>::const_iterator it;
        while (news_iter != news_end && deads_iter != deads_end)
        {
          map_table[*deads_iter++] = *news_iter++;
        }
        news_iter = news.begin();
        deads_iter = deads.begin();

        for (index = 0; index < header_->bucket_item_; ++index)
        {
          iter = std::find(deads.begin(), deads_end, tables_[index]);
          if (deads_end != iter)
          {
            it = map_table.find(tables_[index]);
            assert(it != map_table.end());
            tables_[index] = it->second;
          }
        }
        news.erase(news.begin(), news_end);
        deads.erase(deads.begin(), deads_end);
     }
     return TFS_SUCCESS;
   }

    int BuildTable::fill_new_tables( const std::set<uint64_t>& servers,
                                    std::vector<uint64_t>& news, std::vector<uint64_t>& deads)
    {
      assert(!(!news.empty()&& !deads.empty()));
      int32_t iret = news.empty() && deads.empty() && servers.empty() ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        int64_t index = 0;
        std::vector<uint64_t>::const_iterator iter;
        std::set<uint64_t>::const_iterator it = servers.begin();
        if (news.empty()
            && !deads.empty())
        {
          for (index = 0; index < header_->bucket_item_ ; ++index)
          {
            iter = std::find(deads.begin(), deads.end(), tables_[index]);
            if (deads.end() != iter)
            {
              tables_[index] = *it++;
              if (it == servers.end())
                it = servers.begin();
            }
          }
        }
        if (!news.empty()
            && deads.empty())
        {
          iter = news.begin();
          int32_t new_min_bucket_count = header_->bucket_item_ / servers.size();
          int64_t replace_count = news.size() * (new_min_bucket_count + 1);
          int32_t diff = servers.size() - news.size();
          int32_t inc_bucket_count = diff <= 0 ? 0 : header_->bucket_item_ / diff - new_min_bucket_count;
          int32_t old_max_bucket_count = diff <= 0 ? 0 : header_->bucket_item_ / diff + 1;

          TBSYS_LOG(DEBUG, "new_min_bucket: %d, diff: %d, inc_bucket: %d, old_max_bucket_count: %d",
            new_min_bucket_count, diff, inc_bucket_count, old_max_bucket_count);
          std::map<uint64_t, std::vector<int32_t> > maps;
          std::map<uint64_t, std::vector<int32_t> >::iterator it;
          std::pair<std::map<uint64_t, std::vector<int32_t> >::iterator, bool> res;
          for (index = 0; index < header_->bucket_item_ ; ++index)
          {
            if (0 == tables_[index])
            {
              tables_[index] = *iter++;
              if (news.end() == iter)
                iter = news.begin();
            }
            else
            {
              it = maps.find(tables_[index]);
              if (maps.end() == it)
                it = maps.insert(std::map<uint64_t, std::vector<int32_t> >::value_type(
                  tables_[index], std::vector<int32_t>())).first;
              it->second.push_back(index);
            }
          }
          if (news.end() == iter)
              iter = news.begin();

          int64_t round = 0;
          while (replace_count > 0
            && !maps.empty())
          {
            it = maps.begin();
            for (; it != maps.end() && replace_count > 0; ++it, --replace_count)
            {
              assert(round < static_cast<int64_t>(it->second.size()));
              index = it->second[round];
              tables_[index] = *iter++;
              if (news.end() == iter)
                iter = news.begin();
            }
            ++round;
          }
        }
      }
      return iret;
    }

    int64_t BuildTable::get_difference(int64_t& new_count, int64_t& old_count)
    {
      new_count = 0;
      old_count = 0;
      int64_t index = 0;
      for (index = 0; index < header_->bucket_item_; ++index)
      {
        if (tables_[index] == active_tables_[index])
          ++old_count;
        else
          ++new_count;
      }
      return (old_count + new_count);
    }

    int BuildTable::build_table(bool& change, NEW_TABLE& new_tables, const std::set<uint64_t>& servers)
    {
      change = false;
      new_tables.clear();
      int32_t iret = servers.size() <= 0U ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        std::set<uint64_t> old_servers;

        get_old_servers(old_servers);

        std::vector<uint64_t> news;
        std::vector<uint64_t> deads;

        int64_t diff = get_difference(old_servers, servers, news, deads);
        if (diff > 0)
        {
          TBSYS_LOG(INFO, "total server size: %zd, new server size: %zd, dead server size: %zd", servers.size(), news.size(), deads.size());
          memcpy(reinterpret_cast<unsigned char*>(tables_), reinterpret_cast<unsigned char*>(active_tables_),
              header_->bucket_item_ * INT64_SIZE);
          if (!news.empty() && !deads.empty())
          {
            iret = fill_old_tables(news, deads);
          }
          if (TFS_SUCCESS == iret)
          {
            iret = fill_new_tables(servers, news, deads);
            if (TFS_SUCCESS == iret)
            {
              int64_t new_count = 0;
              int64_t old_count = 0;
              get_difference(new_count, old_count);
              change = new_count > 0;
              if (change)
              {
                //dump_tables(TBSYS_LOG_LEVEL_DEBUG,DUMP_TABLE_TYPE_TABLE);
                unsigned char* dest = new unsigned char[MAX_BUCKET_DATA_LENGTH];
                uint64_t dest_length = MAX_BUCKET_DATA_LENGTH;
                iret = compress(dest, &dest_length, reinterpret_cast<unsigned char*>(tables_), MAX_BUCKET_DATA_LENGTH);
                if (Z_OK != iret)
                {
                  TBSYS_LOG(ERROR, "compress error: %d, build version: %"PRI64_PREFIX"d", iret, header_->build_table_version_);
                  iret = TFS_ERROR;
                }
                else
                {
                  TBSYS_LOG(DEBUG, "compress dest_length: %"PRI64_PREFIX"d, length: %"PRI64_PREFIX"d",
                    dest_length, MAX_BUCKET_DATA_LENGTH);
                  NEW_TABLE_ITER iter;
                  std::pair<NEW_TABLE_ITER, bool> res;
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
                  inc_build_version();
                  header_->compress_table_length_ = dest_length;
                  memcpy(compress_tables_, dest, dest_length);
                  iret = file_->sync_file() ? TFS_SUCCESS : TFS_ERROR;
                }
                tbsys::gDeleteA(dest);
              }
            }
          }
        }
      }
      return iret;
    }

    int BuildTable::update_tables_item_status(const uint64_t server, const int64_t version,
                                              const int8_t status, const int8_t phase, common::NEW_TABLE& tables)
    {
      int32_t iret = server <= 0 ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        iret = version != header_->build_table_version_ ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          NEW_TABLE_ITER iter = tables.find(server);
          if (tables.end() != iter)
          {
            iret = phase != iter->second.phase_ ? TFS_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              iret = iter->second.status_ >= NEW_TABLE_ITEM_UPDATE_STATUS_RUNNING ?
                TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                iter->second.status_ = status;
              }
            }
          }
        }
      }
      return iret;
    }

    int BuildTable::update_table( const int8_t phase, common::NEW_TABLE& tables, bool& update_complete)
    {
      if (UPDATE_TABLE_PHASE_2 == phase)
      {
        NEW_TABLE_ITER iter = tables.begin();
        for (; iter != tables.end(); ++iter)
        {
          iter->second.status_ = NEW_TABLE_ITEM_UPDATE_STATUS_BEGIN;
          iter->second.begin_time_ = 0;
          iter->second.send_msg_time_ = 0;
          iter->second.phase_ = UPDATE_TABLE_PHASE_2;
        }
      }
      return check_update_table_complete(phase, tables, update_complete);
    }

    int BuildTable::check_update_table_complete(const int8_t phase,
                    common::NEW_TABLE& tables, bool& update_complete)
    {
      uint64_t success_count = 0;
      uint64_t error_count = 0;
      int32_t iret = tables.empty() ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        tbutil::Time now = tbutil::Time::now();
        tbutil::Time item_expired_time =
            tbutil::Time::milliSeconds(SYSPARAM_RTSERVER.mts_rts_lease_expired_time_ * 1000);
        tbutil::Time send_msg_expired_time = tbutil::Time::milliSeconds(500);
        NEW_TABLE_ITER iter = tables.begin();
        for (; iter != tables.end(); ++iter)
        {
          if (phase == iter->second.phase_)
          {
            if (NEW_TABLE_ITEM_UPDATE_STATUS_BEGIN == iter->second.status_)
            {
              iter->second.status_ = NEW_TABLE_ITEM_UPDATE_STATUS_RUNNING;
              iter->second.begin_time_ = now;
              iter->second.send_msg_time_ = now;
              send_msg_to_server(iter->first, phase);
            }
            else if (NEW_TABLE_ITEM_UPDATE_STATUS_RUNNING == iter->second.status_)
            {
              if ((iter->second.begin_time_ + item_expired_time) >= now)
              {
                iter->second.status_ = NEW_TABLE_ITEM_UPDATE_STATUS_FAILED;
                ++error_count;
              }
              else
              {
                if (iter->second.send_msg_time_ + send_msg_expired_time >= now)
                {
                  iter->second.send_msg_time_ = now;
                  send_msg_to_server(iter->first, phase);
                }
              }
            }
            else if (NEW_TABLE_ITEM_UPDATE_STATUS_END == iter->second.status_)
            {
              ++success_count;
            }
            else
            {
              ++error_count;
            }
          }
        }
      }
      TBSYS_LOG(DEBUG, "total: %zd, success: %"PRI64_PREFIX"u, error: %"PRI64_PREFIX"u",
          tables.size(), success_count, error_count);
      update_complete = (success_count == tables.size() && !tables.empty());
      return iret;
    }

    int BuildTable::switch_table(void)
    {
      int32_t iret = NULL != file_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        header_->compress_active_table_length_ = header_->compress_table_length_;
        memcpy(reinterpret_cast<unsigned char*>(active_tables_), reinterpret_cast<unsigned char*>(tables_),
            header_->bucket_item_ * INT64_SIZE);
        memcpy(compress_active_tables_, compress_tables_, header_->compress_table_length_);
        TBSYS_LOG(DEBUG, "old active version: %"PRI64_PREFIX"d", this->get_active_table_version());
        set_active_version();
        TBSYS_LOG(DEBUG, "new active version: %"PRI64_PREFIX"d", this->get_active_table_version());
        iret = file_->sync_file() ? TFS_SUCCESS : TFS_ERROR;
      }
      return iret;
    }
    void BuildTable::dump_tables(int32_t level,const int8_t type,
        const char* file, const int32_t line,
        const char* function) const
    {
      if (level >= TBSYS_LOGGER._level)
      {
        int64_t i = 0;
        uint64_t server = 0;
        std::map<uint64_t, std::vector<uint64_t> > map;
        std::map<uint64_t, std::vector<uint64_t> > ::iterator iter;
        for (i = 0; i < header_->bucket_item_; i++)
        {
          server = type == DUMP_TABLE_TYPE_ACTIVE_TABLE ? active_tables_[i] : tables_[i];
          iter = map.find(server);
          if (iter == map.end())
            iter = map.insert(std::map<uint64_t, std::vector<uint64_t> >::value_type(server, std::vector<uint64_t>())).first;
          iter->second.push_back(i);
        }
        iter = map.begin();
        std::vector<uint64_t>::const_iterator it;
        for (; iter != map.end(); ++iter)
        {
          std::stringstream tstr;
          tstr << tbsys::CNetUtil::addrToString(iter->first) << ": " << "[ " <<iter->second.size() <<" ] ";
          for (i = 0, it = iter->second.begin(); it != iter->second.end(); ++it, ++i)
          {
            tstr << " " << (*it) << " ";
          }
          tstr << std::endl;
          TBSYS_LOGGER.logMessage(level, file, line, function, "%s", tstr.str().c_str());
        }
      }
    }

    int BuildTable::dump_tables(common::Buffer& buf, const int8_t type)
    {
      UNUSED(buf);
      UNUSED(type);
      return TFS_SUCCESS;
    }

    int64_t BuildTable::get_build_table_version() const
    {
      return (NULL != file_ &&  NULL != header_)
          ? header_->build_table_version_
          : INVALID_TABLE_VERSION;
    }

    int64_t BuildTable::get_active_table_version() const
    {
      return (NULL != file_ &&  NULL != header_)
          ? header_->active_table_version_
          : INVALID_TABLE_VERSION;
    }

    const char* BuildTable::get_active_table() const
    {
      return (NULL != file_) ? reinterpret_cast<const char*>(compress_active_tables_) : NULL;
    }

    const char* BuildTable::get_build_table() const
    {
      return (NULL != file_) ? reinterpret_cast<const char*>(compress_tables_) : NULL;
    }

    int BuildTable::set_tables_pointer( const int64_t max_bucket_item, const int64_t max_server_item)
    {
      int32_t iret = NULL != file_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        header_ = reinterpret_cast<TablesHeader*>(file_->get_data());
        uint64_t* data = static_cast<uint64_t*>(file_->get_data());
        int64_t offset = header_->length()/ INT64_SIZE;
        server_tables_ = data + offset;
        offset += max_server_item ;
        active_tables_ = data + offset;
        offset += max_bucket_item ;
        tables_ = data + offset;
        offset += max_bucket_item ;
        compress_tables_ = reinterpret_cast<unsigned char*>(data + offset);
        offset += max_bucket_item;
        compress_active_tables_ = reinterpret_cast<unsigned char*>(data + offset);
      }
      return iret;
    }

    int BuildTable::send_msg_to_server(const uint64_t server, const int8_t phase)
    {
      int32_t iret = server > 0 && phase >= UPDATE_TABLE_PHASE_1 && phase <= UPDATE_TABLE_PHASE_2
                ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        UpdateTableMessage packet;
        packet.set_version(get_build_table_version());
        packet.set_phase(phase);
        if (UPDATE_TABLE_PHASE_1 == phase)
        {
          char* data = packet.alloc(get_build_table_length());
          iret = NULL == data ? TFS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            iret = NULL == get_build_table() ? TFS_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              memcpy(data, get_build_table(), get_build_table_length());
            }
          }
        }
        iret = post_msg_to_server(server, client, &packet, rs_async_callback);
      }
      return iret;
    }

    int BuildTable::update_active_tables(const unsigned char* tables, const int64_t length, const int64_t version)
    {
      int32_t iret = NULL == tables || length != MAX_BUCKET_DATA_LENGTH ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        TBSYS_LOG(DEBUG, "master version: %ld, slave version: %ld", version, header_->active_table_version_);
        iret = header_->active_table_version_  >= TABLE_VERSION_MAGIC ?
            version > INVALID_TABLE_VERSION ? TFS_SUCCESS : EXIT_TABLE_VERSION_ERROR :
            version >= header_->active_table_version_ ? TFS_SUCCESS : EXIT_TABLE_VERSION_ERROR;
        if (TFS_SUCCESS == iret)
        {
          header_->active_table_version_ = version;
          header_->build_table_version_  = version;
          memcpy((unsigned char*)active_tables_, tables, length);
        }
      }
      return iret;
    }

    void BuildTable::inc_build_version()
    {
      if (INVALID_TABLE_VERSION == header_->build_table_version_
          || header_->build_table_version_ >= TABLE_VERSION_MAGIC)
      {
        header_->build_table_version_ = 1;
      }
      else
      {
        ++header_->build_table_version_;
      }
    }

    void BuildTable::set_active_version()
    {
      header_->active_table_version_ = header_->build_table_version_;
    }

    /*void BuildTable::inc_active_version()
    {
      if (INVALID_TABLE_VERSION == header_->active_table_version_
          || header_->active_table_version_ >= TABLE_VERSION_MAGIC)
      {
        header_->active_table_version_ = 1;
      }
      else
      {
        ++header_->active_table_version_;
      }
    }*/
  } /** root server **/
}/** tfs **/
