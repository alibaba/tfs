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
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include "common/parameter.h"
#include "block_collect.h"
#include "server_collect.h"
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t BlockCollect::BLOCK_CREATE_FLAG_NO = 0;
    const int8_t BlockCollect::BLOCK_CREATE_FLAG_YES = 1;
    const int8_t BlockCollect::VERSION_AGREED_MASK = 2;
    BlockCollect::BlockCollect(const uint32_t block_id, const time_t now):
      GCObject(now),
      create_flag_(BLOCK_CREATE_FLAG_NO),
      in_replicate_queue_(BLOCK_IN_REPLICATE_QUEUE_NO)
    {
      servers_ = new (std::nothrow)ServerCollect*[SYSPARAM_NAMESERVER.max_replication_];
      assert(servers_);
      memset(servers_, 0, sizeof(ServerCollect*) * SYSPARAM_NAMESERVER.max_replication_);
      memset(&info_, 0, sizeof(info_));
      info_.block_id_ = block_id;
      info_.seq_no_ = 1;
    }

    BlockCollect::BlockCollect(const uint32_t block_id):
      GCObject(0),
      servers_(NULL)
    {
      info_.block_id_ = block_id;
      //for query
    }

    BlockCollect::~BlockCollect()
    {
      tbsys::gDeleteA(servers_);
    }

    bool BlockCollect::add(bool& writable, bool& master, ServerCollect*& invalid_server, const ServerCollect* server)
    {
      master = false;
      writable  = false;
      invalid_server = NULL;
      bool complete = false;
      bool ret = server != NULL;
      if (ret)
      {
        dump(TBSYS_LOG_LEVEL(DEBUG));
        writable = !is_full();
        ServerCollect** result = get_(server);//get server by pointer
        if (NULL == result)//not found
        {
          result = get_(server, false);//get server by id
          //根据ID能查询到Server结构，说明这个Server才下线不久又上线了，Block与这个Server的关系
          //还没有解除，有可能是遗漏了，需要等待GC进回调清理,这里我们可以解简单的先进行清理
          if (NULL != result)
          {
            invalid_server = *result;
            *result = NULL;
          }
          int8_t index = 0;
          int8_t random_index = random() % SYSPARAM_NAMESERVER.max_replication_;
          TBSYS_LOG(DEBUG, "random_index : %d, servers_size: %d", random_index, get_servers_size());
          for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; i++, ++random_index)
          {
            index = random_index % SYSPARAM_NAMESERVER.max_replication_;
            if (servers_[index] == NULL)
            {
              complete = true;
              servers_[index] = const_cast<ServerCollect*>(server);
              break;
            }
          }
        }
        if (complete)
          master = is_master(server);
        else
          ret = false;
      }
      return ret;
    }

    bool BlockCollect::remove(const ServerCollect* server, const time_t now, const int8_t flag)
    {
      update_last_time(now);
      if (NULL != server)
      {
        bool ret = false;
        ServerCollect* current = NULL;
        bool comp_id_result = false;
        bool comp_pointer_result = false;
        for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
        {
          current = servers_[i];
          comp_id_result = ((NULL != current) && (current->id() == server->id()));
          comp_pointer_result = (current == server);
          if (BLOCK_COMPARE_SERVER_BY_ID == flag)
            ret = comp_id_result;
          else if (BLOCK_COMPARE_SERVER_BY_POINTER == flag)
            ret = comp_pointer_result;
          else if (BLOCK_COMPARE_SERVER_BY_ID_POINTER == flag)
            ret = (comp_id_result && comp_pointer_result);
          else
            ret = false;
          if (NULL != current && ret)
            servers_[i] = NULL;
        }
      }
      return true;
    }

    bool BlockCollect::exist(const ServerCollect* const server, const bool pointer) const
    {
      bool ret = NULL != server;
      if (ret)
      {
        ServerCollect** result = get_(server, pointer);
        ret = ((NULL != result) && (NULL != *result));
      }
      return ret;
    }

    bool BlockCollect::is_master(const ServerCollect* const server) const
    {
      ServerCollect* first = servers_[0];
      return ((NULL != server) && (NULL != first)) ? server->id() == first->id() : false;
    }

    bool BlockCollect::is_writable() const
    {
      //TBSYS_LOG(DEBUG, "is_full: %d, size: %d", is_full(), get_servers_size());
      return ((!is_full()) && (get_servers_size() >= SYSPARAM_NAMESERVER.max_replication_));
    }

    bool BlockCollect::is_creating() const
    {
      return BLOCK_CREATE_FLAG_YES == create_flag_;
    }

    bool BlockCollect::in_replicate_queue() const
    {
      return BLOCK_IN_REPLICATE_QUEUE_YES == in_replicate_queue_;
    }

    void BlockCollect::get_servers(ArrayHelper<ServerCollect*>& servers) const
    {
      ServerCollect* pserver = NULL;
      for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        pserver = servers_[i];
        if (NULL != pserver && pserver->is_alive())
          servers.push_back(pserver);
      }
    }

    void BlockCollect::get_servers(std::vector<uint64_t>& servers) const
    {
      ServerCollect* pserver = NULL;
      for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        pserver = servers_[i];
        if (NULL != pserver && pserver->is_alive())
          servers.push_back(pserver->id());
      }
    }

    void BlockCollect::get_servers(std::vector<ServerCollect*>& servers) const
    {
      ServerCollect* pserver = NULL;
      for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        pserver = servers_[i];
        if (NULL != pserver && pserver->is_alive())
          servers.push_back(pserver);
      }
    }

    bool BlockCollect::check_version(LayoutManager& manager, common::ArrayHelper<ServerCollect*>& removes,
        bool& expire_self, common::ArrayHelper<ServerCollect*>& other_expires, ServerCollect*& invalid_server,
        const ServerCollect* server,const int8_t role, const bool isnew, const common::BlockInfo& info, const time_t now)
    {
      expire_self = false;
      invalid_server = NULL;
      bool ret = NULL != server && info_.block_id_ == info.block_id_;
      if (ret)
      {
        ServerCollect** result = get_(server);
        ret = NULL == result;
        if (ret)
        {
          result = get_(server, false);
          if (NULL != result)//这里处理方式和add一样
          {
            invalid_server = *result;
            *result = NULL;
          }
          int8_t size = get_servers_size();
          if (size >= SYSPARAM_NAMESERVER.max_replication_)
          {
            if ((info_.file_count_ > info.file_count_)
               || (info_.file_count_ == info.file_count_ && info_.size_ != info.size_))
            {
              expire_self = (role == NS_ROLE_MASTER);
              ret = false;
            }
          }
          if (ret)
          {
            //check block version
            if (std::abs(info_.version_ - info.version_) <= VERSION_AGREED_MASK)//version agreed
            {
              if (size >= SYSPARAM_NAMESERVER.max_replication_)
              {
                ServerCollect* servers[SYSPARAM_NAMESERVER.max_replication_ + 1];
                ArrayHelper<ServerCollect*> helper(SYSPARAM_NAMESERVER.max_replication_ + 1, servers);

                get_servers(helper);
                helper.push_back(const_cast<ServerCollect*>(server));
                ServerCollect* result = NULL;
                manager.get_server_manager().choose_excess_backup_server(result, helper);
                //这里不可能出现选不出的情况，也就是reuslt始终不会为NULL
                if (server == result)
                {
                  ret = false;
                  expire_self = (role == NS_ROLE_MASTER);//i'm master, we're going to expire blocks
                }
                else
                {
                  remove(result, now, BLOCK_COMPARE_SERVER_BY_ID_POINTER);//从当前拥有列表中删除
                  removes.push_back(result);//解除与dataserver的关系
                  if (role == NS_ROLE_MASTER)
                    other_expires.push_back(result);
                }
              }
              else
              {
                if (((info_.version_ > info.version_) && (size <= 0))
                    || (info_.version_ <= info.version_))
                {
                  info_ = info;
                  //memcpy(&info_, &info, sizeof(info_));
                }
              }
            }
            else
            {
              if (info_.version_ > info.version_)// nameserver version > dataserver version
              {
                if (size > 0)//has dataserver hold block, release
                {
                  TBSYS_LOG(INFO, "block: %u in dataserver: %s version error %d:%d",
                      info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                      info_.version_, info.version_);
                  expire_self = (role == NS_ROLE_MASTER);
                  ret = false;
                }
                else //we'll accept current version
                {
                  TBSYS_LOG(WARN, "block: %u in dataserver: %s version error %d:%d, but not found dataserver",
                      info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                      info_.version_, info.version_);
                  info_ = info;
                  //memcpy(&info_,&info, sizeof(info_));
                }
              }
              else if ( info_.version_ < info.version_) // nameserver version < dataserver version , we'll accept new version and release all dataserver
              {
                int32_t old_version = info_.version_;
                info_ = info;
                //memcpy(&info_, &info, sizeof(info_));
                if (!isnew)//release dataserver
                {
                  TBSYS_LOG(INFO, "block: %u in dataserver: %s version error %d:%d,replace ns version, current dataserver size: %u",
                      info.block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str(),
                      old_version, info.version_, size);
                  if (role == NS_ROLE_MASTER)
                  {
                    update_last_time(now);
                    ServerCollect* pserver = NULL;
                    for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
                    {
                      pserver = servers_[i];
                      servers_[i] = NULL;
                      if (NULL != pserver)
                      {
                        TBSYS_LOG(INFO, "release relation dataserver: %s, block: %u",
                            tbsys::CNetUtil::addrToString(pserver->id()).c_str(), info_.block_id_);
                        other_expires.push_back(pserver);
                        removes.push_back(pserver);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      return ret;
    }

    /**
     * to check a block if replicate
     * @return: -1: none, 0: normal, 1: emergency
     */
    PlanPriority BlockCollect::check_replicate(const time_t now) const
    {
      PlanPriority priority = PLAN_PRIORITY_NONE;
      if (BLOCK_CREATE_FLAG_YES != create_flag_)
      {
        int32_t size = get_servers_size();
        //TBSYS_LOG(DEBUG, "replicate block %u, size: %d, time: %ld, now: %ld",
        //  id(), size, last_update_time_, now);
        if (size <= 0)
        {
          TBSYS_LOG(WARN, "block: %u has been lost, do not replicate", info_.block_id_);
        }
        else if (size == 1 && SYSPARAM_NAMESERVER.max_replication_ > 1)
        {
          if (last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_ <= now)
          {
            TBSYS_LOG(DEBUG, "emergency replicate block: %u", info_.block_id_);
            priority = PLAN_PRIORITY_EMERGENCY;
          }
        }
        else if (size < SYSPARAM_NAMESERVER.max_replication_)
        {
          if (last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_ <= now)
          {
            TBSYS_LOG(DEBUG, "replicate block: %u", info_.block_id_);
            priority = PLAN_PRIORITY_NORMAL;
          }
        }
      }
      return priority;
    }

    /**
     * to check a block if compact
     * @return: return true if need compact
     */
    bool BlockCollect::check_compact() const
    {
      bool bret = false;
      if (BLOCK_CREATE_FLAG_YES != create_flag_)
      {
        int32_t size = get_servers_size();
        if ((size == SYSPARAM_NAMESERVER.max_replication_) && is_full())
        {
          if ((info_.file_count_ > 0)
              && (info_.size_ > 0)
              && (info_.del_file_count_ > 0)
              && (info_.del_size_ > 0))
          {
            int32_t delete_file_num_ratio =
              static_cast<int32_t>(100 * static_cast<float>(info_.del_file_count_) / static_cast<float>(info_.file_count_));
            int32_t delete_size_ratio =
              static_cast<int32_t>(100 * static_cast<float>(info_.del_size_) / static_cast<float>(info_.size_));
            if ((delete_file_num_ratio >  SYSPARAM_NAMESERVER.compact_delete_ratio_)
                || (delete_size_ratio > SYSPARAM_NAMESERVER.compact_delete_ratio_))
            {
              TBSYS_LOG(DEBUG, "block: %u need compact", info_.block_id_);
              bret = true;
            }
          }
        }
      }
      return bret;
    }

    bool BlockCollect::check_balance() const
    {
      return get_servers_size() >= SYSPARAM_NAMESERVER.max_replication_;
    }

    int BlockCollect::scan(SSMScanParameter& param) const
    {
      int16_t child_type = param.child_type_;
      bool has_dump = (child_type & SSM_CHILD_BLOCK_TYPE_FULL) ? is_full() : true;
      if (has_dump)
      {
        if (child_type & SSM_CHILD_BLOCK_TYPE_INFO)
        {
          param.data_.writeBytes(&info_, sizeof(info_));
        }
        if (child_type & SSM_CHILD_BLOCK_TYPE_SERVER)
        {
          param.data_.writeInt8(get_servers_size());
          for (int8_t i = 0; i < common::SYSPARAM_NAMESERVER.max_replication_; ++i)
          {
            ServerCollect* server = servers_[i];
            if (NULL != server)
            {
              param.data_.writeInt64(server->id());
            }
          }
        }
      }
      return has_dump ? TFS_SUCCESS : TFS_ERROR;
    }

    void BlockCollect::dump(int32_t level, const char* file, const int32_t line, const char* function) const
    {
      if (level >= TBSYS_LOGGER._level)
      {
        std::string str;
        ServerCollect* server = NULL;
        for (int8_t i = 0; i < common::SYSPARAM_NAMESERVER.max_replication_; ++i)
        {
          server = servers_[i];
          if (NULL != server)
          {
            str += CNetUtil::addrToString(servers_[i]->id());
            str += "/";
          }
        }
        TBSYS_LOGGER.logMessage(level, file, line, function,
            "block_id: %u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %d, servers: %s",
            info_.block_id_, info_.version_, info_.file_count_,
            info_.size_, info_.del_file_count_, info_.del_size_,
            info_.seq_no_, str.c_str());
      }
    }

    void BlockCollect::callback(LayoutManager& manager)
    {
      clear(manager,Func::get_monotonic_time());
    }

    bool BlockCollect::clear(LayoutManager& manager, const time_t now)
    {
      ServerCollect* server = NULL;
      for (int8_t i = 0; i < common::SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        server = servers_[i];
        if (NULL != server)
          manager.relieve_relation(this, server, now, BLOCK_COMPARE_SERVER_BY_ID);
      }
      return true;
    }

    int8_t BlockCollect::get_servers_size() const
    {
      int8_t size = 0;
      ServerCollect* server = NULL;
      for (int8_t i = 0; i < common::SYSPARAM_NAMESERVER.max_replication_; ++i)
      {
        server = servers_[i];
        if (NULL != server && server->is_alive())
          ++size;
      }
      return size;
    }

    ServerCollect** BlockCollect::get_(const ServerCollect* const server, const bool pointer) const
    {
      ServerCollect** result = NULL;
      if (NULL != server)
      {
        ServerCollect* current = NULL;
        for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_ && NULL == result; ++i)
        {
          current = servers_[i];
          if (pointer)
          {
            if (NULL != current && current == server)
              result = &servers_[i];
          }
          else
          {
            if (NULL != current && current->id() == server->id())
              result = &servers_[i];
          }
        }
      }
      return result;
    }
  }/** end namesapce nameserver **/
}/** end namesapce tfs **/
