/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: checkserver.cpp 746 2012-04-13 13:09:59Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <unistd.h>
#include <iostream>
#include <string>


#include "common/directory_op.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/checkserver_message.h"
#include "common/client_manager.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tbutil;
using namespace tbsys;

#include "checkserver.h"

namespace tfs
{
  namespace checkserver
  {
    void CheckThread::run()
    {
      for (uint32_t i = 0; i < ds_list_.size(); i++)
      {
        CheckBlockInfoVec check_ds_result;
        int ret = ServerHelper::check_ds(ds_list_[i],
            check_time_, last_check_time_, check_ds_result);
        if (TFS_SUCCESS == ret)  // error handled in ServerHelper
        {
          add_result_map(check_ds_result);
        }
      }
    }

    void CheckThread::add_result_map(const CheckBlockInfoVec& result_vec)
    {
      // add into result map
      result_map_lock_->lock();
      CheckBlockInfoVecConstIter iter = result_vec.begin();
      for ( ; iter != result_vec.end(); iter++)
      {
        CheckBlockInfoMapIter mit = result_map_->find(iter->block_id_);
        if (mit == result_map_->end())
        {
          CheckBlockInfoVec tmp_vec;
          tmp_vec.push_back(*iter);
          result_map_->insert(CheckBlockInfoMap::value_type(iter->block_id_, tmp_vec));
        }
        else
        {
          mit->second.push_back(*iter);
        }
      }
      result_map_lock_->unlock();
    }

    void RecheckThread::run()
    {
      uint64_t master_ns_id = check_server_->get_master_nsid();
      uint64_t slave_ns_id = check_server_->get_slave_nsid();
      for (uint32_t i = 0; i < block_list_.size(); i++)
      {
        CheckBlockInfoVec m_info_vec;
        CheckBlockInfoVec s_info_vec;
        int m_ret = ServerHelper::check_block(master_ns_id, block_list_[i], m_info_vec);
        int s_ret = ServerHelper::check_block(slave_ns_id, block_list_[i], s_info_vec);

        if (TFS_SUCCESS == m_ret && TFS_SUCCESS == s_ret)
        {
          CheckBlockInfo m_info = check_server_->select_main_block(m_info_vec);
          CheckBlockInfo s_info = check_server_->select_main_block(s_info_vec);
          check_server_->compare_block(m_info, s_info);
        }
        else if (TFS_SUCCESS == m_ret)
        {
          CheckBlockInfo m_info = check_server_->select_main_block(m_info_vec);

          // add to sync_to_slave list
          if (0 != m_info.file_count_)
          {
            TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
                m_info.block_id_, m_info.version_, m_info.file_count_, m_info.total_size_);
            check_server_->add_m_sync_list(block_list_[i]);
            TBSYS_LOG(INFO, "block %"PRI64_PREFIX"d NOT_IN_SLAVE, sync to slave", block_list_[i]);
          }
        }
        else if (TFS_SUCCESS == s_ret)
        {
          CheckBlockInfo s_info = check_server_->select_main_block(s_info_vec);

          // add to sync_to_master list
          if (0 != s_info.file_count_)
          {
            TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
                s_info.block_id_, s_info.version_, s_info.file_count_, s_info.total_size_);
            check_server_->add_s_sync_list(block_list_[i]);
            TBSYS_LOG(INFO, "block %"PRI64_PREFIX"d NOT_IN_MASTER, sync to master", block_list_[i]);
          }
        }
        else
        {
          // or block not in both clusters
          TBSYS_LOG(WARN, "block %"PRI64_PREFIX"d NOT_IN_CLUSTER", block_list_[i]);
        }
      }
    }

    /**
     * @brief help implementation
     */
    void CheckServer::help()
    {
      fprintf(stderr, "Usage: %s -f -i [-d] [-h]\n", "checkserver");
      fprintf(stderr, "       -f config file path\n");
      fprintf(stderr, "       -i cluster index\n");
      fprintf(stderr, "       -d daemonize\n");
      fprintf(stderr, "       -h help\n");
    }

    /**
     * @brief version information
     */
    void CheckServer::version()
    {
      printf("checkserver: version-2.1\n");
    }

    /**
     * @brief parse command line
     *
     * @param argc: number of args
     * @param argv[]: arg list
     * @param errmsg: error message
     *
     * @return
     */
    int CheckServer::parse_common_line_args(int argc, char* argv[], std::string& errmsg)
    {
      // analyze arguments
      int ret = TFS_SUCCESS;
      int ch = 0;
      string index_str;
      while ((ch = getopt(argc, argv, "i:")) != EOF)
      {
        switch (ch)
        {
          case 'i':
            index_str = optarg;
            index_ = atoi(optarg);
            break;
          default:
            errmsg = "unknown options";
            ret = TFS_ERROR;
            help();
        }
      }

      if (index_ <= 0)
      {
        ret = TFS_ERROR;
      }
      else
      {
        snprintf(app_name_, MAX_FILE_NAME_LEN, "%s_%s", argv[0], index_str.c_str());
        argv[0] = app_name_;  // ugly trick
      }

      return ret;
    }

    int CheckServer::init_meta()
    {
      // init meta file
      int ret = TFS_SUCCESS;
      string meta_path = meta_dir_ + "/cs.meta";
      meta_fd_ = open(meta_path.c_str(), O_CREAT | O_RDWR, 0644);
      if (meta_fd_ < 0)
      {
        TBSYS_LOG(ERROR, "open meta file %s error, ret: %d",
            meta_path.c_str(), -errno);
        ret = TFS_ERROR;
      }
      else
      {
        // maybe an empty file, but it doesn't matter
        int rlen = read(meta_fd_, &last_check_time_, sizeof(uint32_t));
        if (rlen < 0)
        {
          TBSYS_LOG(ERROR, "read mete file %s error", meta_path.c_str());
          ret = TFS_ERROR;
        }
      }

      return ret;
    }

    void CheckServer::update_meta()
    {
      // it doesn't matter if fail
      if (meta_fd_ >= 0)
      {
        pwrite(meta_fd_, &last_check_time_, sizeof(uint32_t),  0);
        fsync(meta_fd_);
      }
    }

    int CheckServer::open_file(const uint32_t check_time)
    {
      int ret = TFS_SUCCESS;
      string m_blk, s_blk;
      char time_str[64];
      CTimeUtil::timeToStr(check_time, time_str);
      m_blk = meta_dir_ + "/sync_to_slave.blk." + time_str;
      s_blk = meta_dir_ + "/sync_to_master.blk." + time_str;

      if (NULL == mfp_)
      {
        mfp_ = fopen(m_blk.c_str(), "a+");
        if (NULL == mfp_)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "open %s error, ret: %d", m_blk.c_str(), ret);
        }
      }

      if (TFS_SUCCESS == ret && NULL == sfp_)
      {
        sfp_ = fopen(s_blk.c_str(), "a+");
        if (NULL == sfp_)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "open %s error, ret: %d", s_blk.c_str(), ret);
        }
      }

      return ret;
    }

    void CheckServer::close_file()
    {
      if (NULL != mfp_)
      {
        fclose(mfp_);
        mfp_ = NULL;
      }
      if (NULL != sfp_)
      {
        fclose(sfp_);
        sfp_ = NULL;
      }
    }

    int CheckServer::initialize()
    {
      // load check server config
      int ret = SYSPARAM_CHECKSERVER.initialize(config_file_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load config file fail.");
      }
      else
      {
        // initialize parameter
        master_ns_id_ = SYSPARAM_CHECKSERVER.master_ns_id_;
        slave_ns_id_ = SYSPARAM_CHECKSERVER.slave_ns_id_;
        thread_count_ = SYSPARAM_CHECKSERVER.thread_count_;
        check_interval_ = SYSPARAM_CHECKSERVER.check_interval_ * 60;
        overlap_check_time_ = SYSPARAM_CHECKSERVER.overlap_check_time_ * 60;
        block_stable_time_ = SYSPARAM_CHECKSERVER.block_stable_time_ * 60;

        // initialize meta direcotry
        stringstream tmp_stream;
        tmp_stream << get_work_dir() << "/checkserver_" << index_;
        tmp_stream >> meta_dir_;
        if (!DirectoryOp::create_full_path(meta_dir_.c_str()))
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "create meta path fail");
        }

        if (TFS_SUCCESS == ret)
        {
          ret = init_meta();
        }

        if (TFS_SUCCESS == ret)
        {
          ret = NewClientManager::get_instance().initialize(factory_, streamer_);
        }
      }
      return ret;
    }

  int CheckServer::run(int argc, char* argv[])
  {
    UNUSED(argc);
    UNUSED(argv);
    int ret = initialize();
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "initialize error, ret: %d", ret);
    }
    else
    {
      ret = run_check();
    }

    return ret;
  }

  int CheckServer::run_check()
  {
    int ret = TFS_SUCCESS;
    while (false == stop_)
    {
      ret = check_logic_cluster();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "check cluster fail.");
      }
      else
      {
        TBSYS_LOG(INFO, "check cluster success.");
      }
      sleep(check_interval_);
    }
    return ret;
  }

  int CheckServer::check_logic_cluster()
  {
    TBSYS_LOG(INFO, "[check cluster begin]");

    int ret = 0;
    uint32_t check_time = time(NULL);
    CheckBlockInfoMap master_check_result;
    CheckBlockInfoMap slave_check_result;

    ret = open_file(check_time);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "open file fail");
    }
    else
    {
      // check master
      if (TFS_SUCCESS == ret && 0 != master_ns_id_)
      {
        ret = check_physical_cluster(master_ns_id_, check_time, master_check_result);
      }

      // check slave
      if (TFS_SUCCESS == ret && 0 != slave_ns_id_)
      {
        ret = check_physical_cluster(slave_ns_id_, check_time, slave_check_result);
      }

      // diff master & slave
      if (TFS_SUCCESS == ret && 0 != master_ns_id_ && 0 != slave_ns_id_)
      {
        common::VUINT recheck_list;
        compare_cluster(master_check_result, slave_check_result, recheck_list);
        recheck_block(recheck_list);
      }

      close_file();
    }

    // update meta on success
    if (ret == TFS_SUCCESS)
    {
      last_check_time_ = check_time;
      update_meta();
    }

    TBSYS_LOG(INFO, "[check cluster end]");
    return ret;
  }

  CheckBlockInfo& CheckServer::select_main_block(CheckBlockInfoVec& block_infos)
  {
    assert(block_infos.size() > 0);
    int idx = 0;
    uint32_t max_count = block_infos[0].file_count_;
    int max_version = block_infos[0].version_;
    for (uint32_t i = 1; i < block_infos.size(); i++)
    {
      // version, file_count
      if (block_infos[i].version_ > max_version ||
          (block_infos[i].version_ == max_version &&
           block_infos[i].file_count_ > max_count))
      {
        max_count = block_infos[i].file_count_;
        max_version = block_infos[i].version_;
        idx = i;
      }
    }
    return block_infos[idx];
  }

  int CheckServer::check_physical_cluster(const uint64_t ns_id, const uint32_t check_time,
      CheckBlockInfoMap& cluster_result)
  {
    int ret = TFS_SUCCESS;
    VUINT64 ds_list;
      ret = ServerHelper::get_ds_list(ns_id, ds_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get dataserver list error, ret: %d", ret);
      }
      else
      {
        // compute check range for ds
        uint32_t low_bound = 0;
        uint32_t high_bound = check_time - block_stable_time_;
        if (0 != last_check_time_)
        {
          low_bound = last_check_time_ - block_stable_time_ - overlap_check_time_;
        }

        tbutil::Mutex result_map_lock;
        CheckThreadPtr* work_threads = new CheckThreadPtr[thread_count_];
        for (int i = 0; i < thread_count_; i++)
        {
          work_threads[i] = new CheckThread(&cluster_result, &result_map_lock);
          work_threads[i]->set_check_time(high_bound);
          work_threads[i]->set_last_check_time(low_bound);
        }

        // dispatch task to multi thread
        for (uint32_t i = 0; i < ds_list.size(); i++)
        {
          int idx = i % thread_count_;
          work_threads[idx]->add_ds(ds_list[i]);
        }

        for (int i = 0; i < thread_count_; i++)
        {
          try
          {
            work_threads[i]->start();
          }
          catch (exception& e)
          {
            TBSYS_LOG(ERROR, "start thread exception: %s", e.what());
          }
        }

        for (int i = 0; i < thread_count_; i++)
        {
          try
          {
            work_threads[i]->join();
          }
          catch (exception& e)
          {
            TBSYS_LOG(ERROR, "join thread exception: %s", e.what());
          }
        }

        tbsys::gDeleteA(work_threads);
      }

      return ret;
    }

    void CheckServer::recheck_block(const VUINT& recheck_block)
    {
      RecheckThreadPtr* work_threads = new RecheckThreadPtr[thread_count_];
      for (int i = 0; i < thread_count_; i++)
      {
        work_threads[i] = new RecheckThread(this);
      }

      // dispatch task to multi thread
      for (uint32_t i = 0; i < recheck_block.size(); i++)
      {
        int idx = i % thread_count_;
        work_threads[idx]->add_block(recheck_block[i]);
      }

      for (int i = 0; i < thread_count_; i++)
      {
        try
        {
          work_threads[i]->start();
        }
        catch (exception e)
        {
          TBSYS_LOG(ERROR, "start thread exception: %s", e.what());
        }
      }

      for (int i = 0; i < thread_count_; i++)
      {
        try
        {
          work_threads[i]->join();
        }
        catch (exception e)
        {
          TBSYS_LOG(ERROR, "join thread exception: %s", e.what());
        }
      }

      tbsys::gDeleteA(work_threads);
    }

    void CheckServer::compare_block(const CheckBlockInfo& left, const CheckBlockInfo& right)
    {
      int ret = 0;
      uint32_t block_id = left.block_id_;
      TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
          left.block_id_, left.version_, left.file_count_, left.total_size_);
      TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
          right.block_id_, right.version_, right.file_count_, right.total_size_);

      if (left.file_count_ == right.file_count_ &&
            left.total_size_ == right.total_size_)
      {
        ret = 0;
      }
      else
      {
        if (left.version_ > right.version_)
        {
          ret = 1;
        }
        else if(left.version_ < right.version_)
        {
          ret = -1;
        }
        else
        {
          if (left.file_count_ >= right.file_count_)
          {
            ret = 1;
          }
          else
          {
            ret = -1;
          }
        }
      }

      if (ret > 0)
      {
        TBSYS_LOG(INFO, "block %u DIFF, sync to slave", block_id);
        add_m_sync_list(block_id);
      }
      else if (ret < 0)
      {
        TBSYS_LOG(INFO, "block %u DIFF, sync to master", block_id);
        add_s_sync_list(block_id);
      }
      else
      {
        TBSYS_LOG(INFO, "block %u SAME", block_id);
      }
    }

    void CheckServer::compare_cluster(CheckBlockInfoMap& master_result,
        CheckBlockInfoMap& slave_result, common::VUINT& recheck_list)
    {
      CheckBlockInfoMapIter iter = master_result.begin();
      for ( ; iter != master_result.end(); iter++)
      {
        CheckBlockInfo& m_result = select_main_block(iter->second);
        CheckBlockInfoMapIter target = slave_result.find(m_result.block_id_);
        if (target == slave_result.end())
        {
          // may not in slave, if block not empty, recheck
          if (0 != m_result.file_count_)
          {
            TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
                m_result.block_id_, m_result.version_, m_result.file_count_, m_result.total_size_);
            TBSYS_LOG(DEBUG, "may not in slave, recheck");
            recheck_list.push_back(iter->first);
          }
        }
        else
        {
          CheckBlockInfo& s_result = select_main_block(target->second);
          compare_block(m_result, s_result);
          slave_result.erase(target);
        }
      }

      // may not exist in master
      iter = slave_result.begin();
      for ( ; iter != slave_result.end(); iter++)
      {
        CheckBlockInfo& item = select_main_block(iter->second);
        if (0 != item.file_count_)
        {
          TBSYS_LOG(DEBUG, "id: %u, version: %d, count: %u, size: %u",
              item.block_id_, item.version_, item.file_count_, item.total_size_);
          TBSYS_LOG(DEBUG, "may not in master, recheck");
          recheck_list.push_back(iter->first);
        }
      }
    }
  }
}

using namespace tfs::checkserver;

int main(int argc, char** argv)
{
  CheckServer check_server;
  return check_server.main(argc, argv);
}

