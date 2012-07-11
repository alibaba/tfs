/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: global_factory.cpp 2139 2011-03-28 09:15:26Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "global_factory.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "server_collect.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    NsGlobalStatisticsInfo NsGlobalStatisticsInfo::instance_;
    NsRuntimeGlobalInformation NsRuntimeGlobalInformation::instance_;
    StatManager<std::string, std::string, StatEntry >GFactory::stat_mgr_;
    std::string GFactory::tfs_ns_stat_ = "tfs-ns-stat";
    //std::string GFactory::tfs_ns_stat_block_count_ = "tfs-ns-stat-block-count";

    int GFactory::initialize(tbutil::TimerPtr timer)
    {
      int32_t ret = stat_mgr_.initialize(timer);
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "initialize stat manager fail");
        return ret;
      }
      int64_t current = tbsys::CTimeUtil::getTime();
      StatEntry<std::string, std::string>::StatEntryPtr stat_ptr = new StatEntry<std::string, std::string>(tfs_ns_stat_, current, true);
      stat_ptr->add_sub_key("tfs-ns-read-success");
      stat_ptr->add_sub_key("tfs-ns-read-failed");
      stat_ptr->add_sub_key("tfs-ns-write-success");
      stat_ptr->add_sub_key("tfs-ns-write-failed");
      stat_ptr->add_sub_key("tfs-ns-unlink");

      stat_mgr_.add_entry(stat_ptr, SYSPARAM_NAMESERVER.dump_stat_info_interval_);

      /*StatEntry<std::string, std::string>::StatEntryPtr ptr = new StatEntry<std::string, std::string>(tfs_ns_stat_block_count_, current, false);
      ptr->add_sub_key("tfs-ns-block-count");
      stat_mgr_.add_entry(ptr, SYSPARAM_NAMESERVER.dump_stat_info_interval_);*/
      return ret;
    }

    int GFactory::wait_for_shut_down()
    {
      stat_mgr_.wait_for_shut_down();
      return common::TFS_SUCCESS;
    }

    int GFactory::destroy()
    {
      get_runtime_info().destroy();
      stat_mgr_.destroy();
      return common::TFS_SUCCESS;
    }
  }/** nameserver **/
}/** tfs **/
