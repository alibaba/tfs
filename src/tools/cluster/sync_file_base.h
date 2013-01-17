/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_base.h 1126 2011-12-07 03:35:00Z chuyu $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SYNCFILEBASE_H_
#define TFS_TOOLS_SYNCFILEBASE_H_

#include <stdio.h>
#include <stdlib.h>
#include <tbsys.h>
#include <TbThread.h>
#include <vector>
#include <string>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <Mutex.h>
#include <Memory.hpp>

#include "common/func.h"
#include "common/directory_op.h"
#include "new_client/fsname.h"
#include "new_client/tfs_client_impl.h"

enum ActionInfo
{
  WRITE_ACTION = 1,
  HIDE_SOURCE = 2,
  DELETE_SOURCE = 4,
  UNHIDE_SOURCE = 8,
  UNDELE_SOURCE = 16,
  HIDE_DEST = 32,
  DELETE_DEST = 64,
  UNHIDE_DEST = 128,
  UNDELE_DEST = 256
};
enum StatFlag
{
  DELETE_FLAG = 1,
  HIDE_FLAG = 4
};
typedef std::vector<ActionInfo> ACTION_VEC;
typedef std::vector<ActionInfo>::const_iterator ACTION_VEC_ITER;
static const int32_t MAX_READ_DATA_SIZE = 81920;

class SyncAction
{
  public:
  SyncAction() :
    trigger_index_(-1), force_index_(-1)
  {
    action_.clear();
  }
  void push_back(const ActionInfo& action)
  {
    action_.push_back(action);
  }

  std::string dump()
  {
    std::string action_str = "";
    ACTION_VEC_ITER iter = action_.begin();
    for (; iter != action_.end(); iter++)
    {
      switch (*iter)
      {
        case WRITE_ACTION:
          action_str += " rewrite";
          break;
        case HIDE_SOURCE:
          action_str += " hide_src";
          break;
        case DELETE_SOURCE:
          action_str += " dele_src";
          break;
        case UNHIDE_SOURCE:
          action_str += " unhide_src";
          break;
        case UNDELE_SOURCE:
          action_str += " undele_src";
          break;
        case HIDE_DEST:
          action_str += " hide_dest";
          break;
        case DELETE_DEST:
          action_str += " dele_dest";
          break;
        case UNHIDE_DEST:
          action_str += " unhide_dest";
          break;
        case UNDELE_DEST:
          action_str += " undele_dest";
          break;
      }
    }
    return action_str;
  }

  ACTION_VEC action_;
  int32_t trigger_index_;
  int32_t force_index_;
};

class SyncFileBase
{

  public:
    SyncFileBase();
    ~SyncFileBase();
    void initialize(const std::string& src_ns_addr, const std::string& dest_ns_addr);
    int cmp_file_info(const uint32_t block_id, const tfs::common::FileInfo& source_file_info, SyncAction& sync_action, const bool force, const int32_t modify_time);
    int cmp_file_info(const std::string& file_name, SyncAction& sync_action, const bool force, const int32_t modify_time);
    int do_action(const std::string& file_name, const SyncAction& sync_action);

  private:
    void fileinfo_to_filestat(const tfs::common::FileInfo& file_info, tfs::common::TfsFileStat& buf);
    int get_file_info(const std::string& tfs_client, const std::string& file_name, tfs::common::TfsFileStat& buf);
    int cmp_file_info_ex(const std::string& file_name, const tfs::common::TfsFileStat& source_buf,
        SyncAction& sync_action, const bool force, const int32_t modify_time);
    void change_stat(const int32_t source_flag, const int32_t dest_flag, SyncAction& SyncAction);
    int do_action_ex(const std::string& file_name, const ActionInfo& action);
    int copy_file(const std::string& file_name);

  private:
    std::string src_ns_addr_;
    std::string dest_ns_addr_;
};
#endif
