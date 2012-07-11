/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_log.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
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
#include "new_client/tfs_client_api.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

typedef set<string> FILE_SET;
typedef set<string>::iterator FILE_SET_ITER;
typedef map<uint32_t, FILE_SET> SYNC_FILE_MAP;
typedef map<uint32_t, FILE_SET>::iterator SYNC_FILE_MAP_ITER;
typedef vector<int32_t> ACTION_VEC;
typedef vector<int32_t>::const_iterator ACTION_VEC_ITER;
struct ActionOp
{
  ACTION_VEC action_;
  int32_t trigger_index_;
  int32_t force_index_;
  ActionOp() :
    trigger_index_(-1), force_index_(-1)
  {
    action_.clear();
  }
  void push_back(int32_t action)
  {
    action_.push_back(action);
  }
};
struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};
enum OpAction
{
  WRITE_ACTION = 1,
  HIDE_SOURCE = 2,
  HIDE_DEST = 4,
  UNHIDE_SOURCE = 8,
  UNHIDE_DEST = 16,
  UNDELE_DEST = 32,
  DELETE_DEST = 64
};

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
int get_file_list(const string& log_file, SYNC_FILE_MAP& sync_file_map);

int sync_file(const string& source_tfs_client, const string& dest_tfs_client, FILE_SET& name_set, const string& modify_time);
int cmp_file_info(const string& source_tfs_client, const string& dest_tfs_client, string& file_name, const string& modify_time, ActionOp& action_op);
int do_action(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name, const ActionOp& action_op);
int do_action_ex(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name, int32_t action);
int copy_file(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name);
int get_file_info(const string& tfs_client, string& file_name, TfsFileStat& buf);
void change_stat(int32_t source_flag, int32_t dest_flag, ActionOp& action_op);
char* str_match(char* data, const char* prefix);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& source_ns_ip, const string& dest_ns_ip, const string& modify_time):
      source_tfs_client_(source_ns_ip), dest_tfs_client_(dest_ns_ip), modify_time_(modify_time)
  {
    TfsClient::Instance()->initialize(source_ns_ip.c_str());
  }
    virtual ~WorkThread()
    {

    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    virtual void run()
    {
      sync_file(source_tfs_client_, dest_tfs_client_, name_set_, modify_time_);
    }

    void push_back(FILE_SET name_set)
    {
      name_set_.insert(name_set.begin(), name_set.end());
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    string source_tfs_client_;
    string dest_tfs_client_;
    FILE_SET name_set_;
    string modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;
static WorkThreadPtr* gworks = NULL;
static int32_t thread_count = 1;
static const int32_t MAX_READ_DATA_SIZE = 81920;
static const int32_t MAX_READ_LEN = 256;
string source_ns_ip_ = "", dest_ns_ip_ = "";
FILE *g_sync_succ = NULL, *g_sync_fail = NULL;

struct LogFile g_log_fp[] =
{
  {&g_sync_succ, "sync_succ_file"},
  {&g_sync_fail, "sync_fail_file"},
  {NULL, NULL}
};

int init_log_file(char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s%s", dir_path, g_log_fp[i].file_);
    *g_log_fp[i].fp_ = fopen(file_path, "w");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d -f [-g] [-l] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f file name, assign a file to sync\n");
  fprintf(stderr, "       -g log file name, redirect log info to log file, optional\n");
  fprintf(stderr, "       -l log file level, set log file level, optional\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

static void interrupt_callback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal);
  switch( signal )
  {
    case SIGTERM:
    case SIGINT:
    default:
      if (gworks != NULL)
      {
        for (int32_t i = 0; i < thread_count; ++i)
        {
          if (gworks != 0)
          {
            gworks[i]->destroy();
          }
        }
      }
      break;
  }
}
int main(int argc, char* argv[])
{
  int32_t i;
  string source_ns_ip = "", dest_ns_ip = "";
  string file_name = "";
  string modify_time = "";
  string log_file = "sync_report.log";
  string level = "info";

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:m:t:g:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        source_ns_ip = optarg;
        break;
      case 'd':
        dest_ns_ip = optarg;
        break;
      case 'f':
        file_name = optarg;
        break;
      case 'm':
        modify_time = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'g':
        log_file = optarg;
        break;
      case 'l':
        level = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if ((source_ns_ip.empty()) || (source_ns_ip.compare(" ") == 0)
      || dest_ns_ip.empty() || (dest_ns_ip.compare(" ") == 0)
      || file_name.empty() || (file_name.compare(" ") == 0)
      || modify_time.length() < 8)
  {
    usage(argv[0]);
  }

  modify_time += "000000";

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    fprintf(stderr, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  char base_path[256];
  char log_path[256];
  snprintf(base_path, 256, "%s%s", dirname(argv[0]), "/log/");
  DirectoryOp::create_directory(base_path);
  init_log_file(base_path);

  snprintf(log_path, 256, "%s%s", base_path, log_file.c_str());
  if (strlen(log_path) != 0 && access(log_path, R_OK) == 0)
  {
    char old_log_file[256];
    sprintf(old_log_file, "%s.%s", log_path, Func::time_to_str(time(NULL), 1).c_str());
    rename(log_path, old_log_file);
  }
  else if (!DirectoryOp::create_full_path(log_path, true))
  {
    TBSYS_LOG(ERROR, "create file(%s)'s directory failed", log_path);
    return TFS_ERROR;
  }

  TBSYS_LOGGER.setFileName(log_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);
  TBSYS_LOGGER.setLogLevel(level.c_str());

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));
  SYNC_FILE_MAP sync_file_map;

  get_file_list(file_name, sync_file_map);
  if (sync_file_map.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(source_ns_ip, dest_ns_ip, modify_time);
    }
    int32_t index = 0;
    int64_t count = 0;
    SYNC_FILE_MAP_ITER iter = sync_file_map.begin();
    for (; iter != sync_file_map.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(iter->second);
      ++count;
    }
    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->start();
    }

    signal(SIGHUP, interrupt_callback);
    signal(SIGINT, interrupt_callback);
    signal(SIGTERM, interrupt_callback);
    signal(SIGUSR1, interrupt_callback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
    }

    tbsys::gDeleteA(gworks);
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }

  fprintf(stdout, "TOTAL COUNT: %"PRI64_PREFIX"d, ACTUAL_COUNT: %"PRI64_PREFIX"d, SUCCESS COUNT: %"PRI64_PREFIX"d, FAIL COUNT: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_, g_sync_stat_.success_count_, g_sync_stat_.fail_count_);
  fprintf(stdout, "LOG FILE: %s\n", log_path);

  return TFS_SUCCESS;
}
int get_file_list(const string& log_file, SYNC_FILE_MAP& sync_file_map)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen (log_file.c_str(), "r");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", log_file.c_str(), strerror(errno));
  }
  else
  {
    char info[MAX_READ_LEN];
    while(!feof(fp))
    {
      memset(info, 0, MAX_READ_LEN);
      fgets (info, MAX_READ_LEN, fp);
      if (strlen(info) > 0)
      {
        char* block = str_match(info, "blockid: ");
        uint32_t block_id = static_cast<uint32_t>(atoi(block));
        char* file = str_match(block + strlen(block) + 1, "fileid: ");
        uint64_t file_id = strtoull(file, NULL, 10);
        FSName fs;
        fs.set_block_id(block_id);
        fs.set_file_id(file_id);
        string file_name = string(fs.get_name());
        SYNC_FILE_MAP_ITER iter = sync_file_map.find(block_id);
        if (iter != sync_file_map.end())
        {
          iter->second.insert(file_name);
        }
        else
        {
          FILE_SET name_set;
          name_set.insert(file_name);
          sync_file_map.insert(make_pair(block_id, name_set));
        }

        TBSYS_LOG(INFO, "block_id: %u, file_id: %"PRI64_PREFIX"u, name: %s\n", block_id, file_id, fs.get_name());
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}
int do_action(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name, const ActionOp& action_op)
{
  int ret = TFS_SUCCESS;
  const ACTION_VEC& action_vec = action_op.action_;
  ACTION_VEC_ITER iter = action_vec.begin();
  int32_t index = 0;
  for (; iter != action_vec.end(); iter++)
  {
    int32_t action = (*iter);
    ret = do_action_ex(source_tfs_client, dest_tfs_client, file_name, action);
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(INFO, "tfs file(%s) do (%d)th action(%d) success", file_name.c_str(), index, action);
      index++;
    }
    else
    {
      TBSYS_LOG(ERROR, "tfs file(%s) do (%d)th action(%d) failed, ret: %d", file_name.c_str(), index, action, ret);
      break;
    }
  }
  if ((index > action_op.trigger_index_) && (index <= action_op.force_index_))
  {
    int tmp_ret = do_action_ex(source_tfs_client, dest_tfs_client, file_name, action_vec[action_op.force_index_]);
    TBSYS_LOG(ERROR, "former operation error occures, file(%s) must do force action(%d), ret: %d", file_name.c_str(), action_vec[action_op.force_index_], tmp_ret);
    ret = TFS_ERROR;
  }
  return ret;
}
int do_action_ex(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name, int32_t action)
{
  int64_t file_size = 0;
  int ret = TFS_SUCCESS;
  switch (action)
  {
    case WRITE_ACTION:
      ret = copy_file(source_tfs_client, dest_tfs_client, file_name);
      break;
    case HIDE_SOURCE:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, source_tfs_client.c_str(), CONCEAL);
      usleep(20000);
      break;
    case HIDE_DEST:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_tfs_client.c_str(), CONCEAL);
      usleep(20000);
      break;
    case UNHIDE_SOURCE:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, source_tfs_client.c_str(), REVEAL);
      usleep(20000);
      break;
    case UNHIDE_DEST:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_tfs_client.c_str(), REVEAL);
      usleep(20000);
      break;
    case UNDELE_DEST:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_tfs_client.c_str(), UNDELETE);
      usleep(20000);
      break;
    case DELETE_DEST:
      ret = TfsClient::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_tfs_client.c_str(), DELETE);
      usleep(20000);
      break;
    default:
      break;
  }
  return ret;
}

int sync_file(const string& source_tfs_client, const string& dest_tfs_client, FILE_SET& name_set, const string& modify_time)
{
  int ret = TFS_SUCCESS;
  FILE_SET_ITER iter = name_set.begin();
  for (; iter != name_set.end(); iter++)
  {
    string file_name = (*iter);
    ActionOp action_op;

    // compare file info
    ret = cmp_file_info(source_tfs_client, dest_tfs_client, file_name, modify_time, action_op);
    // do sync file
    if (ret != TFS_ERROR)
    {
      ret = do_action(source_tfs_client, dest_tfs_client, file_name, action_op);
    }
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(INFO, "sync file(%s) succeed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_sync_succ, "%s\n", file_name.c_str());
    }
    else
    {
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.fail_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
    }
    g_sync_stat_.actual_count_++;
  }
  return ret;
}
int copy_file(const string& source_tfs_client, const string& dest_tfs_client, const string& file_name)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_DATA_SIZE];
  int32_t rlen = 0;

  int source_fd = TfsClient::Instance()->open(file_name.c_str(), NULL, source_tfs_client.c_str(), T_READ);
  if (source_fd < 0)
  {
    TBSYS_LOG(ERROR, "open source tfsfile fail when copy file, filename: %s", file_name.c_str());
    ret = TFS_ERROR;
  }
  int dest_fd = TfsClient::Instance()->open(file_name.c_str(), NULL, dest_tfs_client.c_str(), T_WRITE | T_NEWBLK);
  if (dest_fd < 0)
  {
    TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, filename: %s", file_name.c_str());
    ret = TFS_ERROR;
  }
  if (TFS_SUCCESS == ret)
  {
    for (;;)
    {
      rlen = TfsClient::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);
      if (rlen < 0)
      {
        TBSYS_LOG(ERROR, "read tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
        ret = TFS_ERROR;
        break;
      }
      if (rlen == 0)
      {
        break;
      }

      if (TfsClient::Instance()->write(dest_fd, data, rlen) != rlen)
      {
        TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
        ret = TFS_ERROR;
        break;
      }

      if (rlen < MAX_READ_DATA_SIZE)
      {
        break;
      }
    }
  }
  if (source_fd > 0)
  {
    TfsClient::Instance()->close(source_fd);
  }
  if (dest_fd > 0)
  {
    if (TFS_SUCCESS != TfsClient::Instance()->close(dest_fd))
    {
      ret = TFS_ERROR;
      TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s", file_name.c_str());
    }
  }
  return ret;
}

int cmp_file_info(const string& source_tfs_client, const string& dest_tfs_client, string& file_name, const string& modify_time, ActionOp& action_op)
{
  UNUSED(modify_time);
  int ret = TFS_SUCCESS;
  int tmp_ret = TFS_SUCCESS;
  TfsFileStat source_buf, dest_buf;
  memset(&source_buf, 0, sizeof(source_buf));
  memset(&dest_buf, 0, sizeof(dest_buf));
  tmp_ret = get_file_info(source_tfs_client, file_name, source_buf);
  if (tmp_ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get source file info (%s) failed, ret: %d", file_name.c_str(), tmp_ret);
    ret = TFS_ERROR;
  }
  else
  {
    tmp_ret = get_file_info(dest_tfs_client, file_name, dest_buf);
    TBSYS_LOG(INFO, "file(%s): flag--(%d -> %d), crc--(%d -> %d), size--(%d -> %d), ret: %d", file_name.c_str(), source_buf.flag_, dest_buf.flag_, source_buf.crc_, dest_buf.crc_, source_buf.size_, dest_buf.size_, tmp_ret);
    // 1. source file exists, dest file is not exist or diff from source, rewrite file.
    if (((source_buf.flag_ & 1) == 0) && ((tmp_ret != TFS_SUCCESS) || ((dest_buf.size_ != source_buf.size_) || (dest_buf.crc_ != source_buf.crc_)))) // dest file exist
    {
      // if source file is normal, just rewrite it
      if (source_buf.flag_ == 0)
      {
        action_op.push_back(WRITE_ACTION);
      }
      // if source file has been hided, reveal it and then rewrite
      else if (source_buf.flag_ == 4)
      {
        action_op.push_back(UNHIDE_SOURCE);
        action_op.push_back(WRITE_ACTION);
        action_op.push_back(HIDE_SOURCE);
        action_op.push_back(HIDE_DEST);
        action_op.trigger_index_ = 0;
        action_op.force_index_ = 2;
      }
    }
    // 2. source file is the same to dest file, but in diff stat
    else if ((tmp_ret == TFS_SUCCESS) && (source_buf.flag_ != dest_buf.flag_) && (((source_buf.flag_ & 1) != 0) || ((dest_buf.crc_ == source_buf.crc_) && (dest_buf.size_ == source_buf.size_))))
    {
      TBSYS_LOG(WARN, "file in diff stat(%d -> %d) or source file(%s) has been deleted(%d)", dest_buf.crc_, source_buf.crc_, file_name.c_str(), source_buf.flag_);
      change_stat(source_buf.flag_, dest_buf.flag_, action_op);
    }
  }
  TBSYS_LOG(INFO, "cmp file (%s): ret: %d, source_flag: %d, dest_flag: %d, action_set: %d", file_name.c_str(), ret,
      ((ret == TFS_SUCCESS) ? source_buf.flag_:-1), ((tmp_ret == TFS_SUCCESS)? dest_buf.flag_:-1), action_op.action_.size());
  return ret;
}

int get_file_info(const string& tfs_client, string& file_name, TfsFileStat& buf)
{
  int ret = TFS_SUCCESS;

  int fd = TfsClient::Instance()->open(file_name.c_str(), NULL, tfs_client.c_str(), T_READ);
  if (fd < 0)
  {
    ret = TFS_ERROR;
    TBSYS_LOG(ERROR, "open file(%s) failed, ret: %d", file_name.c_str(), ret);
  }
  else
  {
    TfsClient::Instance()->fstat(fd, &buf);
    TfsClient::Instance()->close(fd);
  }
  return ret;
}
void change_stat(int32_t source_flag, int32_t dest_flag, ActionOp& action_op)
{
  ACTION_VEC action;
  if (source_flag == 0)
  {
    if (dest_flag & 1)
    {
      action_op.push_back(UNDELE_DEST);
    }
    if (dest_flag & 4)
    {
      action_op.push_back(UNHIDE_DEST);
    }
  }
  if ((source_flag & 4) != 0)
  {
    if (dest_flag == 0)
    {
      action_op.push_back(HIDE_DEST);
    }
    else if ((source_flag & 1) == 0)
    {
      if ((dest_flag & 1) != 0)
      {
        action_op.push_back(UNDELE_DEST);
      }
      if ((dest_flag & 4) == 0)
      {
        action_op.push_back(HIDE_DEST);
      }
    }
  }
  if (((source_flag & 1) != 0) && ((dest_flag & 1) == 0))
  {
    action_op.push_back(DELETE_DEST);
  }
}
char* str_match(char* data, const char* prefix)
{
  if (data == NULL || prefix == NULL)
  {
    return NULL;
  }
  char* pch = strstr(data, prefix);
  if (pch != NULL)
  {
    pch += strlen(prefix);
    char* tmp = strchr(pch , ',');
    if (tmp != NULL)
    {
      *tmp = '\0';
    }
  }

  return pch;
}
