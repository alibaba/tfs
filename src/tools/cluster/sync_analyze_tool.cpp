/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_analyze_tool.cpp 1772 2012-12-11 13:55:56Z chuyu $
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
#include "common/internal.h"
#include "common/func.h"
#include "common/directory_op.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/file_info_message.h"
#include "message/server_status_message.h"
#include "new_client/fsname.h"
#include "new_client/tfs_client_impl.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::tools;

typedef vector<string> VEC_FILE_NAME;
typedef vector<string>::iterator VEC_FILE_NAME_ITER;

enum StatFlag
{
  NORMAL_FLAG = 0,
  DELETE_FLAG = 1,
  HIDE_FLAG = 4,
  DELETE_HIDE_FLAG = 5
};

enum SyncResultType
{
  DEST_UPDATE_FAIL = 0,
  DEAL_FAIL = 1,
  DEAL_SUCC = 2,
  SYNC_SUCC = 3
};
struct SyncStat
{
  int64_t total_count_;
  int64_t src_stat_fail_count_;
  int64_t dest_update_fail_count_;
  int64_t deal_fail_count_;
  int64_t deal_succ_count_;
  int64_t sync_succ_count_;
  SyncStat()
    :total_count_(0), src_stat_fail_count_(0), dest_update_fail_count_(0), deal_fail_count_(0), deal_succ_count_(0), sync_succ_count_(0)
  {
  }
  ~SyncStat()
  {
  }
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};

FILE *g_analyze_report = NULL;
FILE *g_src_stat_fail = NULL, *g_dest_update_fail = NULL, *g_deal_fail = NULL, *g_deal_succ = NULL, *g_sync_succ = NULL;
struct LogFile g_log_fp[] =
{
  {&g_analyze_report, "analyze_report"},
  {&g_src_stat_fail, "src_stat_fail_file"},
  {&g_dest_update_fail, "dest_update_fail_file"},
  {&g_deal_succ, "deal_succ_file"},
  {&g_deal_fail, "deal_fail_file"},
  {&g_sync_succ, "sync_succ_file"},
  {NULL, NULL}
};
static int32_t thread_count = 2;
static const int32_t MAX_REPL_COUNT = 2;
static const int32_t MAX_READ_LEN = 256;
static const int32_t NONE_FLAG = -1;
static string analyze_log("analyze_info.log");
static string report_log("analyze_report");
tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
static TfsClientImpl* g_tfs_client = NULL;

static void usage(const char* name);
int init_log_file(const char* dir_path);
int get_file_list(const string& log_file, VEC_FILE_NAME& name_set);
int cmp_file(const string& src_ns_addr, const string& dest_ns_addr, const string& tfs_name);
static void interrupt_callback(int signal);

int rename_file(const char* file_path);
SyncResultType check_status(const TfsFileStat& src_stat, const vector<FileInfo>& fileinfos);
bool is_diff_copys(const vector<FileInfo>& fileinfos);
string dump_flag(const vector<FileInfo>& fileinfos);
int read_file_info(const uint64_t ds_id, const uint32_t block_id, const uint64_t file_id, const int32_t mode, FileInfo& fileinfo);
float get_percent(int64_t dividend, int64_t divisor);
void trim(const char* input, char* output);
static uint64_t string_to_addr(const std::string& ns_ip_port);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), destroy_(false)
    {
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
      VEC_FILE_NAME_ITER iter = name_set_.begin();
      for (; iter != name_set_.end(); iter++)
      {
        if (destroy_)
        {
          break;
        }
        else
        {
          cmp_file(src_ns_addr_, dest_ns_addr_, (*iter));
        }
      }
    }

    void push_back(string& file_name)
    {
      name_set_.push_back(file_name);
    }

  private:
    WorkThread(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    VEC_FILE_NAME name_set_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;
static WorkThreadPtr* gworks = NULL;

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s src_addr -d dest_addr -f file_list [-t thread_count] [-p log_path] [-l level] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f sync fail file list\n");
  fprintf(stderr, "       -p the dir path of log file and result logs, both absolute and relative path are ok, if path not exists, it will be created. default is under logs/, optional\n");
  fprintf(stderr, "       -l log file level, set log file level to (debug|info|warn|error), default is info, optional\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int init_log_file(const char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s/%s", dir_path, g_log_fp[i].file_);
    rename_file(file_path);
    *g_log_fp[i].fp_ = fopen(file_path, "wb");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  char log_file_path[256];
  snprintf(log_file_path, 256, "%s/%s", dir_path, analyze_log.c_str());
  rename_file(log_file_path);
  TBSYS_LOGGER.setFileName(log_file_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);

  return TFS_SUCCESS;
}

int get_file_list(const string& log_file, VEC_FILE_NAME& name_set)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen (log_file.c_str(), "r");
  if (fp == NULL)
  {
    TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", log_file.c_str(), strerror(errno));
  }
  else
  {
    char name[MAX_READ_LEN];
    char buf[MAX_READ_LEN];
    while(!feof(fp))
    {
      memset(buf, 0, MAX_READ_LEN);
      fgets (buf, MAX_READ_LEN, fp);
      trim(buf, name);
      if (strlen(name) > 0)
      {
        name_set.push_back(string(name));
        //TBSYS_LOG(DEBUG, "file name: %s\n", name);
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int cmp_file(const string& src_ns_addr, const string& dest_ns_addr, const string& tfs_name)
{
  TfsFileStat src_stat;
  memset(&src_stat, 0, sizeof(src_stat));
  int ret = TFS_ERROR;
  ret = g_tfs_client->stat_file(&src_stat, tfs_name.c_str(), NULL, FORCE_STAT, src_ns_addr.c_str());
  if (ret != TFS_SUCCESS)
  {
    {
      tbutil::Mutex::Lock lock(g_mutex_);
      g_sync_stat_.src_stat_fail_count_++;
    }
    fprintf(g_src_stat_fail, "%s\n", tfs_name.c_str());
  }
  else
  {
    FSName name(tfs_name.c_str());
    uint32_t block_id = name.get_block_id();
    uint64_t file_id = name.get_file_id();

    VUINT64 ds_list;
    ret = ToolUtil::get_block_ds_list(string_to_addr(dest_ns_addr), block_id, ds_list);
    if (ret != TFS_SUCCESS)
    {
      TBSYS_LOG(WARN, "get block info fail, block_id: %u, ret: %d\n", block_id, ret);
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.deal_succ_count_++;
      }
      fprintf(g_deal_succ, "%s, src: %d, dest: %d\n", tfs_name.c_str(), src_stat.flag_, NONE_FLAG);
    }
    else
    {
      int32_t ds_size = ds_list.size();
      vector<FileInfo> fileinfos;
      for (int32_t i = 0; i < ds_size; i++)
      {
        FileInfo fileinfo;
        ret = read_file_info(ds_list.at(i), block_id, file_id, FORCE_STAT, fileinfo);
        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(WARN, "read dest file info failed, filename: %s, block_id: %u, file_id: %"PRI64_PREFIX"u", tfs_name.c_str(), block_id, file_id);
          fileinfo.flag_ = NONE_FLAG;
        }
        fileinfos.push_back(fileinfo);
      }
      // classify sync error type
      SyncResultType ret_type = check_status(src_stat, fileinfos);
      switch(ret_type)
      {
        case DEST_UPDATE_FAIL:
          {
            tbutil::Mutex::Lock lock(g_mutex_);
            g_sync_stat_.dest_update_fail_count_++;
          }
          fprintf(g_dest_update_fail, "%s, crc: %d -> %d, size: %"PRI64_PREFIX"d -> %d\n",
              tfs_name.c_str(), src_stat.crc_, fileinfos.at(0).crc_, src_stat.size_, fileinfos.at(0).size_);
          break;
        case DEAL_FAIL:
          {
            tbutil::Mutex::Lock lock(g_mutex_);
            g_sync_stat_.deal_fail_count_++;
          }
          fprintf(g_deal_fail, "%s, src: %d, dest:%s\n", tfs_name.c_str(), src_stat.flag_, dump_flag(fileinfos).c_str());
          break;
        case DEAL_SUCC:
          {
            tbutil::Mutex::Lock lock(g_mutex_);
            g_sync_stat_.deal_succ_count_++;
          }
          fprintf(g_deal_succ, "%s, src: %d, dest:%s\n", tfs_name.c_str(), src_stat.flag_, dump_flag(fileinfos).c_str());
          break;
        case SYNC_SUCC:
          {
            tbutil::Mutex::Lock lock(g_mutex_);
            g_sync_stat_.sync_succ_count_++;
          }
          fprintf(g_sync_succ, "%s\n", tfs_name.c_str());
          break;
        default:
          break;
      }
    }
  }
  return TFS_SUCCESS;
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
        for (int32_t i = 0; g_log_fp[i].file_; i++)
        {
          if (!*g_log_fp[i].fp_)
          {
            fflush(*g_log_fp[i].fp_);
          }
        }
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

int rename_file(const char* file_path)
{
  int ret = TFS_SUCCESS;
  if (access(file_path, F_OK) == 0)
  {
    char old_file_path[256];
    snprintf(old_file_path, 256, "%s.%s", file_path, Func::time_to_str(time(NULL), 1).c_str());
    ret = rename(file_path, old_file_path);
  }
  return ret;
}

SyncResultType check_status(const TfsFileStat& src_stat, const vector<FileInfo>& fileinfos)
{
  SyncResultType ret;
  (is_diff_copys(fileinfos)) ? (ret = DEAL_SUCC) : (ret = DEAL_FAIL) ;

  int32_t size = fileinfos.size();
  for (int32_t i = 0; i < size; i++)
  {
    if (src_stat.flag_ == fileinfos.at(i).flag_)
    {
      if  ((src_stat.crc_ == fileinfos.at(i).crc_) && (src_stat.size_ == fileinfos.at(i).size_))
      {
        ret = SYNC_SUCC;
      }
      else
      {
        if (src_stat.flag_ == NORMAL_FLAG)
        {
          ret = DEST_UPDATE_FAIL;
        }
      }
    }
    else
    {
      break;
    }
  }

  return ret;
}

bool is_diff_copys(const vector<FileInfo>& fileinfos)
{
  bool is_diff = false;
  for (int32_t i = 0; i < MAX_REPL_COUNT - 1; i++)
  {
    for (int32_t j = i + 1; j < MAX_REPL_COUNT; j++)
    {
       if (fileinfos.at(i).flag_ != fileinfos.at(j).flag_)
       {
         is_diff = true;
       }
    }
  }
  return is_diff;
}

string dump_flag(const vector<FileInfo>& fileinfos)
{
  string ret_str;
  int32_t size = fileinfos.size();
  for (int32_t i = 0; i < size; i++)
  {
    char s[8];
    sprintf(s, " %d", fileinfos.at(i).flag_);
    ret_str += s;
  }
  return ret_str;
}
int read_file_info(const uint64_t ds_id, const uint32_t block_id, const uint64_t file_id, const int32_t mode, FileInfo& fileinfo)
{
  FileInfoMessage req_fi_msg;
  req_fi_msg.set_block_id(block_id);
  req_fi_msg.set_file_id(file_id);
  req_fi_msg.set_mode(mode);

  int ret_status = TFS_ERROR;
  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* ret_msg = NULL;
  ret_status = send_msg_to_server(ds_id, client, &req_fi_msg, ret_msg);
  if ((ret_status == TFS_SUCCESS))
  {
    if (RESP_FILE_INFO_MESSAGE == ret_msg->getPCode())
    {
      RespFileInfoMessage* resp_fi_msg = dynamic_cast<RespFileInfoMessage*> (ret_msg);
      if (resp_fi_msg->get_file_info() != NULL)
      {
        memcpy(&fileinfo, resp_fi_msg->get_file_info(), FILEINFO_SIZE);
        if (fileinfo.id_ == file_id)
        {
          ret_status = TFS_SUCCESS;
        }
      }
    }
    else if (STATUS_MESSAGE == ret_msg->getPCode())
    {
      TBSYS_LOG(ERROR, "read file info error:%s\n", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
      ret_status = TFS_ERROR;
    }
    else
    {
      TBSYS_LOG(ERROR, "message type is error.\n");
      ret_status = TFS_ERROR;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "read file info fail\n");
    ret_status = TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret_status;
}

float get_percent(int64_t dividend, int64_t divisor)
{
  return divisor > 0 ? dividend * 100 / divisor : -1;
}
void trim(const char* input, char* output)
{
  if (output == NULL)
  {
    TBSYS_LOG(WARN, "output str is null");
  }
  else
  {
    int32_t size = strlen(input);
    int32_t j = 0;
    for (int32_t i = 0; i < size; i++)
    {
      if (!isspace(input[i]))
      {
        output[j] = input[i];
        j++;
      }
    }
    output[j] = '\0';
  }
}

static uint64_t string_to_addr(const std::string& ns_ip_port)
{
  string::size_type pos = ns_ip_port.find_first_of(":");
  if (pos == string::npos)
  {
    return TFS_ERROR;
  }
  string tmp = ns_ip_port.substr(0, pos);
  return Func::str_to_addr(tmp.c_str(), atoi(ns_ip_port.substr(pos + 1).c_str()));
}

int main(int argc, char* argv[])
{
  int32_t i;
  string src_ns_addr, dest_ns_addr;
  string file_list;
  string log_path("logs/");
  string level("info");

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:t:p:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        src_ns_addr = optarg;
        break;
      case 'd':
        dest_ns_addr = optarg;
        break;
      case 'f':
        file_list = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'p':
        log_path = optarg;
        break;
      case 'l':
        level = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (src_ns_addr.empty()
      || dest_ns_addr.empty()
      || file_list.empty())
  {
    usage(argv[0]);
  }

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    TBSYS_LOG(ERROR, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  if ((access(log_path.c_str(), F_OK) == -1) && (!DirectoryOp::create_full_path(log_path.c_str())))
  {
    TBSYS_LOG(ERROR, "create log file path failed. log_path: %s", log_path.c_str());
    return TFS_ERROR;
  }

  init_log_file(log_path.c_str());
  TBSYS_LOGGER.setLogLevel(level.c_str());

  g_tfs_client = TfsClientImpl::Instance();
  g_tfs_client->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, 1000, false);

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  VEC_FILE_NAME name_set;
  if (!file_list.empty())
  {
    get_file_list(file_list, name_set);
  }
  if (name_set.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr);
    }
    int32_t index = 0;
    VEC_FILE_NAME_ITER iter = name_set.begin();
    for (; iter != name_set.end(); iter++)
    {
      index = g_sync_stat_.total_count_ % thread_count;
      gworks[index]->push_back(*iter);
      g_sync_stat_.total_count_++;
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

  fprintf(g_analyze_report, "total_count: %"PRI64_PREFIX"d\n"
      "src_stat_fail_count: %"PRI64_PREFIX"d, percent: %.2f\n"
      "dest_update_fail_count: %"PRI64_PREFIX"d, percent: %.2f\n"
      "deal_fail_count: %"PRI64_PREFIX"d, percent: %.2f\n"
      "deal_succ_count: %"PRI64_PREFIX"d, percent: %.2f\n"
      "sync_succ_count: %"PRI64_PREFIX"d, percent: %.2f\n",
      g_sync_stat_.total_count_,
      g_sync_stat_.src_stat_fail_count_, get_percent(g_sync_stat_.src_stat_fail_count_, g_sync_stat_.total_count_),
      g_sync_stat_.dest_update_fail_count_, get_percent(g_sync_stat_.dest_update_fail_count_, g_sync_stat_.total_count_),
      g_sync_stat_.deal_fail_count_, get_percent(g_sync_stat_.deal_fail_count_, g_sync_stat_.total_count_),
      g_sync_stat_.deal_succ_count_, get_percent(g_sync_stat_.deal_succ_count_, g_sync_stat_.total_count_),
      g_sync_stat_.sync_succ_count_, get_percent(g_sync_stat_.sync_succ_count_, g_sync_stat_.total_count_));

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    if (*g_log_fp[i].fp_ != NULL)
    {
      fclose(*g_log_fp[i].fp_);
      *g_log_fp[i].fp_ = NULL;
    }
  }

  return TFS_SUCCESS;
}
