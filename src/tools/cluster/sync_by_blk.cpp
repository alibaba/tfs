/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_by_blk.cpp 1542 2012-06-27 02:02:37Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "sync_file_base.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/block_info_message.h"
#include "message/server_status_message.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;

typedef vector<uint32_t> BLOCK_ID_VEC;
typedef vector<uint32_t>::iterator BLOCK_ID_VEC_ITER;
typedef vector<std::string> FILE_NAME_VEC;
typedef vector<std::string>::iterator FILE_NAME_VEC_ITER;

struct BlockFileInfo
{
  BlockFileInfo(const uint32_t block_id, const FileInfo& file_info)
    : block_id_(block_id)
  {
    memcpy(&file_info_, &file_info, sizeof(file_info));
  }
  ~BlockFileInfo()
  {
  }
  uint32_t block_id_;
  FileInfo file_info_;
};

typedef vector<BlockFileInfo> BLOCK_FILE_INFO_VEC;
typedef vector<BlockFileInfo>::iterator BLOCK_FILE_INFO_VEC_ITER;

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

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
string src_ns_addr = "", dest_ns_addr = "";
FILE *g_blk_done= NULL;
FILE *g_file_succ = NULL, *g_file_fail = NULL;

//int get_server_status();
//int get_diff_block();
int get_file_list(const string& ns_addr, const uint32_t block_id, BLOCK_FILE_INFO_VEC& v_block_file_info);
int get_file_list(const string& ns_addr, const uint32_t block_id, FILE_NAME_VEC& v_file_name);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint32_t block_id, FileInfo& file_info, const bool force, const int32_t modify_time);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr, const bool need_remove_file, const bool force, const int32_t modify_time):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), need_remove_file_(need_remove_file), force_(force), modify_time_(modify_time), destroy_(false)
  {
    TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, true);
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
      if (!destroy_)
      {
        BLOCK_ID_VEC_ITER iter = v_block_id_.begin();
        for (; iter != v_block_id_.end(); iter++)
        {
          if (!destroy_)
          {
            uint32_t block_id = (*iter);
            bool block_done = false;
            TBSYS_LOG(INFO, "sync block started. blockid: %u", block_id);

            BLOCK_FILE_INFO_VEC v_block_file_info;
            get_file_list(src_ns_addr_, block_id, v_block_file_info);
            TBSYS_LOG(INFO, "file size: %zd", v_block_file_info.size());
            BLOCK_FILE_INFO_VEC_ITER file_info_iter = v_block_file_info.begin();
            for (; file_info_iter != v_block_file_info.end(); file_info_iter++)
            {
              if (destroy_)
              {
                break;
              }
              sync_file(src_ns_addr_, dest_ns_addr_, block_id, (*file_info_iter).file_info_, force_, modify_time_);
            }
            block_done = (file_info_iter == v_block_file_info.end());

            if (need_remove_file_)
            {
              block_done = false;
              if (!destroy_)
              {
                FILE_NAME_VEC src_v_file_name, dest_v_file_name, diff_v_file_name;

                get_file_list(src_ns_addr_, block_id, src_v_file_name);
                TBSYS_LOG(DEBUG, "source file list size: %zd", src_v_file_name.size());

                get_file_list(dest_ns_addr_, block_id, dest_v_file_name);
                TBSYS_LOG(DEBUG, "dest file list size: %zd", dest_v_file_name.size());

                get_diff_file_list(src_v_file_name, dest_v_file_name, diff_v_file_name);
                TBSYS_LOG(DEBUG, "diff file list size: %zd", diff_v_file_name.size());

                unlink_file_list(diff_v_file_name);
                TBSYS_LOG(DEBUG, "unlink file list size: %zd", diff_v_file_name.size());

                block_done = true;
              }
            }

            if (block_done)
            {
              fprintf(g_blk_done, "%u\n", block_id);
              TBSYS_LOG(INFO, "sync block finished. blockid: %u", block_id);
            }
          }
        }
      }
    }

    void push_back(uint32_t block_id)
    {
      v_block_id_.push_back(block_id);
    }

    void get_diff_file_list(FILE_NAME_VEC& src_v_file_name, FILE_NAME_VEC& dest_v_file_name, FILE_NAME_VEC& out_v_file_name)
    {
      FILE_NAME_VEC_ITER dest_iter = dest_v_file_name.begin();
      for(; dest_iter != dest_v_file_name.end(); dest_iter++)
      {
        vector<std::string>::iterator src_iter = src_v_file_name.begin();
        for(; src_iter != src_v_file_name.end(); src_iter++)
        {
          if (*src_iter == *dest_iter)
          {
            break;
          }
        }
        if (src_iter == src_v_file_name.end())
        {
          TBSYS_LOG(INFO, "dest file need to be deleted. file_name: %s", (*dest_iter).c_str());
          out_v_file_name.push_back(*dest_iter);
        }
      }
    }

    int64_t unlink_file_list(vector<std::string>& out_v_file_name)
    {
      int64_t size = 0;
      vector<std::string>::iterator iter = out_v_file_name.begin();
      for(; iter != out_v_file_name.end(); iter++)
      {
        TfsClientImpl::Instance()->unlink(size, (*iter).c_str(), NULL, dest_ns_addr_.c_str());
      }
      return size;
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    bool need_remove_file_;
    bool force_;
    BLOCK_ID_VEC v_block_id_;
    int32_t modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static WorkThreadPtr* gworks = NULL;

struct LogFile g_log_fp[] =
{
  {&g_blk_done, "sync_done_blk"},
  {&g_file_succ, "sync_succ_file"},
  {&g_file_fail, "sync_fail_file"},
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
  fprintf(stderr, "Usage: %s -s -d -f -e -u [-g] [-l] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f block list file, assign a file to sync\n");
  fprintf(stderr, "       -m modify time, the file modified after it will be ignored\n");
  fprintf(stderr, "       -e force flag, need strong consistency\n");
  fprintf(stderr, "       -u flag, need delete redundent file in dest cluster\n");
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
  string src_ns_addr = "", dest_ns_addr = "";
  string block_list_file = "";
  bool force = false;
  bool need_remove_file = false;
  string modify_time = "";
  string log_file = "sync_report.log";
  string level = "debug";

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:eum:t:g:l:h")) != EOF)
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
        block_list_file = optarg;
        break;
      case 'e':
        force = true;
        break;
      case 'u':
        need_remove_file = true;
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

  if ((src_ns_addr.empty()) || (src_ns_addr.compare(" ") == 0)
      || dest_ns_addr.empty() || (dest_ns_addr.compare(" ") == 0)
      || block_list_file.empty() || modify_time.empty())
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
  snprintf(base_path, 256, "%s%s", dirname(argv[0]), "/logs/");
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

  TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, 1000, false);

  BLOCK_ID_VEC v_block_id;
  if (! block_list_file.empty())
  {
    FILE *fp = fopen(block_list_file.c_str(), "rb");
    if (fp)
    {
      char tbuf[256];
      while(fgets(tbuf, 256, fp))
      {
        uint32_t block_id = strtoul(tbuf, NULL, 10);
        v_block_id.push_back(block_id);
      }
      fclose(fp);
    }
  }
  TBSYS_LOG(INFO, "blockid list size: %zd", v_block_id.size());

  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  if (v_block_id.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    int32_t time = tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str()));
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr, need_remove_file, force, time);
    }
    int32_t index = 0;
    int64_t count = 0;
    BLOCK_ID_VEC_ITER iter = v_block_id.begin();
    for (; iter != v_block_id.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(*iter);
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
int get_file_list(const string& ns_addr, const uint32_t block_id, FILE_NAME_VEC& v_file_name)
{
  BLOCK_FILE_INFO_VEC v_block_file_info;
  int ret = get_file_list(ns_addr, block_id, v_block_file_info);
  if (ret == TFS_SUCCESS)
  {
    BLOCK_FILE_INFO_VEC_ITER iter = v_block_file_info.begin();
    for (; iter != v_block_file_info.end(); iter++)
    {
      FSName fsname(iter->block_id_, iter->file_info_.id_, TfsClientImpl::Instance()->get_cluster_id(ns_addr.c_str()));
      v_file_name.push_back(string(fsname.get_name()));
    }
  }
  return ret;
}
int get_file_list(const string& ns_addr, const uint32_t block_id, BLOCK_FILE_INFO_VEC& v_block_file_info)
{
  int ret = TFS_ERROR;
  VUINT64 ds_list;
  GetBlockInfoMessage gbi_message;
  gbi_message.set_block_id(block_id);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  ret = send_msg_to_server(Func::get_host_ip(ns_addr.c_str()), client, &gbi_message, rsp);

  if (rsp != NULL)
  {
    if (rsp->getPCode() == SET_BLOCK_INFO_MESSAGE)
    {
      ds_list = dynamic_cast<SetBlockInfoMessage*>(rsp)->get_block_ds();
      if (ds_list.size() > 0)
      {
        uint64_t ds_id = ds_list[0];
        TBSYS_LOG(INFO, "ds_ip: %s", tbsys::CNetUtil::addrToString(ds_id).c_str());
        GetServerStatusMessage req_gss_msg;
        req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO);
        req_gss_msg.set_return_row(block_id);

        int ret_status = TFS_ERROR;
        tbnet::Packet* ret_msg = NULL;
        NewClient* new_client = NewClientManager::get_instance().create_client();
        ret_status = send_msg_to_server(ds_id, new_client, &req_gss_msg, ret_msg);

        //if the information of file can be accessed.
        if ((ret_status == TFS_SUCCESS))
        {
          TBSYS_LOG(INFO, "ret_msg->pCODE: %d", ret_msg->getPCode());
          if (BLOCK_FILE_INFO_MESSAGE == ret_msg->getPCode())
          {
            FILE_INFO_LIST* file_info_list = (dynamic_cast<BlockFileInfoMessage*> (ret_msg))->get_fileinfo_list();
            int32_t i = 0;
            int32_t list_size = file_info_list->size();
            for (i = 0; i < list_size; i++)
            {
              FileInfo& file_info = file_info_list->at(i);
              FSName fsname(block_id, file_info.id_, TfsClientImpl::Instance()->get_cluster_id(ns_addr.c_str()));
              TBSYS_LOG(INFO, "block_id: %u, file_id: %"PRI64_PREFIX"u, name: %s\n", block_id, file_info.id_, fsname.get_name());
              v_block_file_info.push_back(BlockFileInfo(block_id, file_info));
            }
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            printf("%s", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
          }
        }
        else
        {
          fprintf(stderr, "Get File list in Block failure\n");
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }
    }
    else if (rsp->getPCode() == STATUS_MESSAGE)
    {
      ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
      fprintf(stderr, "get block info from %s fail, error: %s\n", ns_addr.c_str(), dynamic_cast<StatusMessage*>(rsp)->get_error());
    }
  }
  else
  {
    fprintf(stderr, "get NULL response message, ret: %d\n", ret);
  }

  NewClientManager::get_instance().destroy_client(client);

  return ret;
}
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint32_t block_id, FileInfo& file_info,
    const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;

  SyncFileBase sync_op;
  sync_op.initialize(src_ns_addr, dest_ns_addr);
  SyncAction sync_action;

  // compare file info
  ret = sync_op.cmp_file_info(block_id, file_info, sync_action, force, modify_time);

  FSName fsname(block_id, file_info.id_, TfsClientImpl::Instance()->get_cluster_id(src_ns_addr.c_str()));
  string file_name = string(fsname.get_name());
  // do sync file
  if (ret == TFS_SUCCESS)
  {
    ret = sync_op.do_action(file_name, sync_action);
  }
  if (ret == TFS_SUCCESS)
  {
    TBSYS_LOG(INFO, "sync file(%s) succeed.", file_name.c_str());
    {
      tbutil::Mutex::Lock lock(g_mutex_);
      g_sync_stat_.success_count_++;
    }
    fprintf(g_file_succ, "%s\n", file_name.c_str());
  }
  else
  {
    TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
    {
      tbutil::Mutex::Lock lock(g_mutex_);
      g_sync_stat_.fail_count_++;
    }
    fprintf(g_file_fail, "%s\n", file_name.c_str());
  }
  g_sync_stat_.actual_count_++;

  return ret;
}
