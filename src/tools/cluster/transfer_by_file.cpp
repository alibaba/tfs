/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: transfer_by_file.cpp 1126 2012-04-09 03:35:00Z xueya.yy $
 *
 * Authors:
 *   xueya.yy <xueya.yy@taobao.com>
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
#include "new_client/tfs_client_impl.h"
#include "dataserver/dataserver_define.h"
#include "common/internal.h"

using namespace std;
using namespace tfs::dataserver;
using namespace tfs::common;
using namespace tfs::client;

static const int32_t MAX_READ_DATA_SIZE = 81920;

typedef vector<string> VEC_FILE_NAME;
typedef vector<string>::iterator VEC_FILE_NAME_ITER;
struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
  int64_t del_count_;
};

struct LogFile
{
  FILE** fp_;
  const char* file_;
};

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;

bool b_special = false;
bool b_suffix = false;

string g_rfile_list("sync_result.txt");
string optional_suffix;

int get_file_list(const string& log_file, VEC_FILE_NAME& name_set);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, VEC_FILE_NAME& name_set);
void trim(const char* input, char* output);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), destroy_(false)
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
      if (! destroy_)
      {
        sync_file(src_ns_addr_, dest_ns_addr_, name_set_);
      }
    }

    void push_back(string& file_name)
    {
      name_set_.push_back(file_name);
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    VEC_FILE_NAME name_set_;
    bool destroy_;
};

typedef tbutil::Handle<WorkThread> WorkThreadPtr;
static WorkThreadPtr* gworks = NULL;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
FILE *g_sync_succ = NULL, *g_sync_fail = NULL, *g_sync_del = NULL;
FILE* g_fp = NULL;

struct LogFile g_log_fp[] =
{
  {&g_sync_succ, "sync_succ_file"},
  {&g_sync_fail, "sync_fail_file"},
  {&g_sync_del, "sync_del_file"},
  {NULL, NULL}
};

int init_log_file(const char* dir_path)
{
  for (int i = 0; NULL != g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s%s", dir_path, g_log_fp[i].file_);
    *g_log_fp[i].fp_ = fopen(file_path, "w");
    if (NULL == *g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -d -f -[r] [-t] [-g] [-l] [-x] [-y] [-z] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f source file list, assign a path to sync\n");
  fprintf(stderr, "       -r dest file list, assign a path to return result, optional, default is sync_result.txt\n");
  fprintf(stderr, "       -t work thread num, assign multi-thread work, optional\n");
  fprintf(stderr, "       -g log file name, redirect log info to log file, optional\n");
  fprintf(stderr, "       -l log file level, set log file level, optional\n");
  fprintf(stderr, "       -x whether need special tfs name, default is false, optional\n");
  fprintf(stderr, "       -y whether need suffxi , default is false, optional\n");
  fprintf(stderr, "       -z suffix, assign optional suffix\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

static void interrupt_callback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal);
  if (gworks != NULL)
  {
    for (int32_t i = 0; i < thread_count; i++)
    {
      if (gworks != 0)
      {
        gworks[i]->destroy();
      }
    }
  }
}

int main(int argc, char* argv[])
{
  int32_t i = 0;
  string src_ns_addr, dest_ns_addr;
  string file_list;
  string log_file("sync_report.log");
  string level("info");

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:r:t:g:l:z:xyh")) != EOF)
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
      case 'r':
        g_rfile_list = optarg;
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
      case 'x':
        b_special = true;
        break;
      case 'y':
        b_suffix = true;
        break;
      case 'z':
        optional_suffix = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (src_ns_addr.empty() || dest_ns_addr.empty() || file_list.empty())
  {
    usage(argv[0]);
  }


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

  VEC_FILE_NAME name_set;
  if (!file_list.empty())
  {
    get_file_list(file_list, name_set);
  }

  if (!g_rfile_list.empty())
  {
    g_fp = fopen (g_rfile_list.c_str(), "w");

    if (g_fp == NULL)
    {
      TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", g_rfile_list.c_str(), strerror(errno));
      return TFS_ERROR;
    }
  }

  if (name_set.size() > 0)
  {
    g_sync_stat_.total_count_ = name_set.size();
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr);
    }
    int32_t index = 0;
    int64_t count = 0;
    VEC_FILE_NAME_ITER iter = name_set.begin();
    for (; iter != name_set.end(); iter++)
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
      //delete gworks[i];
    }

    tbsys::gDeleteA(gworks);
  }

  if (g_fp != NULL) fclose(g_fp);

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }

  fprintf(stdout, "TOTAL COUNT: %"PRI64_PREFIX"d, ACTUAL_COUNT: %"PRI64_PREFIX"d, SUCCESS COUNT: %"PRI64_PREFIX"d, FAIL COUNT: %"PRI64_PREFIX"d, DEL COUNT: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_, g_sync_stat_.success_count_, g_sync_stat_.fail_count_, g_sync_stat_.del_count_);
  fprintf(stdout, "LOG FILE: %s\n", log_path);

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
        TBSYS_LOG(INFO, "name: %s\n", name);
      }
    }
    fclose (fp);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int sync_file(const string& src_ns_addr, const string& dest_ns_addr, VEC_FILE_NAME& name_set)
{
  int ret = TFS_SUCCESS;
  int flag = T_DEFAULT;
  int status = 0;
  int fd  = -1;
  int write_len = 0;
  int32_t rlen = 0;
  char ret_tfs_name[256];
  char tmp[256];
  char* temp = "tmp";
  string suffix;

  snprintf(tmp, 256, "%s%lu", temp, pthread_self());

  //printf("current tmp file is %s\n", tmp);

  VEC_FILE_NAME_ITER iter = name_set.begin();

  for (; iter != name_set.end(); iter++)
  {
    flag = T_READ | T_FORCE;
    ret = TFS_SUCCESS;
    TfsFileStat fileInfo;
    uint32_t source_crc = 0;
    uint32_t dest_crc = 0;
    int64_t source_size = 0;
    int64_t dest_size = 0;
    char data[MAX_READ_DATA_SIZE];
    char name_map[256];
    string real_suffix;
    string file_name = (*iter);
    int32_t src_name_len = file_name.size();

    // trunc the suffix
    if (src_name_len > FILE_NAME_LEN)
    {
      suffix = file_name.substr(FILE_NAME_LEN, src_name_len - FILE_NAME_LEN);
      printf("suffix is %s\n", suffix.c_str());
    }

    // form real_suffix
    if (suffix != "")
    {
      real_suffix = suffix;
    }
    else if (optional_suffix != "")
    {
      real_suffix = optional_suffix;
    }

    //open source file
    if (file_name[0] == 'L')
    {
      flag |= T_LARGE;
    }


    // first stat to check status (hide, delete, normal, not existed)
    if ((ret = TfsClientImpl::Instance()->stat_file(&fileInfo, file_name.c_str(), NULL, FORCE_STAT, src_ns_addr.c_str())) == TFS_SUCCESS)
    {
      source_size = fileInfo.size_;
      source_crc = fileInfo.crc_;
      status = fileInfo.flag_;
      // contain delete status
      if (status & FI_DELETED)
      {
        TBSYS_LOG(WARN, "this is deleted file, filename: %s", file_name.c_str());
        TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
        {
          tbutil::Mutex::Lock lock(g_mutex_);
          g_sync_stat_.del_count_++;
          g_sync_stat_.actual_count_++;
        }
        fprintf(g_sync_del, "%s\n", file_name.c_str());
        continue;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "file is not existed, filename: %s", file_name.c_str());
      ret = TFS_ERROR;
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.fail_count_++;
        g_sync_stat_.actual_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
      continue;
    }


    // fetch file to tmp file

    int source_fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, src_ns_addr.c_str(), flag);
    int read_size = 0;

    // open fail
    if (source_fd < 0)
    {
      TBSYS_LOG(ERROR, "open source tfsfile fail when open file, filename: %s, fd: %d", file_name.c_str(), source_fd);
      ret = TFS_ERROR;
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.fail_count_++;
        g_sync_stat_.actual_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
      continue;
    }

    // read
    if (TFS_SUCCESS == ret)
    {
      // open different file
      fd = open(tmp, O_WRONLY|O_CREAT|O_TRUNC, S_IRWXU|S_IRWXG|S_IRWXO);
      if (fd < 0)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "open temp file error, fd: %d", fd);
      }

      if (TFS_SUCCESS == ret)
      {
        for (;;)
        {
          rlen = TfsClientImpl::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);

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

          read_size += rlen;

          write_len = write(fd, data, rlen);

          if (write_len < 0 || write_len != rlen)
          {
            TBSYS_LOG(ERROR, "write to temp file error, rlen: %d, write len: %d", rlen, write_len);
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
        TfsClientImpl::Instance()->close(source_fd);
      }

      if (fd > 0)
      {
        close(fd);
      }

      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "write to tmp file successfully");
      }
    }

    // read to tmp file fail
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.actual_count_++;
        g_sync_stat_.fail_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
      continue;
    }

    // write to dest_ns_addr

    flag = (file_name[0] == 'L' ? T_LARGE : T_DEFAULT);

    //save_file add b_specail param

    if (file_name[0] == 'L')
    {
      if (real_suffix == "")
      {
        ret = TfsClientImpl::Instance()->save_file(ret_tfs_name, 256, tmp, flag, NULL, dest_ns_addr.c_str(), false);
      }
      else
      {
        ret = TfsClientImpl::Instance()->save_file(ret_tfs_name, 256, tmp, flag, real_suffix.c_str(), dest_ns_addr.c_str(), false);
      }
    }
    else
    {
      if (real_suffix == "")
      {
        ret = TfsClientImpl::Instance()->save_file(ret_tfs_name, 256, tmp, flag, NULL, dest_ns_addr.c_str(), b_special);
      }
      else
      {
        ret = TfsClientImpl::Instance()->save_file(ret_tfs_name, 256, tmp, flag, real_suffix.c_str(), dest_ns_addr.c_str(), b_special);
      }
    }

    //printf("return size: %d\n", ret);

    if (ret < 0)
    {
      TBSYS_LOG(ERROR, "write to dest_ns_addr fail, filename: %s", file_name.c_str());
      TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.actual_count_++;
        g_sync_stat_.fail_count_++;
      }
      fprintf(g_sync_fail, "%s\n", file_name.c_str());
      ret = TFS_ERROR;
      continue;
    }

    //printf("ret_tfs_name: %s\n", ret_tfs_name);

    // compare crc
    if ((ret = TfsClientImpl::Instance()->stat_file(&fileInfo, ret_tfs_name, NULL, FORCE_STAT, dest_ns_addr.c_str())) == TFS_SUCCESS)
    {
      dest_crc = fileInfo.crc_;
      dest_size = fileInfo.size_;
    }

    if (TFS_SUCCESS == ret)
    {
      // contain hide status
      if (status & FI_CONCEAL)
      {
        if (file_name[0] == 'L')
        {
          source_size = read_size;
        }

        int64_t file_size = 0;
        ret = TfsClientImpl::Instance()->unlink(file_size, ret_tfs_name, NULL, dest_ns_addr.c_str(), CONCEAL);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "hide file: %s fail", ret_tfs_name);
          TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
          {
            tbutil::Mutex::Lock lock(g_mutex_);
            g_sync_stat_.fail_count_++;
            g_sync_stat_.actual_count_++;
          }
          fprintf(g_sync_fail, "%s\n", file_name.c_str());
          continue;
        }
      }
    }



    if ( file_name[0] == 'T' && ret_tfs_name[0] == 'T' && source_size == dest_size && source_crc == dest_crc && TFS_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "sync file(%s) succeed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_sync_succ, "%s\n", file_name.c_str());

      if (b_special)
      {
        snprintf(name_map, 256, "%s %s\n", file_name.c_str(), ret_tfs_name);
      }
      else
      {
        b_suffix ? snprintf(name_map, 256, "%s %s%s\n", file_name.c_str(), ret_tfs_name, real_suffix.c_str())
                   : snprintf(name_map, 256, "%s %s\n", file_name.c_str(), ret_tfs_name);

      }
      fputs(name_map, g_fp);
    }
    else
    {
      // if is large_file
      if (file_name[0] == 'L' && ret_tfs_name[0] == 'L'
          && source_size == dest_size)
      {
        TBSYS_LOG(INFO, "sync file(%s) succeed.", file_name.c_str());
        {
          tbutil::Mutex::Lock lock(g_mutex_);
          g_sync_stat_.success_count_++;
        }
        fprintf(g_sync_succ, "%s\n", file_name.c_str());
        b_suffix ? snprintf(name_map, 256, "%s %s%s\n", file_name.c_str(), ret_tfs_name, real_suffix.c_str())
                   : snprintf(name_map, 256, "%s %s\n", file_name.c_str(), ret_tfs_name);
        fputs(name_map, g_fp);
      }
      else
      {
        TBSYS_LOG(INFO, "source crc: %u is not the same as dest crc: %u or source_size: %"PRI64_PREFIX"d is not the same as dest size: %"PRI64_PREFIX"d ", source_crc, dest_crc, source_size, dest_size);
        TBSYS_LOG(INFO, "sync file(%s) failed.", file_name.c_str());
        {
          tbutil::Mutex::Lock lock(g_mutex_);
          g_sync_stat_.fail_count_++;
        }
        fprintf(g_sync_fail, "%s\n", file_name.c_str());
      }
    }

    tbutil::Mutex::Lock lock(g_mutex_);
    g_sync_stat_.actual_count_++;

  }
  if (access(tmp, F_OK) == 0)
  {
    unlink(tmp);
  }
  return ret;
}

void trim(const char* input, char* output)
{
  if (output == NULL)
  {
    fprintf(stderr, "output str is null");
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
