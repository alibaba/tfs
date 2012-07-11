/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_by_file.cpp 479 2011-06-10 08:32:47Z daoan@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#include <vector>
#include <algorithm>
#include <functional>
#include <Mutex.h>
#include <Timer.h>
#include <Handle.h>
#include <sys/types.h>
#include <pthread.h>
#include <Memory.hpp>
#include <tbsys.h>
#include <curses.h>
#include "common/func.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "new_client/tfs_client_api.h"

using namespace tfs::common;
using namespace tfs::client;
using namespace tbsys;
using namespace std;

struct StatParam
{
  int64_t  total_;
  atomic_t copy_none_;
  atomic_t copy_success_;
  atomic_t copy_failure_;

  void dump(void)
  {
    fprintf(stderr, "[copy] total: %"PRI64_PREFIX"d, success: %d, fail: %d, none: %d\n", total_, atomic_read(&copy_success_), atomic_read(&copy_failure_), atomic_read(&copy_none_));
  }
};

static StatParam gstat;
class WorkThread : public tbutil::Thread
{
  public:
    WorkThread();
    WorkThread(std::string& ip, std::string& target, int32_t timestamp_day):
      src_ns_ip_port_(ip),
      target_ns_ip_port_(target),
      TIMESTAMP_DAY(timestamp_day),
      destroy_(false)
  {

  }

    virtual ~WorkThread()
    {

    }

    int initialize()
    {
      int iret = TfsClient::Instance()->initialize(NULL, 0, 0);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "tfsclient initialize failed");
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    void dump(FILE* file, FILE* uncomplete_file)
    {
      bool bret = file != NULL && uncomplete_file != NULL && !errors_.empty();
      if (bret)
      {
        vector<std::string>::iterator iter = errors_.begin();
        char name_buf[0xff];
        for(; iter != errors_.end(); ++iter)
        {
          strncpy(name_buf, (*iter).c_str(), FILE_NAME_LEN);
          name_buf[FILE_NAME_LEN] = '\n';
          name_buf[FILE_NAME_LEN + 1] = '\0';
          fwrite(name_buf, strlen(name_buf), 1, file);
        }
        iter = files_.begin();
        for (; iter != files_.end(); ++iter)
        {
          strncpy(name_buf, (*iter).c_str(), FILE_NAME_LEN);
          name_buf[FILE_NAME_LEN] = '\n';
          name_buf[FILE_NAME_LEN + 1] = '\0';
          fwrite(name_buf, strlen(name_buf), 1, uncomplete_file);
        }
      }
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
      int32_t count = 0;
      const int16_t MAX_COUNT_RESET = 32;
      int32_t iret = TFS_ERROR;
      std::string file;
      TfsFileStat src_file_info;
      TfsFileStat target_file_info;
      while(!destroy_ && !files_.empty())
      {
        if (count > MAX_COUNT_RESET)
        {
          count = 0;
          gstat.dump();
        }
        ++count;
        file = files_.back();
        files_.pop_back();
        memset(&src_file_info, 0, sizeof(src_file_info));
        memset(&target_file_info, 0, sizeof(target_file_info));

        const char* prefix = NULL;//file.c_str() + FILE_NAME_LEN;
        int tfs_fd = TfsClient::Instance()->open(file.c_str(), prefix, src_ns_ip_port_.c_str(), T_READ);
        if (tfs_fd < 0)
        {
          fprintf(stderr, "open tfsfile error %s\n", file.c_str());
          errors_.push_back(file);
          atomic_inc(&gstat.copy_failure_);
          continue;
        }

        iret = TfsClient::Instance()->fstat(tfs_fd, &src_file_info);
        if (iret != TFS_SUCCESS)
        {
          fprintf(stderr, "stat tfsfile error %s\n", file.c_str());
          errors_.push_back(file);
          atomic_inc(&gstat.copy_failure_);
          TfsClient::Instance()->close(tfs_fd);
          continue;
        }

        int target_tfs_fd = TfsClient::Instance()->open(file.c_str(), prefix, target_ns_ip_port_.c_str(), T_READ);
        if (target_tfs_fd < 0)
        {
          fprintf(stderr, "open tfsfile(target) error: %s\n", file.c_str());
          errors_.push_back(file);
          atomic_inc(&gstat.copy_failure_);
          TfsClient::Instance()->close(tfs_fd);
          continue;
        }

        iret = TfsClient::Instance()->fstat(target_tfs_fd, &target_file_info);
        if (iret != TFS_SUCCESS)
        {
          fprintf(stderr, "stat tfsfile(target) error: %s\n", file.c_str());
        }

        time_t now = time(NULL) - 60;
        if (src_file_info.modify_time_ > now
            || (target_file_info.file_id_ != 0 && target_file_info.modify_time_ > now))
        {
          fprintf(stderr, "[INFO]: %s is new file\n", file.c_str());
          atomic_inc(&gstat.copy_none_);
        }
        else if (src_file_info.flag_ == 0
            && (target_file_info.file_id_ == 0 || src_file_info.size_ != target_file_info.size_
              || src_file_info.crc_ != target_file_info.crc_)
            && (TIMESTAMP_DAY <= 0 || src_file_info.modify_time_ < (now - TIMESTAMP_DAY * 86400))) 
        {
          int32_t max_size = (src_file_info.size_ / MAX_READ_SIZE + 1) * MAX_READ_SIZE; 
          char* data = new char[max_size];
          int32_t len = 0;
          int32_t offset = 0;
          uint32_t crc = 0;
          while ((len = TfsClient::Instance()->read(tfs_fd, (data+offset), MAX_READ_SIZE)) > 0)
          {
            crc = Func::crc(crc, (data+offset), len);
            offset += len;
            if (len < MAX_READ_SIZE)
              break;
          }
          if (src_file_info.crc_ != crc
              || src_file_info.size_ != offset)
          {
            fprintf(stderr, "read tfsfile(src) fail: old crc(%u) != new crc(%u) or old size(%"PRI64_PREFIX"d) != new size(%d)\n", src_file_info.crc_, crc, src_file_info.size_, offset);
            tbsys::gDeleteA(data);
            errors_.push_back(file);
            atomic_inc(&gstat.copy_failure_);
            TfsClient::Instance()->close(tfs_fd);
            TfsClient::Instance()->close(target_tfs_fd);
            continue;
          }
          TfsClient::Instance()->close(target_tfs_fd);


          target_tfs_fd = TfsClient::Instance()->open(file.c_str(), prefix, target_ns_ip_port_.c_str(), T_WRITE);
          if (target_tfs_fd < 0)
          {
            fprintf(stderr, "open tfsfile(target) error: %s\n", file.c_str());
            tbsys::gDeleteA(data);
            errors_.push_back(file);
            atomic_inc(&gstat.copy_failure_);
            TfsClient::Instance()->close(tfs_fd);
            continue;
          }
          int32_t write_len = 0;
          int32_t write_offset = 0;
          bool complete = true;
          while (offset > 0)
          {
            write_len = MAX_READ_SIZE;
            write_len = std::min(write_len, MAX_READ_SIZE);
            len = TfsClient::Instance()->write(target_tfs_fd, (data+write_offset), write_len);
            if (len != write_len)
            {
              fprintf(stderr, "write tfsfile(target) error: %s\n", file.c_str());
              tbsys::gDeleteA(data);
              errors_.push_back(file);
              atomic_inc(&gstat.copy_failure_);
              complete = false;
              break;
            }
            write_offset += len;
            offset -= len;
          }
          if (complete)
          {
            atomic_inc(&gstat.copy_success_);
          }
        }
        else
        {
          atomic_inc(&gstat.copy_none_);
        }
        TfsClient::Instance()->close(tfs_fd);
        TfsClient::Instance()->close(target_tfs_fd);
      }
    }

    void push_back(const char* name)
    {
      files_.push_back(name);
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    std::vector<std::string> files_;
    std::vector<std::string> errors_;
    std::string src_ns_ip_port_;
    std::string target_ns_ip_port_;
    std::string file_path_;
    const int32_t TIMESTAMP_DAY;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static int32_t thread_count = 1;
static WorkThreadPtr* gworks = NULL;
static FILE* gfile = NULL;
static FILE* guncomplete_file = NULL;

void helper()
{
  std::string options=
    "Options:\n"
    "-s                 src ip:port\n"
    "-d                 target ip:port\n"
    "-t                 thread count\n"
    "-f                 file path\n"
    "-o                 output file path\n"
    "-h                 show this message\n"
    "-v                 show porgram version\n";
  fprintf(stderr,"Usage:\n%s" ,options.c_str());
}

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal );
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
  std::string src_ns_ip_port;
  std::string target_ns_ip_port;
  std::string file_path;
  std::string output_file_path;
  int32_t index = 0;
  int32_t timestamp_day = 0;
  bool help = false;
  while ((index = getopt(argc, argv, "s:d:f:t:o:vh")) != EOF)
  {
    switch (index)
    {   
      case 's':
        src_ns_ip_port = optarg;
        break;
      case 'd':
        target_ns_ip_port = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'o':
        output_file_path = optarg;
        break;
      case 'n':
        timestamp_day = atoi(optarg);
        break;
      case 'v':
        break;
      case 'h':
      default:
        help = true;
        break;
    }
  }
  help = src_ns_ip_port.empty() || target_ns_ip_port.empty() || help || file_path.empty();
  if (help)
  {
    helper();
    return TFS_ERROR;
  }

  {
    if (access(output_file_path.c_str(), W_OK) < 0)
    {
      fprintf(stderr, "access file(%s) error: %s\n", output_file_path.c_str(), strerror(errno));
      return TFS_ERROR;
    }

    if (access(file_path.c_str(), R_OK) < 0)
    {
      fprintf(stderr, "access file(%s) error: %s\n", file_path.c_str(), strerror(errno));
      return TFS_ERROR;
    }

    gfile = fopen(output_file_path.c_str(), "w+");
    if (gfile == NULL)
    {
      fprintf(stderr, "open output file(%s) error: %s\n", output_file_path.c_str(), strerror(errno));
      return TFS_ERROR;
    }

    std::string uncomplete_file_path = file_path.c_str() + string(".uncomplete");
    guncomplete_file = fopen(uncomplete_file_path.c_str(), "w+");
    if (guncomplete_file == NULL)
    {
      fprintf(stderr, "open uncomplete output file(%s) error: %s\n", uncomplete_file_path.c_str(), strerror(errno));
      return TFS_ERROR;
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, SIG_IGN);                                                                   
    signal(SIGTERM, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_ip_port, target_ns_ip_port, timestamp_day);
    }

    memset(&gstat, 0, sizeof(gstat));

    FILE* file = fopen(file_path.c_str(), "rb");
    if (file == NULL)
    {
      fprintf(stderr, "open read file(%s) failed, exit\n", file_path.c_str());
      tbsys::gDeleteA(gworks);
      fclose(gfile);
      return TFS_ERROR;
    }
    else
    {
      int32_t index = 0;
      int64_t count = 0;
      char name[0xff] = {'\0'};
      while (fgets(name, 0xff, file))
      {
        index = count % thread_count;
        gworks[index]->push_back(name);
        ++count;
        memset(name, 0, sizeof(name));
        ++gstat.total_;
      }

      fclose(file);

      bool all_complete = true;
      for (i = 0; i < thread_count; ++i)
      {
        if (gworks[i]->initialize() != TFS_SUCCESS)
        {
          fprintf(stderr, "work thread initialize fail, must be exit\n");
          all_complete = false;
          break;
        }
        gworks[i]->start();
      }
      if (!all_complete)
      {
        for (i = 0; i < thread_count; ++i)
        {
          gworks[i]->destroy();
        }
      }
    }

    signal(SIGHUP, interruptcallback);
    signal(SIGINT, interruptcallback);                                                           
    signal(SIGTERM, interruptcallback);
    signal(SIGUSR1, interruptcallback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
      gworks[i]->dump(gfile, guncomplete_file);
    }

    fclose(gfile);
    fclose(guncomplete_file);
    tbsys::gDeleteA(gworks);

    gstat.dump();
  }
  return TFS_SUCCESS;
}
