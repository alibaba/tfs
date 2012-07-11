/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: util.h 575 2011-07-18 06:51:44Z daoan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "new_client/tfs_client_api.h"
#include <tbsys.h>
#include <sys/types.h>
#include <sys/time.h>
#include "thread.h"
#include <string>
#include <stdint.h>
#include <limits.h>
#include <map>

#define THREAD_SIZE 500
#define BUFFER_SIZE 4096 * 1000
#define RETRY_TIMES 3
#define FILE_COUNT  100
//#define MAX_READ_SIZE 1048576
typedef unsigned int uint32;

using tfs::client::TfsClient;

struct ThreadParam
{
  int32_t index_;
  int32_t file_count_;
  int32_t succ_count_;
  int32_t file_size_;
  int32_t min_size_;
  int32_t max_size_;
  bool profile_;
  bool random_;

  int64_t succ_time_consumed_;
  int64_t fail_time_consumed_;

  tbsys::CThreadMutex* locker_;
  //add 
  std::string conf_;
  std::string oper_ratio_;
  std::string sample_file_;
  std::string ns_ip_port_;
  ThreadParam() :
    index_(0), file_count_(0), succ_count_(0), file_size_(0), min_size_(0),
    max_size_(0), profile_(false), random_(false), succ_time_consumed_(0), 
    fail_time_consumed_(0)
  {
    oper_ratio_ = "200:8:2:1";
  }
};

struct TimeConsumed
{
  int64_t time_consumed_;
  int64_t min_time_consumed_;
  int64_t max_time_consumed_;
  int64_t accumlate_time_consumed_;

  uint32 total_count_;
  uint32 fail_count_;
  const std::string consume_name_;
  TimeConsumed(const std::string& name) :
    time_consumed_(0), min_time_consumed_(LONG_LONG_MAX), max_time_consumed_(0), accumlate_time_consumed_(0),
        total_count_(0), fail_count_(0), consume_name_(name)
  {
  }

  uint32 succ_count()
  {
    return total_count_ - fail_count_;
  }

  uint64_t avg()
  {
    if ((total_count_ - fail_count_) == 0)
    {
      return 0;
    }

    return accumlate_time_consumed_ / (total_count_ - fail_count_);
  }

  void process();
  void display();
};

int retry_open_file(TfsClient* tfsclient, char* filename, char* prefix, int mode, int& fd);
void print_rate(unsigned int num, double time_taken);
uint32_t generate_data(char* buf, uint32_t size);
int read_local_file(const char* filename, char* data, uint32_t& length);
int write_data(TfsClient* tfsclient, int fd, char* data, uint32_t length);
int read_data(TfsClient* tfsclient, char* filename);
int copy_file_v2(TfsClient* tfsclient, char* tfsname, int local_fd);
//int copy_file_v3(TfsClient &tfsclient, char* tfsname, uint32_t width, uint32_t height, int local_fd, bool & zoomed);
int copy_file(TfsClient* tfsclient, char* tfsname, int local_fd);
int fetch_input_opt(int argc, char** argv, ThreadParam& param, int& thread_count);
uint32_t convname(const char* tfsname, char* prefix, uint32_t& blockid, uint64_t& fileid);
double calc_iops(int32_t count, int64_t consumed);
double calc_rate(int32_t count, int64_t consumed);

class Timer
{
public:
  Timer();
  int64_t start();
  int64_t consume();
  int64_t current();

private:
  int64_t start_;
  int64_t end_;
};

class Stater
{
public:
  Stater(const std::string& name) :
    stat_name_(name)
  {
  }
  int stat_time_count(uint64_t cost_time);
  void dump_time_stat();
private:
  uint32_t time_category(uint64_t cost_time);
  std::string time_category_desc(uint32_t category);
private:
  std::map<uint32_t, uint64_t> time_stat_map_;
  const std::string stat_name_;
};
