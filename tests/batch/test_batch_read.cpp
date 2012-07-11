/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_batch_read.cpp 155 2011-02-21 07:33:27Z zongdai@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "util.h"
#include "thread.h"
#include "common/func.h"
#include "new_client/tfs_client_api.h"
#include <vector>
#include <algorithm>
#include <functional>

using namespace KFS;
using namespace tfs::client;
using namespace tfs::common;
using namespace std;

static int32_t m_stop = 0;
void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
      m_stop = 1;
      break;
  }
}

#define FILE_NAME_LEN 18

void* read_worker(void* arg)
{
  ThreadParam param = *(ThreadParam*) (arg);

  char file_list_name[100];
  sprintf(file_list_name, "wf_%d", param.index_);
  FILE* input_fd = fopen(file_list_name, "rb");
  if (NULL == input_fd)
  {
    printf("index(%d) open read file failed, exit\n", param.index_);
    return NULL;
  }

  /*
     param.locker_->lock();
     TfsSession session;
     session.init((char*) param.conf_.c_str());
     param.locker_->unlock();
     TfsFile tfs_file;
     tfs_file.set_session(&session);
     */
  printf("init connection to nameserver:%s\n", param.ns_ip_port_.c_str());
  TfsClient* tfsclient = TfsClient::Instance();
  int iret = tfsclient->initialize(param.ns_ip_port_.c_str());
  if (iret == TFS_ERROR)
  {
    return NULL;
  }
  Timer timer;
  Stater stater("read");
  const int32_t BUFLEN = 32;
  char name_buf[BUFLEN];
  int32_t failed_count = 0;
  int32_t zoomed_count = 0;
  int32_t total_count = 0;
  int64_t time_start = Func::curr_time();
  int64_t time_consumed = 0, min_time_consumed = 0, max_time_consumed = 0, accumlate_time_consumed = 0;

  // 1M file number, each 18 bit, total 18M
  vector < std::string > write_file_list;
  while (fgets(name_buf, BUFLEN, input_fd))
  {
    ssize_t endpos = strlen(name_buf) - 1;
    while (name_buf[endpos] == '\n')
    {
      name_buf[endpos] = 0;
      --endpos;
    }
    write_file_list.push_back(name_buf);
  }
  fclose(input_fd);

  //random read
  if (param.random_)
  {
    vector<std::string>::iterator start = write_file_list.begin();
    vector<std::string>::iterator end = write_file_list.end();
    random_shuffle(start, end);
  }

  /*bool read_image = false;
  bool image_scale_random = false;
  if ((param.max_size_ != 0) && (param.min_size_ != 0))
  {
    read_image = true;
  }
  if (param.max_size_ != param.min_size_)
  {
    image_scale_random = true;
  }*/

  vector<std::string>::iterator vit = write_file_list.begin();
  for (; vit != write_file_list.end(); vit++)
  {
    if (m_stop)
    {
      break;
    }
    if ((param.file_count_ > 0) && (total_count >= param.file_count_))
    {
      break;
    }
    timer.start();
    int32_t ret = 0;
    /*bool zoomed = true;
    if (read_image)
    {
      if (image_scale_random)
      {
        srand(time(NULL) + rand() + pthread_self());
        int32_t min_width = (param.max_size_ / 6);
        int32_t min_height = (param.min_size_ / 6);
        int32_t scale_width = rand() % (param.max_size_ - min_width) + min_width;
        int32_t scale_height = rand() % (param.min_size_ - min_height) + min_height;
        ret = copy_file_v3(tfsclient, (char *) (*vit).c_str(), scale_width, scale_height, -1, zoomed);
        if (zoomed)
        {
          ++zoomed_count;
        }
      }
    }
    else*/
    {
      ret = copy_file_v2(tfsclient, (char *) (*vit).c_str(), -1);
    }
    if (ret != TFS_SUCCESS)
    {
      printf("read file fail %s ret : %d\n", (*vit).c_str(), ret);
      ++failed_count;
    }
    else
    {
      time_consumed = timer.consume();
      stater.stat_time_count(time_consumed);
      if (total_count == 0)
      {
        min_time_consumed = time_consumed;
      }
      if (time_consumed < min_time_consumed)
      {
        min_time_consumed = time_consumed;
      }
      if (time_consumed > max_time_consumed)
      {
        max_time_consumed = time_consumed;
      }
      accumlate_time_consumed += time_consumed;
    }
    if (param.profile_)
    {
      printf("read file (%s), (%s)\n", (*vit).c_str(), ret == TFS_SUCCESS ? "success" : "failed");
      print_rate(ret, time_consumed);
    }
    ++total_count;
  }

  uint32_t total_succ_count = total_count - failed_count;
  ((ThreadParam*) arg)->file_count_ = total_succ_count;
  int64_t time_stop = Func::curr_time();
  time_consumed = time_stop - time_start;

  double iops = calc_iops(total_succ_count, time_consumed);
  double rate = calc_rate(total_succ_count, time_consumed);
  double aiops = calc_iops(total_succ_count, accumlate_time_consumed);
  double arate = calc_rate(total_succ_count, accumlate_time_consumed);

  printf("thread index:%5d   count:%5d  failed:%5d, zoomed:%5d, filesize:%6d  iops:%10.3f  rate:%10.3f ,"
      " min:%" PRI64_PREFIX "d, max:%" PRI64_PREFIX "d,avg:%" PRI64_PREFIX "d aiops:%10.3f, arate:%10.3f \n", param.index_, total_count, failed_count, zoomed_count,
      param.file_size_, iops, rate, min_time_consumed, max_time_consumed, (!total_succ_count)? 0:(accumlate_time_consumed / total_succ_count),
      aiops, arate);
  stater.dump_time_stat();
  return NULL;
}

int main(int argc, char* argv[])
{
  int32_t thread_count = 1;
  ThreadParam input_param;
  int32_t ret = fetch_input_opt(argc, argv, input_param, thread_count);
  if (ret != TFS_SUCCESS || input_param.ns_ip_port_.empty() || thread_count > THREAD_SIZE)
  {
    printf("usage: -d -t \n");
    exit(-1);
  }

  //if (thread_count > 0) ClientPoolCollect::setClientPoolCollectParam(thread_count,0,0);
  signal(SIGINT, sign_handler);

  MetaThread threads[THREAD_SIZE];
  ThreadParam param[THREAD_SIZE];
  tbsys::CThreadMutex glocker;

  int64_t time_start = Func::curr_time();
  for (int32_t i = 0; i < thread_count; ++i)
  {
    param[i].index_ = i;
    param[i].conf_ = input_param.conf_;
    param[i].file_count_ = input_param.file_count_;
    param[i].file_size_ = input_param.file_size_;
    param[i].min_size_ = input_param.min_size_;
    param[i].max_size_ = input_param.max_size_;
    param[i].profile_ = input_param.profile_;
    param[i].random_ = input_param.random_;
    param[i].locker_ = &glocker;
    param[i].ns_ip_port_ = input_param.ns_ip_port_;
    threads[i].start(read_worker, (void*) &param[i]);
  }

  for (int32_t i = 0; i < thread_count; ++i)
  {
    threads[i].join();
  }

  int32_t total_count = 0;
  for (int32_t i = 0; i < thread_count; ++i)
  {
    total_count += param[i].file_count_;
  }
  int64_t time_stop = Func::curr_time();
  int64_t time_consumed = time_stop - time_start;
  double iops = static_cast<double>(total_count) / (static_cast<double>(time_consumed) / 1000000);
  double rate = static_cast<double>(time_consumed) / total_count;

  printf("read_thread count filesize     iops         rate\n");
  printf("%10d  %5d %9d %8.3f %12.3f \n", thread_count, total_count, input_param.file_size_, iops, rate);
}

