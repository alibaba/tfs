/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_batch_write.cpp 155 2011-02-21 07:33:27Z zongdai@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <string>
#include <set>
#include <stdio.h>
#include "common/func.h"
#include "common/define.h"
#include "new_client/tfs_session.h"
#include "new_client/tfs_file.h"
#include "util.h"
#include "thread.h"

using namespace KFS;
using namespace tfs::client;
using namespace tfs::common;
using namespace std;

char global_write_buf[BUFFER_SIZE];

void* write_worker(void* arg)
{
  ThreadParam param = *(ThreadParam*) (arg);

  char file_list_name[100];
  sprintf(file_list_name, "wf_%d", param.index_);
  int write_fd = open(file_list_name, O_CREAT | O_RDWR | O_APPEND, 0660);
  if (-1 == write_fd)
  {
    fprintf(stderr, "index(%d) open write file failed, exit\n", param.index_);
    return NULL;
  }

/*
  param.locker_->lock();
  TfsSession session;
  session.init((char*) param.conf_.c_str(), NULL, true);
  session.init(NULL, nsip);
  param.locker_->unlock();
  TfsFile* tfs_file = new TfsFile();
  tfs_file->set_session(&session);
  if (NULL == tfs_file)
  {
    fprintf(stderr, "session not init.\n");
    return NULL;
  }
*/
  printf("init connection to nameserver:%s\n", param.ns_ip_port_.c_str());
  TfsClient* tfsclient = TfsClient::Instance();
	int iret = tfsclient->initialize(param.ns_ip_port_.c_str());
	if (iret != TFS_SUCCESS)
	{
		return NULL;
	}
  set<std::string> file_name_set;

  char* data = new char[param.max_size_];
  memset(data, 0, param.max_size_);
  generate_data(data, param.max_size_);
  uint32_t write_size = param.max_size_;
  bool random = param.max_size_ > param.min_size_;

  int32_t ret = TFS_SUCCESS;
  int64_t time_consumed = 0;
  //uint32_t block_id = 0;
  //uint64_t file_id = 0;
  uint32_t failed_count = 0;
  char name_buf[50];

  int64_t min_time_consumed = 0, max_time_consumed = 0, accumlate_time_consumed = 0;

  Stater stater("write");
  Timer timer, total_timer;
  total_timer.start();

  int32_t i = 0;
  int fd;
  char ret_name[FILE_NAME_LEN+1];
  for (i = 0; i < param.file_count_; ++i)
  {
    timer.start();

    // open a remote file
    ret = retry_open_file(tfsclient, NULL, (char*)".jpg", T_WRITE, fd);
    // get block id and file id from remote file name
    //convname(tfsclient.get_file_name(), (char*)".jpg", block_id, file_id);
    if (ret != EXIT_SUCCESS)
    {
      fprintf(stderr, "index:%d, tfsopen failed\n", param.index_);
      ++failed_count;
      continue;
    }
    else
    {
      time_consumed = timer.consume();
      if (param.profile_)
      {
        printf("index:%d, tfsopen ok, spend (%" PRI64_PREFIX "d)\n", param.index_, time_consumed);
      }
    }

    if (random)
    {
      srand(time(NULL) + rand() + pthread_self());
      write_size = rand() % (param.max_size_ - param.min_size_) + param.min_size_;
    }
    param.file_size_ += write_size;

    ret = write_data(tfsclient, fd, data, write_size);
    if (ret < 0)
    {
      fprintf(stderr, "index:%d, tfswrite failed\n", param.index_); 
      tfsclient->close(fd);
      ++failed_count;
    }
    else
    {
      time_consumed = timer.consume();
      if (param.profile_)
      {
        printf("index:%d, tfswrite completed, spend (%" PRI64_PREFIX "d)\n", param.index_, time_consumed);
        print_rate(ret, time_consumed);
      }
      ret = tfsclient->close(fd, ret_name, FILE_NAME_LEN+1);
      if (ret == TFS_SUCCESS)
      {
        time_consumed = timer.consume();
        stater.stat_time_count(time_consumed);
        if (i == 0)
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

        if (param.profile_)
        {
          printf("index:%d, tfs_close (%s) completed, spend (%" PRI64_PREFIX "d)\n", param.index_, 
              ret_name, time_consumed);
        }

        //std::string filename = tfsclient.get_file_name();
        if (file_name_set.find(ret_name) != file_name_set.end())
        {
          fprintf(stderr, "error ! filename (%s) duplicate\n", ret_name);
        }
        else
        {
          file_name_set.insert(ret_name);
          sprintf(name_buf, "%s\n", ret_name);
          write(write_fd, name_buf, strlen(name_buf));
        }
      }
      else
      {
        fprintf(stderr, "index:%d, tfsclose failed\n", param.index_); 
        ++failed_count;
      }
    }
  }

  time_consumed = total_timer.consume();
  int32_t success_count = param.file_count_ - failed_count;
  ((ThreadParam*) arg)->succ_count_ = success_count;
  ((ThreadParam*) arg)->succ_time_consumed_ = accumlate_time_consumed;
  ((ThreadParam*) arg)->fail_time_consumed_ = time_consumed - accumlate_time_consumed;
  ((ThreadParam*) arg)->file_size_ = param.file_size_;

  double iops = calc_iops(success_count, time_consumed);
  double rate = calc_rate(success_count, time_consumed);

  double aiops = calc_iops(success_count, accumlate_time_consumed);
  double arate = calc_rate(success_count, accumlate_time_consumed);

  printf(
      "index  count  succ   fail   succ_time  fail_time  min      max      avg      iops     rate      aiops    arate \n");
  printf("%-6d %-6d %-6d %-6d %-10" PRI64_PREFIX "d %-10" PRI64_PREFIX "d %-8" PRI64_PREFIX "d %-8" PRI64_PREFIX "d %-8" PRI64_PREFIX "d %-8.3f %-8.3f %-8.3f %-8.3f\n", param.index_,
      param.file_count_, success_count, failed_count, accumlate_time_consumed, time_consumed - accumlate_time_consumed,
      min_time_consumed, max_time_consumed, (!success_count)? 0 : (accumlate_time_consumed / success_count), iops, rate, aiops, arate);
  stater.dump_time_stat();
  close(write_fd);

  delete[] data;
  return NULL;
}

int main(int argc, char** argv)
{
  int32_t thread_count = 1;
  ThreadParam input_param;
  int32_t ret = fetch_input_opt(argc, argv, input_param, thread_count);
  if (ret != TFS_SUCCESS || input_param.ns_ip_port_.empty() || input_param.file_count_ == 0 || thread_count > THREAD_SIZE)
  {
    printf("usage: -d nsip:port -c file_count -r size_range\n");
    exit(-1);
  }

  if ((input_param.min_size_ > input_param.max_size_) || (0 == input_param.max_size_))
  {
    printf("usage: -d nsip:port -c file_count -r size_range, must size range. \n");
    exit(-1);
  }

  int64_t time_start = Func::curr_time();
  MetaThread threads[THREAD_SIZE];
  ThreadParam param[THREAD_SIZE];
  tbsys::CThreadMutex glocker;

  for (int32_t i = 0; i < thread_count; ++i)
  {
    param[i].index_ = i;
    param[i].conf_ = input_param.conf_;
    param[i].file_count_ = input_param.file_count_;
    param[i].file_size_ = input_param.file_size_;
    param[i].min_size_ = input_param.min_size_;
    param[i].max_size_ = input_param.max_size_;
    param[i].profile_ = input_param.profile_;
    param[i].ns_ip_port_ = input_param.ns_ip_port_;

    // param[i].self = &threads[i];
    param[i].locker_ = &glocker;
    threads[i].start(write_worker, (void*) &param[i]);
  }

  for (int32_t i = 0; i < thread_count; ++i)
  {
    threads[i].join();
  }

  int32_t total_count = 0;
  int32_t total_succ_count = 0;
  int32_t total_fail_count = 0;
  int64_t total_succ_consumed = 0;
  int64_t total_fail_consumed = 0;
  int64_t total_file_size = 0;
  for (int32_t i = 0; i < thread_count; ++i)
  {
    total_count += param[i].file_count_;
    total_file_size += param[i].file_size_;
    total_succ_count += param[i].succ_count_;
    total_succ_consumed += param[i].succ_time_consumed_;
    total_fail_consumed += param[i].fail_time_consumed_;
  }

  total_fail_count = total_count - total_succ_count;

  int64_t time_stop = Func::curr_time();
  int64_t time_consumed = time_stop - time_start;

  double iops = calc_iops(total_count, time_consumed);
  double siops = calc_iops(total_succ_count, time_consumed);
  double fiops = calc_iops(total_fail_count, time_consumed);
  double srate = calc_rate(total_succ_count, total_succ_consumed);
  double frate = calc_rate(total_fail_count, total_fail_consumed);
  double asize = total_file_size / total_count;
  printf("thread_num count filesize     iops    siops     fiops     srate       frate\n");
  printf("%10d  %4d %5.2f  %8.3f %8.3f %8.3f %10.3f  %10.3f\n", thread_count, total_count, asize, iops, siops, fiops,
      srate, frate);
}

