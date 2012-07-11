/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_batch_mix.cpp 575 2011-07-18 06:51:44Z daoan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <vector>
#include <algorithm>
#include <functional>
#include "common/func.h"
#include "new_client/tfs_client_api.h"
#include "util.h"
#include "thread.h"

using namespace KFS;
using namespace tfs::common;
using namespace tfs::client;
using namespace std;

#define FILE_NAME_LEN 18

int serialize(char* oper, vector<std::string>& operlist, int32_t index, int32_t flag)
{
  char file_list_name[100];
  sprintf(file_list_name, "%sf_%d", oper, index);
  int fd = open(file_list_name, flag, 0660);
  if (fd == -1)
  {
    fprintf(stderr, "index(%d) open %s file failed,  exit, %s=============\n", index, oper, strerror(errno));
    return -1;
  }

  const int32_t BUFLEN = 32;
  char name_buf[BUFLEN];
  vector<std::string>::iterator start = operlist.begin();
  vector<std::string>::iterator end = operlist.end();
  random_shuffle(start, end);
  vector<std::string>::iterator vit = operlist.begin();
  for (; vit != operlist.end(); vit++)
  {
    sprintf(name_buf, "%s\n", (*vit).c_str());
    write(fd, name_buf, strlen(name_buf));
  }
  close(fd);
  return 0;
}

int write_file(ThreadParam& param, TfsClient* tfsclient, char* tfs_name, vector<std::string>& file_list,
    TimeConsumed& write_time_consumed, Stater& write_stater, const char* data)
{
  //uint32_t block_id = 0;
  //uint64_t file_id = 0;
  bool random = param.max_size_ > param.min_size_;
  uint32_t write_size = param.max_size_;
  Timer timer;
  timer.start();
  int fd;
  int ret = retry_open_file(tfsclient, tfs_name, (char*) ".jpg", T_WRITE, fd);
  //convname(tfs_file->get_file_name(), (char*) ".jpg", block_id, file_id);
  if (ret != EXIT_SUCCESS)
  {
    fprintf(stderr, "index:%d, tfsopen failed\n", param.index_);
    ++write_time_consumed.fail_count_;
    ++write_time_consumed.total_count_;
    return ret;
  }
  else
  {
    write_time_consumed.time_consumed_ = timer.consume();
    if (param.profile_)
    {
      printf("index:%d, tfsopen ok. spend (%" PRI64_PREFIX "d)\n", param.index_,
          write_time_consumed.time_consumed_);
    }
  }

  if (random)
  {
    struct timeval val;
    gettimeofday(&val, NULL);
    srand(val.tv_usec);
    write_size = rand() % (param.max_size_ - param.min_size_) + param.min_size_;
  }

  param.file_size_ += write_size;
  ret = write_data(tfsclient, fd, const_cast<char*>(data), write_size);
  if (ret < 0)
  {
    fprintf(stderr, "index:%d, tfswrite failed\n", param.index_);
    tfsclient->close(fd);
    ++write_time_consumed.fail_count_;
    ++write_time_consumed.total_count_;
    return ret;
  }
  else
  {
    write_time_consumed.time_consumed_ = timer.consume();
    if (param.profile_)
    {
      printf("index:%d, tfswrite completed, spend (%" PRI64_PREFIX "d)\n", param.index_,
        write_time_consumed.time_consumed_);
    }

    char ret_name[FILE_NAME_LEN+1];
    if (!tfs_name)
      ret = tfsclient->close(fd, ret_name, FILE_NAME_LEN+1);
    else
      ret = tfsclient->close(fd);
    if (ret == TFS_SUCCESS)
    {
      write_time_consumed.time_consumed_ = timer.consume();
      write_stater.stat_time_count(write_time_consumed.time_consumed_);
      write_time_consumed.process();

      if (param.profile_)
      {
        printf("index:%d, tfs_close, completed, spend (%" PRI64_PREFIX "d)\n", param.index_,
            write_time_consumed.time_consumed_);
      }
      if (!tfs_name)
      {
        file_list.push_back(ret_name);
      }
    }
    else
    {
      std::string type = (!tfs_name) ? "writeop" : "updateop";
      fprintf(stderr, "index:%d, tfs_close failed. type: %s.\n", param.index_, type.c_str());
      ++write_time_consumed.fail_count_;
      ++write_time_consumed.total_count_;
      return ret;
    }
  }

  ++write_time_consumed.total_count_;
  return 0;
}

void* mix_worker(void* arg)
{

  ThreadParam param = *(ThreadParam*) (arg);

  int32_t read_ratio = 200, write_ratio = 8, delete_ratio = 2, update_ratio = 1;
  std::string::size_type pos = param.oper_ratio_.find_first_of(":");
  int32_t i = 0;
  int32_t pre_pos = 0;
  while (std::string::npos != pos)
  {
    if (0 == i)
    {
      read_ratio = atoi(param.oper_ratio_.substr(pre_pos, pos).c_str());
      if (0 == read_ratio)
      {
        fprintf(stderr, "read_ratio can not be zero\n");
        assert(false);
      }
    }
    else if (1 == i)
    {
      write_ratio = atoi(param.oper_ratio_.substr(0, pos).c_str());
    }
    else if (2 == i)
    {
      delete_ratio = atoi(param.oper_ratio_.substr(0, pos).c_str());
    }
    else if (3 == i)
    {
      update_ratio = atoi(param.oper_ratio_.substr(0, pos).c_str());
    }

    i++;
    pre_pos = pos + 1;
    param.oper_ratio_ = param.oper_ratio_.substr(pre_pos, std::string::npos);
    pos = param.oper_ratio_.find_first_of(":");
  }
  if ((3 == i) && (param.oper_ratio_.size() > 0))
  {
    update_ratio = atoi(param.oper_ratio_.c_str());
  }
  int32_t PER_READ = 100;
  int32_t PER_WRITE = PER_READ * write_ratio / read_ratio;
  int32_t PER_DELETE = PER_READ * delete_ratio / read_ratio;
  int32_t PER_UPDATE = PER_READ * update_ratio / read_ratio;

  printf(
      "read_ratio: %d, write_ratio: %d,deleteratio: %d,updateratio: %d, perread: %d, perwrite: %d, perdelete: %d, per_update: %d\n",
      read_ratio, write_ratio, delete_ratio, update_ratio, PER_READ, PER_WRITE, PER_DELETE, PER_UPDATE);
  printf("max_size = %d, min_size = %d, random: %d, count = %d\n", param.max_size_, param.min_size_, param.random_,
      param.file_count_);
  char file_list_name[100];
  sprintf(file_list_name, "wf_%d", param.index_);
  FILE* input_fd = fopen(file_list_name, "rw+");
  if (NULL == input_fd)
  {
    printf("index(%d) open read file failed, exit, %s\n", param.index_, strerror(errno));
    return NULL;
  }
  const int32_t BUFLEN = 32;
  char name_buf[BUFLEN];
  vector<std::string> update_list, delete_list;
  vector<std::string> read_file_list, write_file_list;
  while (fgets(name_buf, BUFLEN, input_fd))
  {
    name_buf[FILE_NAME_LEN] = '\0';
    read_file_list.push_back(name_buf);
  }
  fclose(input_fd);

  /*
   param.locker_->lock();
   TfsSession session;
   session.init((char*) param.conf_.c_str());
   param.locker_->unlock();
   TfsFile tfs_file;
   tfs_file.set_session(&session);
   */
  printf("init connection to nameserver:%s\n", param.ns_ip_port_.c_str());
  TfsClient *tfsclient = TfsClient::Instance();
	int iret = tfsclient->initialize(param.ns_ip_port_.c_str());
	if (iret != TFS_SUCCESS)
	{
		return NULL;
	}
  Timer timer;

  Stater read_stater("read"), write_stater("write"), delete_stater("delete"), update_stater("update");
  TimeConsumed read_time_consumed("read"), write_time_consumed("write"), delete_time_consumed("delete"),
      update_time_consumed("update");

  //random read
  if (param.random_)
  {
    vector<std::string>::iterator start = read_file_list.begin();
    vector<std::string>::iterator end = read_file_list.end();
    random_shuffle(start, end);
  }

  int32_t need_times = param.file_count_ / (PER_READ + PER_DELETE + PER_UPDATE);
  int32_t exist_times = read_file_list.size() / (PER_READ + PER_DELETE + PER_UPDATE);
  int32_t loop_times = (need_times <= exist_times) ? need_times : exist_times;
  int32_t loop_times_bak = loop_times;
  //printf("loop_times: %d, need_times: %d, exist_times: %d, filecount: %d, read_file_list size: %d\n", loop_times, need_times, exist_times, param.file_count, read_file_list.size());
  //for write
  char* data = new char[param.max_size_];
  memset(data, 0, param.max_size_);
  generate_data(data, param.max_size_);
  //bool random = param.max_size_ > param.min_size_;

  int64_t time_start = Func::curr_time();
  vector<std::string>::iterator vit = read_file_list.begin();
  vector<std::string>::iterator bit;
  while (loop_times > 0)
  {
    int32_t i = 0;
    for (i = 0; i < PER_READ; i++)
    {
      if (vit == read_file_list.end())
      {
        printf("Read File. read file_list reach end. \n");
        vit = read_file_list.begin();
      }
      timer.start();
      int ret = copy_file(tfsclient, (char*) (*vit).c_str(), -1);
      if (ret != TFS_SUCCESS)
      {
        ++read_time_consumed.fail_count_;
      }
      else
      {
        read_time_consumed.time_consumed_ = timer.consume();
        read_stater.stat_time_count(read_time_consumed.time_consumed_);
        read_time_consumed.process();
      }

      if (param.profile_)
      {
        printf("read file (%s), (%s)\n", (*vit).c_str(), ret == TFS_SUCCESS ? "success" : "failed");
        print_rate(ret, read_time_consumed.time_consumed_);
      }
      ++read_time_consumed.total_count_;
      vit++;
    }

    for (i = 0; i < PER_WRITE; i++)
    {
      write_file(param, tfsclient, NULL, write_file_list, write_time_consumed, write_stater, data);
    }

    for (i = 0; i < PER_UPDATE; i++)
    {
      if (vit == read_file_list.end())
      {
        printf("Update File. read file_list reach end. \n");
        vit = read_file_list.begin();
      }

      int ret = write_file(param, tfsclient, (char*) (*vit).c_str(), write_file_list, update_time_consumed,
          update_stater, data);
      if (0 == ret)
      {
        update_list.push_back((*vit).c_str());
      }
      vit++;
    }

    for (i = 0; i < PER_DELETE; i++)
    {
      if (vit == read_file_list.end())
      {
        printf("Detele File. read file_list reach end. \n");
        vit = read_file_list.begin();
      }

      timer.start();
      int64_t file_size;
      int ret = tfsclient->unlink(file_size, (char*) (*vit).c_str(), ".jpg");
      if (ret != TFS_SUCCESS)
      {
        printf("thread (%d) unlink file fail (%s)\n", param.index_, (*vit).c_str());
        ++delete_time_consumed.fail_count_;
        vit++;
      }
      else
      {
        delete_time_consumed.time_consumed_ = timer.consume();
        delete_stater.stat_time_count(delete_time_consumed.time_consumed_);
        delete_time_consumed.process();

        delete_list.push_back((*vit).c_str());
        bit = read_file_list.erase(vit);
        vit = bit;
      }
      ++delete_time_consumed.total_count_;
    }
    loop_times--;
  }

  int64_t time_stop = Func::curr_time();
  uint64_t time_consumed = time_stop - time_start;

  uint32_t total_succ_count = read_time_consumed.succ_count() + write_time_consumed.succ_count()
      + delete_time_consumed.succ_count() + update_time_consumed.succ_count();
  uint32_t total_fail_count = read_time_consumed.fail_count_ + write_time_consumed.fail_count_
      + delete_time_consumed.fail_count_ + update_time_consumed.fail_count_;

  ((ThreadParam*) arg)->file_count_ = total_succ_count + total_fail_count;
  ((ThreadParam*) arg)->succ_count_ = total_succ_count;
  ((ThreadParam*) arg)->succ_time_consumed_ = read_time_consumed.accumlate_time_consumed_
      + write_time_consumed.accumlate_time_consumed_ + delete_time_consumed.accumlate_time_consumed_
      + update_time_consumed.accumlate_time_consumed_;
  ((ThreadParam*) arg)->fail_time_consumed_ = time_consumed - ((ThreadParam*) arg)->succ_time_consumed_;
  ((ThreadParam*) arg)->file_size_ = param.file_size_;

  double iops = static_cast<double> (total_succ_count) / (static_cast<double> (time_consumed) / 1000000);
  double rate = static_cast<double> (time_consumed) / total_succ_count;
  double aiops = static_cast<double> (total_succ_count)
      / (static_cast<double> (((ThreadParam*) arg)->succ_time_consumed_) / 1000000);
  //double arate = static_cast<double> (((ThreadParam*) arg)->succ_time_consumed_) / total_succ_count;

  uint32_t oper_total_count = loop_times_bak * (PER_READ + PER_WRITE + PER_UPDATE + PER_DELETE);
  printf("\nINDEX | COUNT  | SUCC   | FAIL   | IOPS   | RATE   | AIOPS  | ARATE\n");
  printf("%-5d | %-6d | %-6d | %-6d | %-6f | %-6f | %-6f | %-6f\n\n", param.index_, oper_total_count, total_succ_count,
      total_fail_count, iops, rate, aiops, aiops);
  printf("------ | --------- | --------- | --------- | --------- | ---------\n");
  printf("TYPE   | SUCCCOUNT | FAILCOUNT | AVG       | MIN       | MAX      \n");
  printf("------ | --------- | --------- | --------- | --------- | ---------\n");
  read_time_consumed.display();
  write_time_consumed.display();
  update_time_consumed.display();
  delete_time_consumed.display();
  printf("------ | --------- | --------- | --------- | --------- | ---------\n\n\n");
  read_stater.dump_time_stat();
  write_stater.dump_time_stat();
  update_stater.dump_time_stat();
  delete_stater.dump_time_stat();

  for (vector<std::string>::iterator vit = write_file_list.begin(); vit != write_file_list.end(); vit++)
  {
    read_file_list.push_back((*vit));
  }
  int ret = serialize("w", read_file_list, param.index_, O_CREAT | O_RDWR | O_TRUNC);
  if (ret)
  {
    return NULL;
  }
  ret = serialize("u", update_list, param.index_, O_CREAT | O_RDWR | O_APPEND);
  if (ret)
  {
    return NULL;
  }

  ret = serialize("d", delete_list, param.index_, O_CREAT | O_RDWR | O_APPEND);
  if (ret)
  {
    return NULL;
  }

  return NULL;
}

int main(int argc, char* argv[])
{
  int32_t thread_count = 1;
  ThreadParam input_param;
  int ret = fetch_input_opt(argc, argv, input_param, thread_count);
  if (ret != TFS_SUCCESS || input_param.ns_ip_port_.empty() || thread_count > THREAD_SIZE)
  {
    printf("usage: -d ip:port -t thread_count -o ratio(read:write:delete:update) -s random -r range\n");
    exit(-1);
  }

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
    param[i].profile_ = input_param.profile_;
    param[i].random_ = input_param.random_;
    param[i].oper_ratio_ = input_param.oper_ratio_;
    param[i].min_size_ = input_param.min_size_;
    param[i].max_size_ = input_param.max_size_;
    param[i].locker_ = &glocker;
    param[i].ns_ip_port_ = input_param.ns_ip_port_;
    threads[i].start(mix_worker, (void*) &param[i]);
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
  double asize = total_count > 0 ? total_file_size / total_count : 0;
  printf("thread_num count filesize     iops    siops     fiops     srate       frate\n");
  printf("%10d  %4d %5.2f  %8.3f %8.3f %8.3f %10.3f  %10.3f\n", thread_count, total_count, asize, iops, siops, fiops,
      srate, frate);

}
