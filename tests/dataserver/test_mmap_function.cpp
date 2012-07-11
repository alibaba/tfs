/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_mmap_function.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include "mmap_file_op.h"
#include "dataserver_define.h"
#include "time.h"
#include "stdlib.h"

using namespace tfs::dataserver;
int main()
{
  int32_t total_num =30000;
  char test_file_name[100];
  char* buf = NULL;
  char* data = NULL;
  int32_t file_size = 0;
  
  struct timeval start;
  struct timeval end;
  long timeuse = 0;

  int32_t random_offset 0;
  int32_t random_file_num 0;
  int32_t read_times = 30000;
  // size for random read 
  int32_t buf_size = 20;

  MMapOption mmap_option;
  MMapFileOperation * mmap_file_operation[total_num];
  FileOperation * file_operation[total_num];

  mmap_option.max_mmap_size_ = 256;
  mmap_option.per_mmap_size_ = 4;
  mmap_option.first_mmap_size_ = 128; 

  srand((unsigned)time(NULL));

  // no mmap write 
  gettimeofday(&start, NULL);
  for (int32_t i = 0; i < total_num; ++i)
  {
    file_size = rand() % mmap_option.first_mmap_size_ + mmap_option.max_mmap_size_ - mmap_option.first_mmap_size_;
    sprintf(test_file_name, "./data/test_mmap_data_%d", i);

    mmap_file_operation[i] = new MMapFileOperation(test_file_name, O_RDWR | O_CREAT);
    mmap_file_operation[i]->open_file();

    buf = new char[file_size];

    int32_t j;
    for (j = 0; j < file_size - 1; ++j)
    {
      buf[j] = 'a' + (j % 6);
    }
    buf[j] = '\0';
    mmap_file_operation[i]->pwrite_file(buf, strlen(buf) + 1, 0);

    delete []buf;
    buf = NULL;
  }
  gettimeofday(&end, NULL);

  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("no mmap write elapsed time: %ld microseconds\n", timeuse);

  // mmap read
  gettimeofday(&start, NULL);
  for (int32_t i = 0; i< total_num; ++i)
  {
    mmap_file_operation[i]->mmap_file(mmap_option);
    file_size = mmap_file_operation[i]->get_file_size();

    data = new char[file_size];
    mmap_file_operation[i]->pread_file(data, file_size, 0);

    delete []data;
    data = NULL;
  }
  gettimeofday(&end, NULL);

  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("mmap read elapsed time: %ld microseconds\n", timeuse);

  // random mmap read
  gettimeofday(&start, NULL);
  int32_t offset_seed = 128 * 1024 - 20;
  data = new char[buf_size];
  for (int32_t i = 0; i< read_times; ++i)
  {
    random_offset = rand() % offset_seed;
    random_file_num = rand() % total_num;

    mmap_file_operation[random_file_num]->pread_file(data, buf_size, random_offset);

  }
  delete []data;
  data = NULL;
  gettimeofday(&end, NULL);

  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("random mmap read elapsed time: %ld microseconds\n", timeuse);

  gettimeofday(&start, NULL);
  for (int32_t i = 0; i < total_num; ++i)
  {
    mmap_file_operation[i]->close_file();
    mmap_file_operation[i]->munmap_file();
    mmap_file_operation[i]->unlink_file();
    delete mmap_file_operation[i];
    mmap_file_operation[i] = NULL;
  }
  gettimeofday(&end, NULL);
  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("unlink elapsed time: %ld microseconds\n", timeuse);

  for (int32_t i = 0; i < total_num; ++i)
  {
    file_size = rand() % mmap_option.first_mmap_size_ + mmap_option.max_mmap_size_ - mmap_option.first_mmap_size_;
    sprintf(test_file_name, "./data/test_mmap_data_%d", i);

    file_operation[i] = new FileOperation(test_file_name, O_RDWR | O_CREAT);
    file_operation[i]->open_file();

    buf = new char[file_size];

    int32_t j;
    for (j = 0; j < file_size - 1; ++j)
    {
      buf[j] = 'a' + (j % 6);
    }
    buf[j] = '\0';
    file_operation[i]->pwrite_file(buf, strlen(buf) + 1, 0);

    delete []buf;
    buf = NULL;
  }
  
  FileOperation * file_operation1[100];
  const int32_t tmp_size = 80 * 1024 * 1000;
  for (int32_t i = 0; i < 100; ++i)
  {
    sprintf(test_file_name, "./data1/test_mmap_data_%d", i);

    file_operation1[i] = new FileOperation(test_file_name, O_RDWR | O_CREAT);
    file_operation1[i]->open_file();

    buf = new char[tmp_size];

    int32_t j;
    for (j = 0; j < tmp_size - 1; ++j)
    {
      buf[j] = 'a' + (j % 6);
    }
    buf[j] = '\0';
    file_operation1[i]->pwrite_file(buf, strlen(buf) + 1, 0);

    delete []buf;
    buf = NULL;
  }

  gettimeofday(&start, NULL);
  for (int32_t i = 0; i< read_times; ++i)
  {
    data = new char[buf_size];
    random_offset = rand() % offset_seed;
    random_file_num = rand() % total_num;

    file_operation[random_file_num]->pread_file(data, buf_size, random_offset); 
    delete []data;
    data = NULL;
  }
  gettimeofday(&end, NULL);

  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("random no  mmap read elapsed time: %ld microseconds\n", timeuse);

  gettimeofday(&start, NULL);
  for (int32_t i = 0; i < total_num; ++i)
  {
    file_operation[i]->close_file();
    file_operation[i]->unlink_file();
    delete mmap_file_operation[i];
    file_operation[i] = NULL;
  }

  gettimeofday(&end, NULL);
  timeuse = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  printf("unlink elapsed time: %ld microseconds\n", timeuse);
  return 0;
}
