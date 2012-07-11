/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_queue_thread.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "file_queue_thread.h"
#include "error_msg.h"
#include <tbsys.h>

using namespace tfs;
using namespace common;
using namespace tbutil;

FileQueueThread::FileQueueThread(FileQueue *queue, const void *arg) :
  args_(const_cast<void*> (arg)), file_queue_(queue), queue_thread_param_(NULL), destroy_(DESTROY_FLAG_NO)
{

}

FileQueueThread::~FileQueueThread()
{
  destroy();
  if (queue_thread_param_ != NULL)
  {
    free( queue_thread_param_);
    queue_thread_param_ = NULL;
  }
}

int FileQueueThread::initialize(const int32_t thread_num, deal_func df)
{
  if (file_queue_ == NULL)
  {
    TBSYS_LOG(ERROR, "file_queue_ is null");
    return EXIT_GENERAL_ERROR;
  }
  if (pids_.size() > 0)
  {
    TBSYS_LOG(ERROR, "pid is exists");
    return EXIT_GENERAL_ERROR;
  }
  deal_func_ = df;
  pthread_t pid;
  queue_thread_param_ = (QueueThreadParam*) malloc(thread_num * sizeof(QueueThreadParam));
  for (int i = 0; i < thread_num; i++)
  {
    queue_thread_param_[i].thread_index_ = i;
    queue_thread_param_[i].queue_thread_ = this;
    pthread_create(&pid, NULL, FileQueueThread::thread_func, &queue_thread_param_[i]);
    pids_.push_back(pid);
  }
  return EXIT_SUCCESS;
}

void FileQueueThread::run(int thread_index)
{
  monitor_.lock();
  while (destroy_ == DESTROY_FLAG_NO)
  {
    while (destroy_ == DESTROY_FLAG_NO && file_queue_->empty())
    {
      monitor_.wait();
    }
    if (destroy_ == DESTROY_FLAG_YES)
    {
      break;
    }
    QueueItem *item = file_queue_->pop(thread_index);
    monitor_.unlock();
    if (item != NULL)
    {
      if (deal_func_(&item->data_[0], item->length_, thread_index, args_) == EXIT_SUCCESS)
      {
        file_queue_->finish(thread_index);
      }
      free(item);
    }
    monitor_.lock();
  }
  monitor_.unlock();
}

int FileQueueThread::write(const void* const data, const int32_t len)
{
  if (file_queue_ == NULL)
  {
    TBSYS_LOG(ERROR, "file_queue_ is null");
    return EXIT_GENERAL_ERROR;
  }
  Monitor<Mutex>::Lock lock(monitor_);
  file_queue_->push(data, len);
  monitor_.notifyAll();
  return EXIT_SUCCESS;
}

void *FileQueueThread::thread_func(void *arg)
{
  QueueThreadParam *qtp = static_cast<QueueThreadParam*> (arg);
  FileQueueThread *t = dynamic_cast<FileQueueThread*> (qtp->queue_thread_);
  t->run(qtp->thread_index_);
  return (void*) NULL;
}

void FileQueueThread::wait()
{
  for (uint32_t i = 0; i < pids_.size(); i++)
  {
    pthread_join(pids_[i], NULL);
  }
}

void FileQueueThread::destroy()
{
  Monitor<Mutex>::Lock lock(monitor_);
  monitor_.notifyAll();
  destroy_ = DESTROY_FLAG_YES;
}
