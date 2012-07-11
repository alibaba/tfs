/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_queue_thread.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_FILE_QUEUE_THREAD_H_
#define TFS_COMMON_FILE_QUEUE_THREAD_H_

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <vector>
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>

#include "func.h"
#include "file_queue.h"

namespace tfs
{
  namespace common
  {
    class FileQueueThread
    {
      enum DESTROY_FLAG
      {
        DESTROY_FLAG_YES = 0x00,
        DESTROY_FLAG_NO
      };
      struct QueueThreadParam
      {
        int32_t thread_index_;
        FileQueueThread *queue_thread_;

      private:
        DISALLOW_COPY_AND_ASSIGN(QueueThreadParam);
      };

      typedef int (*deal_func)(const void * const data, const int64_t len, const int32_t thread_index, void* arg);

    public:
      FileQueueThread(FileQueue *queue, const void *arg);
      virtual ~FileQueueThread();
      int initialize(const int32_t thread_num, deal_func df);
      int write(const void* const data, const int32_t len);
      void wait();
      void destroy();
      void run(const int32_t thread_index);
      static void *thread_func(void *arg);

      inline void update_queue_information_header()
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        QueueInformationHeader head = *file_queue_->get_queue_information_header();
        TBSYS_LOG(INFO, "Update QinfoHead(before): readSeqNo: %d, readOffset: %d, writeSeqNo: %d,"
          "writeFileSize: %d", head.read_seqno_, head.read_offset_, head.write_seqno_, head.write_filesize_);
        head.read_seqno_ = head.write_seqno_;
        head.read_offset_ = head.write_filesize_;
        file_queue_->update_queue_information_header(&head);
        TBSYS_LOG(INFO, "Update QinfoHead(after): readSeqNo: %d, readOffset: %d, writeSeqNo: %d,"
          "writeFileSize: %d", head.read_seqno_, head.read_offset_, head.write_seqno_, head.write_filesize_);
      }

    private:
      DISALLOW_COPY_AND_ASSIGN( FileQueueThread);

      void *args_;
      FileQueue *file_queue_;
      QueueThreadParam *queue_thread_param_;
      std::vector<pthread_t> pids_;
      deal_func deal_func_;
      DESTROY_FLAG destroy_;
      tbutil::Monitor<tbutil::Mutex> monitor_;
    };
  }
}

#endif
