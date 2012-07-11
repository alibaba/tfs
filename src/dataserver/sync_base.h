/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_base.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_SYNCBASE_H_
#define TFS_DATASERVER_SYNCBASE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <queue>
#include <errno.h>
#include <Monitor.h>
#include <Mutex.h>

#include "sync_backup.h"
#include "common/file_queue.h"

namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class SyncBase
    {
      public:
        SyncBase(DataService& service, const int32_t type, const int32_t index, const char* src_addr, const char* dest_addr);
        ~SyncBase();
        int init();
        void stop();

        int write_sync_log(const int32_t cmd, const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id = 0);
        int reset_log();
        int disable_log();
        void set_pause(const int32_t v);
        int run_sync_mirror();

      private:
        SyncBase();
        DISALLOW_COPY_AND_ASSIGN(SyncBase);

        DataService& service_;
        int32_t backup_type_;
        std::string mirror_dir_;
        std::string src_addr_;
        std::string dest_addr_;
        bool is_master_;      // master is responsible to sync to the other backup clusters
        bool stop_;
        int32_t pause_;
        int32_t need_sync_;
        int32_t need_sleep_;
        tbutil::Monitor<tbutil::Mutex> sync_mirror_monitor_;
        tbutil::Monitor<tbutil::Mutex> retry_wait_monitor_;
#if defined(TFS_GTEST)
      public:
#else
#endif
        common::FileQueue* file_queue_;
        SyncBackup* backup_;

      private:
        int do_sync(const char* data, const int32_t len);
        int recover_second_queue();
    };

  }
}
#endif //TFS_DATASERVER_SYNCBASE_H_
