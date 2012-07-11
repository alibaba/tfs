/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_GCWORKER_H_
#define TFS_CLIENT_GCWORKER_H_

#include <Timer.h>
#include "common/error_msg.h"
#include "tfs_client_api.h"
#include "local_key.h"
#include "gc_file.h"
#include "fsname.h"

namespace tfs
{
  namespace client
  {
    enum GcType
    {
      GC_EXPIRED_LOCAL_KEY = 0,
      GC_GARBAGE_FILE
    };

    class GcWorker : public tbutil::TimerTask
    {
    public:
      GcWorker();
      ~GcWorker();

    public:
      virtual void runTimerTask();
      int destroy();

    private:
      int start_gc(const GcType gc_type);
      int get_expired_file(const char* path);
      int check_file(const char* path, const char* file, const time_t now);
      int check_lock(const char* file);
      int do_gc(const GcType gc_type);

      template<class T> int do_gc_ex(T& meta, const char* file_name, const char* addr, int64_t& file_size);
      template<class T> int do_unlink(const T& seg_info, const char* addr, int64_t& file_size);

    private:
      DISALLOW_COPY_AND_ASSIGN(GcWorker);
      bool destroy_;
      LocalKey local_key_;
      GcFile gc_file_;
      std::vector<std::string> file_;
    };
    typedef tbutil::Handle<GcWorker> GcWorkerPtr;

    class GcManager
    {
      public:
        GcManager();
        ~GcManager();

      public:
        int initialize(tbutil::TimerPtr timer, const int64_t schedule_interval_ms);
        int wait_for_shut_down();
        int destroy();
        int reset_schedule_interval(const int64_t schedule_interval_ms);

      private:
        DISALLOW_COPY_AND_ASSIGN(GcManager);
      private:
        bool destroy_;
        tbutil::TimerPtr timer_;
        GcWorkerPtr gc_worker_;
    };

    template<class T> int GcWorker::do_gc_ex(T& meta, const char* file_name, const char* addr, int64_t& file_size)
    {
      int ret = meta.load_file(file_name);

      if (common::EXIT_FILE_BUSY_ERROR == ret)
      {
        TBSYS_LOG(WARN, "file is busy, maybe other gc worker is mastering over it: %s", file_name);
      }
      else
      {
        if (ret != common::TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "load file %s fail, ret: %d", file_name, ret);
        }
        else if ((ret = do_unlink(meta.get_seg_info(), addr, file_size)) != common::TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "do unlink fail, ret: %d", ret);
        }

        // ignore if unlink all file success, just remove ?
        if ((ret = meta.remove()) != common::TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "remove file fail, file: %s, ret: %d", file_name, ret);
        }
      }
      return ret;
    }

    template<class T> int GcWorker::do_unlink(const T& seg_info, const char* addr, int64_t& file_size)
    {
      int ret = common::TFS_SUCCESS;

      for (typename T::const_iterator it = seg_info.begin(); it != seg_info.end(); ++it)
      {
        // just not to depend on inner module, use TfsCliet directely
        // little dummy
        // FSName reuse name, so every varible each loop
        FSName fsname;
        fsname.set_block_id(it->block_id_);
        fsname.set_file_id(it->file_id_);
        if (TfsClient::Instance()->unlink(file_size, fsname.get_name(), NULL, addr) != common::TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "gc segment fail, blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
              it->block_id_, it->file_id_, ret);
          ret = common::TFS_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "gc segment success, blockid: %u, fileid: %"PRI64_PREFIX"u",
              it->block_id_, it->file_id_);
        }
      }
      return ret;
    }

  }
}

#endif
