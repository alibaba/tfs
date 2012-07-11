/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bg_task.h 520 2011-06-20 06:05:52Z daoan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_BG_TASK_H_
#define TFS_BG_TASK_H_

#include <string>
#include <Timer.h>
#include "common/statistics.h"
#include "gc_worker.h"

namespace tfs
{
  namespace client
  {
    class BgTask
    {
      public:
        static int initialize();
        static int wait_for_shut_down();
        static int destroy();
        static int32_t get_cache_hit_ratio(common::CacheType cache_type = common::LOCAL_CACHE);

        static common::StatManager<std::string, std::string, common::StatEntry>& get_stat_mgr()
        {
          return stat_mgr_;
        }
        static GcManager& get_gc_mgr()
        {
          return gc_mgr_;
        }

      private:
        static tbutil::TimerPtr timer_;
        static common::StatManager<std::string, std::string, common::StatEntry> stat_mgr_;
        static GcManager gc_mgr_;
    };
  }
}

#endif
