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
#ifndef TFS_CLIENT_GCFILE_H_
#define TFS_CLIENT_GCFILE_H_

#include "common/internal.h"
#include "common/file_op.h"
#include "segment_container.h"

namespace tfs
{
  namespace client
  {
    const int32_t GC_BATCH_WIRTE_COUNT = 10;
    extern const char* GC_FILE_PATH;
    const mode_t GC_FILE_PATH_MODE = 0777;

    class GcFile : public SegmentContainer< std::vector<common::SegmentInfo> >
    {
    public:
      explicit GcFile(const bool need_save_seg_infos = true);
      virtual ~GcFile();

      virtual int load();
      virtual int add_segment(const common::SegmentInfo& seg_info);
      virtual int validate(const int64_t total_size = 0);
      virtual int save();

      int initialize(const char* name);

    private:
      DISALLOW_COPY_AND_ASSIGN(GcFile);
      int save_gc();
      int load_head();

    private:
      bool need_save_seg_infos_;
      int file_pos_;
    };
  }
}

#endif
