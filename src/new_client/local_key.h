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
#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include <tbsys.h>
#include <Memory.hpp>
#include "common/file_op.h"
#include "common/internal.h"
#include "segment_container.h"
#include "gc_file.h"

namespace tfs
{
  namespace client
  {
    const static int32_t PRI_DS_NOT_INIT = -1;
    const static int32_t PRI_DS_TRY_ALL_OVER = -2;

    enum TfsFileEofFlag
    {
      TFS_FILE_EOF_FLAG_NO = 0x00,
      TFS_FILE_EOF_FLAG_YES
    };

    enum SegmentStatus
    {
      SEG_STATUS_NOT_INIT = 0,      // not initialized
      SEG_STATUS_OPEN_OVER,         // all is completed
      SEG_STATUS_CREATE_OVER,       // create file is completed
      SEG_STATUS_BEFORE_CLOSE_OVER, // all before final close is completed
      SEG_STATUS_ALL_OVER           // all is completed
    };

    enum CacheHitStatus
    {
      CACHE_HIT_NONE = 0,           // all cache miss
      CACHE_HIT_LOCAL,              // hit local cache
      CACHE_HIT_REMOTE,             // hit tair cache
    };

    struct SegmentData
    {
      int32_t cache_hit_;
      bool delete_flag_;        // delete flag
      common::SegmentInfo seg_info_;
      char* buf_;                   // buffer start
      int32_t inner_offset_;        // offset of this segment to operate
      common::FileInfo* file_info_;
      union                     // special value in mutex condition
      {
        uint64_t write_file_number_;           // write as file_number
        common::TfsUnlinkType unlink_action_;  // unlink as unlink type
        common::ReadDataOptionFlag read_flag_; // read as read flag
        common::TfsStatType stat_mode_;        // stat as stat mode
      };
      common::VUINT64 ds_;
      int32_t pri_ds_index_;
      int32_t status_;
      TfsFileEofFlag eof_;

      SegmentData() : cache_hit_(CACHE_HIT_NONE), delete_flag_(true), buf_(NULL),
                      inner_offset_(0), file_info_(NULL), pri_ds_index_(PRI_DS_NOT_INIT),
                      status_(SEG_STATUS_NOT_INIT), eof_(TFS_FILE_EOF_FLAG_NO)
      {
        write_file_number_ = 0;
      }

      SegmentData(const SegmentData& seg_data)
      {
        cache_hit_ = seg_data.cache_hit_;
        delete_flag_ = false;
        memcpy(&seg_info_, &seg_data.seg_info_, sizeof(seg_info_));
        buf_ = seg_data.buf_;
        inner_offset_ = seg_data.inner_offset_;
        file_info_ = NULL;      // not copy
        write_file_number_ = seg_data.write_file_number_;
        ds_ = seg_data.ds_;
        pri_ds_index_ = seg_data.pri_ds_index_;
        status_ = seg_data.status_;
        eof_ = seg_data.eof_;
      }

      ~SegmentData()
      {
        tbsys::gDelete(file_info_);
      }

      void reset_status()
      {
        status_ = SEG_STATUS_OPEN_OVER;
        set_pri_ds_index();
      }

      int64_t get_read_pri_ds() const
      {
        return ds_[pri_ds_index_];
      }

      int64_t get_write_pri_ds() const
      {
        return ds_[0];
      }

      int32_t get_orig_pri_ds_index() const
      {
        return seg_info_.file_id_ % ds_.size();
      }

      void set_pri_ds_index()
      {
        pri_ds_index_ = seg_info_.file_id_ % ds_.size();
      }

      void set_pri_ds_index(int32_t index)
      {
        pri_ds_index_ = index % ds_.size();
      }

      int64_t get_last_read_pri_ds() const
      {
        int32_t index = 0;
        if (PRI_DS_NOT_INIT != pri_ds_index_)  // read
        {
          // pri_ds_index_ < 0, server is the last retry one
          index = PRI_DS_TRY_ALL_OVER == pri_ds_index_ ? get_orig_pri_ds_index() : pri_ds_index_;
          index = index == 0 ? ds_.size() - 1 : index - 1;
        }
        return ds_[index];
      }
    };

#define SEG_DATA_SELF_FMT                                               \
    ", blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, " \
    "size: %d, crc: %d, inneroffset: %d, filenumber: %"PRI64_PREFIX"u, " \
    "status: %d, rserver: %s, wserver: %s."

#define SEG_DATA_SELF_ARGS(SEG)                                              \
    SEG->seg_info_.block_id_, SEG->seg_info_.file_id_, SEG->seg_info_.offset_, \
      SEG->seg_info_.size_, SEG->seg_info_.crc_, SEG->inner_offset_,    \
      SEG->write_file_number_, SEG->status_,               \
      tbsys::CNetUtil::addrToString(SEG->get_last_read_pri_ds()).c_str(), \
      tbsys::CNetUtil::addrToString(SEG->get_write_pri_ds()).c_str()

#define DUMP_SEGMENTDATA(seg, level, fmt, args...)                      \
    (TBSYS_LOG(level,                                                   \
               fmt SEG_DATA_SELF_FMT, ##args, SEG_DATA_SELF_ARGS(seg)))


    extern const char* LOCAL_KEY_PATH;
    const mode_t LOCAL_KEY_PATH_MODE = 0777;

    typedef std::vector<SegmentData*> SEG_DATA_LIST;
    typedef std::vector<SegmentData*>::iterator SEG_DATA_LIST_ITER;
    typedef std::vector<SegmentData*>::const_iterator SEG_DATA_LIST_CONST_ITER;

    class LocalKey : public SegmentContainer< std::set<common::SegmentInfo> >
    {
    public:
      LocalKey();
      virtual ~LocalKey();

      virtual int load();
      virtual int add_segment(const common::SegmentInfo& seg_info);
      virtual int validate(const int64_t total_size = 0);
      virtual int save();

      int initialize(const char* local_key, const uint64_t addr);
      int load(const char* buf, const int32_t buf_len);
      int load_head(const char* buf, const int32_t buf_len);

      int64_t get_segment_for_write(const int64_t offset, const char* buf,
                                const int64_t size, SEG_DATA_LIST& seg_list);
      int64_t get_segment_for_read(const int64_t offset, char* buf,
                               const int64_t size, SEG_DATA_LIST& seg_list);


    private:
      int init_local_key_name(const char* key, const uint64_t addr, char* local_key_name);
      int load_segment(const char* buf, const int32_t buf_len);

      static void get_segment(const int64_t offset, const char* buf,
                       const int64_t size, SEG_DATA_LIST& seg_list);
      void check_overlap(const int64_t offset, SEG_INFO_CONST_ITER& it);

      void gc_segment(SEG_INFO_CONST_ITER it);
      void gc_segment(SEG_INFO_CONST_ITER first, SEG_INFO_CONST_ITER last);

    private:
      uint64_t server_id_;
      GcFile gc_file_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
