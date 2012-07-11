/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_console.h 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_TOOL_BLOCKCONSOLE_H_
#define TFS_TOOL_BLOCKCONSOLE_H_
#include <tbnet.h>
#include <Mutex.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <set>

#include "common/internal.h"
#include "common/error_msg.h"
#include "new_client/tfs_session.h"

const int32_t TRANSFER_FINISH = 10000;

struct StatParam
{
  int64_t  total_;
  atomic_t copy_success_;
  atomic_t copy_failure_;

  StatParam() : total_(0)
  {
    atomic_set(&copy_success_, 0);
    atomic_set(&copy_failure_, 0);
  }

  void dump(void)
  {
    TBSYS_LOG(ERROR, "[copy] total: %"PRI64_PREFIX"d, success: %d, fail: %d\n",
        total_, atomic_read(&copy_success_), atomic_read(&copy_failure_));
  }
};

class BlockConsole
{
  public:
    BlockConsole();
    ~BlockConsole();

    int initialize(const std::string& ts_input_blocks_file, const std::string& dest_ds_ip_file);
    int get_transfer_param(uint32_t& blockid, uint64_t& ds_id);
    int finish_transfer_block(const uint32_t blockid, const int32_t transfer_ret);

  private:
    int locate_cur_pos();

  private:
    std::set<uint32_t> input_blockids_;
    std::set<uint32_t> succ_blockids_;
    std::set<uint32_t> fail_blockids_;
    std::set<uint64_t> dest_ds_ids_;
    std::set<uint64_t>::iterator cur_ds_sit_;
    std::set<uint32_t>::iterator cur_sit_;
    FILE* input_file_ptr_;
    FILE* succ_file_ptr_;
    FILE* fail_file_ptr_;
    StatParam stat_param_;
    tbutil::Mutex mutex_;
};

struct FileInfoCmp
{
  bool operator()(const tfs::common::FileInfo& left, const tfs::common::FileInfo& right) const
  {
    return (left.offset_ < right.offset_);
  }
};

typedef std::set<tfs::common::FileInfo, FileInfoCmp> FileInfoSet;
typedef FileInfoSet::iterator FileInfoSetIter;

class TranBlock
{
  public:
    TranBlock(const uint32_t blockid, const std::string& dest_ns_addr,
        const uint64_t dest_ds_addr, const int64_t traffic, tfs::client::TfsSession* src_session, tfs::client::TfsSession* dest_session);
    ~TranBlock();

    int run();
    int get_tran_size() const
    {
      return total_tran_size_;
    }

  private:
    int get_src_ds();
    int read_index();
    int read_data();
    int recombine_data();
    int check_dest_blk();
    int write_data();
    int write_index();
    int check_integrity();
    int rm_block_from_ns(uint64_t ds_id);
    int rm_block_from_ds(uint64_t ds_id);

  private:
    static const int64_t WAIT_TIME_OUT;
    static const int64_t TRAN_BUFFER_SIZE;
    static const int64_t RETRY_TIMES;

  private:
    //uint32_t block_id_;
    std::string dest_ns_addr_;
    uint64_t dest_ds_id_;
    int32_t cur_offset_;
    int64_t total_tran_size_;
    int64_t traffic_;
    tfs::client::TfsSession* src_session_;
    tfs::client::TfsSession* dest_session_;
    tfs::client::SegmentData seg_data_;
    //tfs::common::VUINT64 rds_;
    FileInfoSet file_set_;
    tbnet::DataBuffer src_content_buf_;
    tbnet::DataBuffer dest_content_buf_;
    tfs::common::RawMetaVec dest_raw_meta_;
    tfs::common::BlockInfo dest_block_info_;
    StatParam stat_param_;
    std::set<uint64_t> concealed_files_;
};

class FindFileId
{
  public:
    FindFileId(const uint64_t file_id) : file_id_(file_id)
    {
    }
    bool operator()(const tfs::common::FileInfo& finfo)
    {
      return (file_id_ == finfo.id_);
    }

  private:
    FindFileId();
  public:
    uint64_t file_id_;
};
#endif
