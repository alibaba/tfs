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
#include <unistd.h>
#include "common/directory_op.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "client_config.h"
#include "fsname.h"
#include "local_key.h"
#include "tfs_client_api.h"

using namespace tfs::client;
using namespace tfs::common;

const char* tfs::client::LOCAL_KEY_PATH = "/tmp/TFSlocalkeyDIR/";

LocalKey::LocalKey() : server_id_(0)
{
}

LocalKey::~LocalKey()
{
  clear_info();
}

int LocalKey::initialize(const char* local_key, const uint64_t addr)
{
  int ret = TFS_SUCCESS;
  char name[MAX_PATH_LENGTH];

  if (!DirectoryOp::create_full_path(LOCAL_KEY_PATH, false, LOCAL_KEY_PATH_MODE))
  {
    TBSYS_LOG(ERROR, "create localkey path %s with mode %d fail, error: %s",
              LOCAL_KEY_PATH, LOCAL_KEY_PATH_MODE, strerror(errno));
    ret = TFS_ERROR;
  }
  else if ((ret = init_local_key_name(local_key, addr, name)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "init local key name fail, ret: %d", ret);
  }
  else
  {
    clear();
    if (0 != access(name, F_OK)) //not exist
    {
      TBSYS_LOG(DEBUG, "create new localkey file: %s", name);
      ret = init_file_op(name, O_RDWR|O_CREAT);
    }
    else if ((ret = init_file_op(name, O_RDWR)) == TFS_SUCCESS)
    {
      ret = load();
      // load fail. localkey is invalid, just delete
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "load local key fail, file is invalid, ignore: %s", name);
        clear();
        ret = TFS_SUCCESS;
      }
    }

    if (TFS_SUCCESS == ret)
    {
      server_id_ = addr;
      // initialize gc file
      ret = gc_file_.initialize(name + strlen(LOCAL_KEY_PATH));
    }
  }

  return ret;
}

int LocalKey::load()
{
  int ret = TFS_SUCCESS;
  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "local key file path not initialize");
    ret = TFS_ERROR;
  }
  else if ((ret = file_op_->pread_file(reinterpret_cast<char*>(&seg_head_), sizeof(SegmentHead), 0)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "load segment head fail, %s, ret: %d", file_op_->get_file_name(), ret);
  }
  else
  {
    TBSYS_LOG(INFO, "load segment count %d, size: %"PRI64_PREFIX"d", seg_head_.count_, seg_head_.size_);

    char* buf = new char[sizeof(SegmentInfo) * seg_head_.count_];
    if ((ret = file_op_->pread_file(buf, sizeof(SegmentInfo) * seg_head_.count_, sizeof(SegmentHead))) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load segment info fail, %s, ret: %d", file_op_->get_file_name(), ret);
    }
    else
    {
      ret = load_segment(buf, sizeof(SegmentInfo) * seg_head_.count_);
    }
    tbsys::gDeleteA(buf);
  }
  return ret;
}

int LocalKey::load(const char* buf, const int32_t buf_len)
{
  int ret = load_head(buf, buf_len);
  if (TFS_SUCCESS == ret)
  {
    ret = load_segment(buf + sizeof(SegmentHead), buf_len - sizeof(SegmentHead));
  }
  return ret;
}

int LocalKey::add_segment(const SegmentInfo& seg_info)
{
  int ret = seg_info_.insert(seg_info).second ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    seg_head_.count_++;
    seg_head_.size_ += seg_info.size_;
    TBSYS_LOG(DEBUG, "add segment successful. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, crc: %u",
              seg_info.block_id_, seg_info.file_id_, seg_info.offset_, seg_info.size_, seg_info.crc_);
  }
  else
  {
    // add to gc ?
    TBSYS_LOG(ERROR, "add segment fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, crc: %u",
              seg_info.block_id_, seg_info.file_id_, seg_info.offset_, seg_info.size_, seg_info.crc_);
  }
  return ret;
}

// check segment info sequencial and completed from offset 0 to offset
// default validate to end
int LocalKey::validate(const int64_t total_size)
{
  int ret = TFS_SUCCESS;

  // specifid validate size
  if (total_size != 0 && seg_head_.size_ < total_size)
  {
    TBSYS_LOG(ERROR, "segment containing size less than required size: %"PRI64_PREFIX"d < %"PRI64_PREFIX"d",
              seg_head_.size_, total_size);
    ret = TFS_ERROR;
  }
  else if (static_cast<size_t>(seg_head_.count_) != seg_info_.size())
  {
    TBSYS_LOG(ERROR, "segment count conflict with head meta info count: %d <> %zd",
              seg_head_.count_, seg_info_.size());
    ret = TFS_ERROR;
  }
  else if (seg_info_.size() > 0)   // not empty
  {
    int64_t size = 0, check_size = total_size != 0 ? total_size : seg_head_.size_;
    SEG_INFO_CONST_ITER it = seg_info_.begin();
    if (it->offset_ != 0)
    {
      TBSYS_LOG(ERROR, "segment info offset not start with 0: %"PRI64_PREFIX"d", it->offset_);
      ret = TFS_ERROR;
    }
    else
    {
      SEG_INFO_CONST_ITER nit = it;
      nit++;
      size += it->size_;

      for (; nit != seg_info_.end() && size < check_size; ++it, ++nit)
      {
        if (it->offset_ + it->size_ != nit->offset_)
        {
          TBSYS_LOG(ERROR, "segment info conflict: (offset + size != next_offset) %"PRI64_PREFIX"d + %d != %"PRI64_PREFIX"d", it->offset_, it->size_, nit->offset_);
          ret = TFS_ERROR;
          break;
        }
        size += nit->size_;
      }

      if (TFS_SUCCESS == ret)
      {
        // should have no overlap segment
        if (size != check_size)
        {
          TBSYS_LOG(ERROR, "segment info conflict: segment size conflict required size: %"PRI64_PREFIX"d, < %"PRI64_PREFIX"d", size, check_size);
          ret = TFS_ERROR;
        }
        else if (total_size != 0 && nit != seg_info_.end()) // not all segment used
        {
          gc_segment(nit, seg_info_.end());
        }
      }
    }
  }
  return ret;
}

int LocalKey::save()
{
  int ret = TFS_SUCCESS;

  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "local save file path not initialize");
    ret = TFS_ERROR;
  }
  else
  {
    int32_t size = get_data_size();
    char* buf = new char[size];
    dump(buf, size);

    if ((ret = file_op_->pwrite_file(buf, size, 0)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "save segment info fail, count: %zd, raw size: %d, file size: %"PRI64_PREFIX"d, ret: %d",
                seg_info_.size(), size, seg_head_.size_, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "save segment info successful, count: %zd, raw size: %d, file size: %"PRI64_PREFIX"d, ret: %d",
                seg_info_.size(), size, seg_head_.size_, ret);
      file_op_->flush_file();

      if ((ret = gc_file_.save()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "save gc file fail, ret: %d", ret);
      }
    }

    tbsys::gDeleteA(buf);
  }
  return ret;
}

int64_t LocalKey::get_segment_for_write(const int64_t offset, const char* buf,
                                        const int64_t size, SEG_DATA_LIST& seg_list)
{
  int64_t cur_offset = offset, remain_size = size, written_size = 0,
    need_write_size = 0, remain_nw_size = 0, // remain_need_write_size
    total_size = 0;
  int32_t tmp_crc = 0;
  TfsFileStat file_stat;
  const char* cur_buf = buf;
  SegmentInfo seg_info;
  SEG_INFO_CONST_ITER it, first_it;

  while (static_cast<int32_t>(seg_list.size()) < ClientConfig::batch_count_ &&
         remain_size > 0)
  {
    written_size = need_write_size = 0;
    // current remain max need write size
    remain_nw_size = (ClientConfig::batch_count_ - seg_list.size()) * ClientConfig::segment_size_;
    if (remain_nw_size > remain_size)
    {
      remain_nw_size = remain_size;
    }

    seg_info.offset_ = cur_offset;
    // cur_offset ~ end() or cur_offset ~ it->offset_ hasn't been written
    first_it = it = seg_info_.lower_bound(seg_info);

    if (seg_info_.end() == first_it)
    {
      need_write_size = remain_nw_size;
      check_overlap(cur_offset, first_it);
    }
    else
    {
      if (it->offset_ != cur_offset)
      {
        need_write_size = it->offset_ - cur_offset;
        check_overlap(cur_offset, first_it);
      }

      if (need_write_size > remain_nw_size)
      {
        need_write_size = remain_nw_size;
      }
      else
      {
        remain_nw_size -= need_write_size;
        // combine adjacent conflicted segments
        // until reach the segment that can be reused.
        while (remain_nw_size > 0 && it != seg_info_.end())
        {
          tmp_crc = 0;
          if (remain_nw_size < it->size_)
          {
            TBSYS_LOG(INFO, "segment size conflict: %d <> %"PRI64_PREFIX"d", it->size_, remain_nw_size);
            need_write_size += remain_nw_size;
            remain_nw_size = 0;
          }
          else if ((tmp_crc = Func::crc(0, cur_buf + need_write_size, it->size_)) != it->crc_)
          {
            TBSYS_LOG(INFO, "segment crc conflict: %d <> %d", it->crc_, tmp_crc);
            need_write_size += it->size_;
            remain_nw_size -= it->size_;
          }
          else if (TfsClient::Instance()->stat_file(&file_stat, FSName(it->block_id_, it->file_id_).get_name(),
                                                    NULL, NORMAL_STAT, tbsys::CNetUtil::addrToString(server_id_).c_str())
                   != TFS_SUCCESS) // server id util
          {
            TBSYS_LOG(INFO, "segment info is not valid. blockid: %u, fileid: %"PRI64_PREFIX"u",
                      it->block_id_, it->file_id_);
            need_write_size += it->size_;
            remain_nw_size -= it->size_;
          }
          else // full segment crc is correct, use it
          {
            TBSYS_LOG(INFO, "segment data written: offset: %"PRI64_PREFIX"d, blockid: %u, fileid: %"PRI64_PREFIX"u, size: %d, crc: %u", it->offset_, it->block_id_, it->file_id_, it->size_, it->crc_);
            written_size += it->size_;
            remain_nw_size = 0;
            break;
          }
          it++;
        }
        // all ramainning size need write
        if (it == seg_info_.end())
        {
          need_write_size += remain_nw_size;
        }
      }
    }

    get_segment(cur_offset, cur_buf, need_write_size, seg_list);
    total_size = need_write_size + written_size;
    remain_size -= total_size;
    cur_buf += total_size;
    cur_offset += total_size;
    gc_segment(first_it, it);
  }

  return (size - remain_size);
}

int64_t LocalKey::get_segment_for_read(const int64_t offset, char* buf,
                                       const int64_t size, SEG_DATA_LIST& seg_list)
{
  if (offset >= seg_head_.size_)
  {
    TBSYS_LOG(INFO, "read file offset not less than size: %"PRI64_PREFIX"d >= %"PRI64_PREFIX"d", offset, seg_head_.size_);
    return 0;
  }

  // To read, segment info SHOULD and MUST be adjacent and completed
  // but not check here ...

  SegmentData* seg_data = NULL;
  int64_t check_size = 0, cur_size = 0;

  SegmentInfo seg_info;
  seg_info.offset_ = offset;
  SEG_INFO_CONST_ITER it = seg_info_.lower_bound(seg_info);

  if (seg_info_.end() == it || it->offset_ != offset)
  {
    // should NEVER happen: queried offset less than least offset(0) in stored segment info
    if (seg_info_.begin() == it)
    {
      TBSYS_LOG(ERROR, "can not find meta info for offset: %"PRI64_PREFIX"d", offset);
      check_size = EXIT_GENERAL_ERROR;
    }
    else                        // found previous segment middle, get info
    {
      SEG_INFO_CONST_ITER pre_it = it;
      --pre_it;
      // actually SHOULD always occur, cause adjacent and completed read segment info
      if (pre_it->offset_ + pre_it->size_ > offset)
      {
        check_size = pre_it->offset_ + pre_it->size_ - offset;
        if (check_size > size)
        {
          check_size = size;
        }
        seg_data = new SegmentData();
        seg_data->buf_ = const_cast<char*>(buf);
        seg_data->seg_info_ = *pre_it;
        seg_data->seg_info_.size_ = check_size; // real size
        seg_data->inner_offset_ = offset - pre_it->offset_; // real segment offset

        seg_list.push_back(seg_data);
      }
    }
  }

  // get following adjacent segment info
  for (; static_cast<int32_t>(seg_list.size()) < ClientConfig::batch_count_ &&
         it != seg_info_.end() && check_size < size;
       check_size += cur_size, ++it)
  {
    if (check_size + it->size_ > size)
    {
      cur_size = size - check_size;
    }
    else
    {
      cur_size = it->size_;
    }

    seg_data = new SegmentData();
    seg_data->seg_info_ = *it;
    seg_data->seg_info_.size_ = cur_size;
    seg_data->buf_ = const_cast<char*>(buf) + check_size;

    seg_list.push_back(seg_data);
  }

  return check_size;
}

void LocalKey::check_overlap(const int64_t offset, SEG_INFO_CONST_ITER& it)
{
  if (it != seg_info_.begin())
  {
    it--;
    // no overlap, no gc
    if (it->offset_ + it->size_ <= offset)
    {
      it++;
    }
  }
}

void LocalKey::get_segment(const int64_t offset, const char* buf,
                           const int64_t size, SEG_DATA_LIST& seg_list)
{
  if (size > 0)
  {
    int64_t cur_size = 0, check_size = 0;
    SegmentData* seg_data = NULL;

    while (check_size < size)
    {
      if (check_size + ClientConfig::segment_size_ > size)
      {
        cur_size = size - check_size;
      }
      else
      {
        cur_size = ClientConfig::segment_size_;
      }

      seg_data = new SegmentData();
      seg_data->seg_info_.offset_ = offset + check_size;
      seg_data->seg_info_.size_ = cur_size;
      seg_data->buf_ = const_cast<char*>(buf) + check_size;
      seg_list.push_back(seg_data);

      TBSYS_LOG(DEBUG, "get segment, seg info size: %d, offset: %"PRI64_PREFIX"d",
                seg_data->seg_info_.size_, seg_data->seg_info_.offset_);
      check_size += cur_size;
    }
  }
}

int LocalKey::load_head(const char* buf, const int32_t buf_len)
{
  int ret = TFS_SUCCESS;
  if (buf_len < static_cast<int32_t>(sizeof(SegmentHead)))
  {
    TBSYS_LOG(ERROR, "buffer length less than base segment head length: %d < %zd", buf_len, sizeof(SegmentHead));
    ret = TFS_ERROR;
  }
  else
  {
    memcpy(&seg_head_, buf, sizeof(SegmentHead));
    TBSYS_LOG(DEBUG, "load segment head, count %d, size: %"PRI64_PREFIX"d", seg_head_.count_, seg_head_.size_);
  }
  return ret;
}

int LocalKey::load_segment(const char* buf, const int32_t buf_len)
{
  int ret = TFS_SUCCESS;
  int64_t size = 0;
  int32_t count = seg_head_.count_;

  if (buf_len < static_cast<int32_t>(sizeof(SegmentInfo)) * count)
  {
    TBSYS_LOG(ERROR, "buffer length less than required segmentInfo. segment count: %d, %d < %zd",
              count, buf_len, sizeof(SegmentInfo) * count);
    ret = TFS_ERROR;
  }
  else
  {
    // clear last segment info ?
    clear_info();

    const SegmentInfo* segment = reinterpret_cast<const SegmentInfo*>(buf);
    for (int32_t i = 0; i < count; ++i)
    {
      TBSYS_LOG(DEBUG, "load segment info, offset: %"PRI64_PREFIX"d, blockid: %u, fileid: %"PRI64_PREFIX"u, size: %d, crc: %u",
                segment[i].offset_, segment[i].block_id_, segment[i].file_id_, segment[i].size_, segment[i].crc_);

      if (!seg_info_.insert(segment[i]).second)
      {
        TBSYS_LOG(ERROR, "load segment info fail, count: %d, failno: %d", count, i + 1);
        ret = TFS_ERROR;
        break;
      }
      size += segment[i].size_;
    }

    if (TFS_SUCCESS == ret && size != seg_head_.size_)
    {
      TBSYS_LOG(ERROR, "segment size conflict with head meta info size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d",
                size, seg_head_.size_);
      ret = TFS_ERROR;
    }
  }

  return ret;
}

void LocalKey::gc_segment(SEG_INFO_CONST_ITER it)
{
  gc_file_.add_segment(*it);

  // update head info
  seg_head_.count_--;
  seg_head_.size_ -= it->size_;
  seg_info_.erase(it);
}

void LocalKey::gc_segment(SEG_INFO_CONST_ITER first, SEG_INFO_CONST_ITER last)
{
  // not update head info
  if (first != last && first != seg_info_.end())
  {
    int64_t total_size = 0;
    for (SEG_INFO_CONST_ITER it = first; it != last && it != seg_info_.end(); it++)
    {
      gc_file_.add_segment(*it);
      total_size += it->size_;
    }
    seg_info_.erase(first, last);
    seg_head_.size_ -= total_size;
    seg_head_.count_ = seg_info_.size();
  }
}

int LocalKey::init_local_key_name(const char* key, const uint64_t addr, char* local_key_name)
{
  int ret = TFS_SUCCESS;

  if (NULL == key || 0 == addr || NULL == local_key_name)
  {
    TBSYS_LOG(ERROR, "null key or addr occur. key: %p, addr: %"PRI64_PREFIX"u, local key name buffer: %p",
              key, addr, local_key_name);
    ret = TFS_ERROR;
  }
  else
  {
    char path_buffer[PATH_MAX];

    if (NULL == realpath(key, path_buffer))
    {
      TBSYS_LOG(ERROR, "initialize local key %s fail, key is not a valid file path. error: %s", key, strerror(errno));
      ret = TFS_ERROR;
    }
    else
    {
      snprintf(local_key_name, MAX_PATH_LENGTH, "%s%s", LOCAL_KEY_PATH, path_buffer);
      char* tmp_file = local_key_name + strlen(LOCAL_KEY_PATH);
      // convert tmp file name
      char* pos = NULL;
      while (NULL != (pos = strchr(tmp_file, '/')))
      {
        tmp_file = pos;
        *pos = '!';
      }

      int len = strlen(local_key_name);
      snprintf(local_key_name + len, MAX_PATH_LENGTH - len, "!%" PRI64_PREFIX "u", addr);
    }
  }

  return ret;
}
