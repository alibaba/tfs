/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bg_task.cpp 166 2011-03-15 07:35:48Z zongdai@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;

TfsLargeFile::TfsLargeFile() : meta_suffix_(NULL)
{
}

TfsLargeFile::~TfsLargeFile()
{
  tbsys::gDeleteA(meta_suffix_);
}

int TfsLargeFile::open(const char* file_name, const char* suffix, const int flags, ... )
{
  int ret = TFS_SUCCESS;

  if (0 == (flags & T_WRITE))       // not write, load meta first
  {
    if ((ret = open_ex(file_name, suffix, flags | T_READ)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open meta file fail, ret: %d", ret);
    }
    else if ((flags & (T_READ | T_UNLINK)) != 0)
    {
      ret = load_meta(flags);
    }
  }
  else  // write flag
  {
    if (NULL != file_name && file_name[0] != '\0')
    {
      TBSYS_LOG(ERROR, "write large file fail, not support update now. file_name: %s", file_name ? file_name : "NULL");
      ret = TFS_ERROR;
    }
    else
    {
      va_list args;
      va_start(args, flags);
      char* local_key = va_arg(args, char*);
      if (NULL == local_key)
      {
        TBSYS_LOG(ERROR, "open with T_LARGE|T_WRITE flag occur null key");
        ret = TFS_ERROR;
      }
      else if ((ret = local_key_.initialize(local_key, tfs_session_->get_ns_addr())) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize local key fail, ret: %d", ret);
      }
      else if (suffix != NULL)
      {
        int32_t len = strlen(suffix);
        if (len > 0)
        {
          meta_suffix_ = new char[len+1];
          memcpy(meta_suffix_, suffix, len + 1);
        }
      }

      va_end(args);
    }
  }

  if (TFS_SUCCESS == ret)
  {
    flags_ = flags;
    offset_ = 0;
    eof_ = TFS_FILE_EOF_FLAG_NO;
    file_status_ = TFS_FILE_OPEN_YES;
  }

  return ret;
}

int64_t TfsLargeFile::read(void* buf, const int64_t count)
{
  return read_ex(buf, count, offset_);
}

int64_t TfsLargeFile::readv2(void* buf, const int64_t count, TfsFileStat* file_info)
{
  int64_t ret = EXIT_GENERAL_ERROR;
  int64_t offset = offset_;

  if (0 == offset && NULL == file_info)
  {
    TBSYS_LOG(ERROR, "null tfsfilestat");
  }
  else
  {
    // read segment not readv2
    ret = read_ex(buf, count, offset_);

    if (0 == offset && ret >= 0)
    {
      if (meta_seg_->file_info_ != NULL)
      {
        wrap_file_info(file_info, meta_seg_->file_info_);
      }
      // stat to get file info
      else if ((ret = fstat(file_info, NORMAL_STAT)) != TFS_SUCCESS)
      {
        ret = EXIT_GENERAL_ERROR;
      }
    }
  }

  return ret;
}

int64_t TfsLargeFile::write(const void* buf, const int64_t count)
{
  return write_ex(buf, count, offset_);
}

int64_t TfsLargeFile::lseek(const int64_t offset, const int whence)
{
  return lseek_ex(offset, whence);
}

int64_t TfsLargeFile::pread(void* buf, const int64_t count, const int64_t offset)
{
  return pread_ex(buf, count, offset);
}

int64_t TfsLargeFile::pwrite(const void* buf, const int64_t count, const int64_t offset)
{
  return pwrite_ex(buf, count, offset);
}

int TfsLargeFile::fstat(TfsFileStat* file_info, const TfsStatType mode)
{
  FileInfo inner_file_info;
  // first stat meta
  int ret = fstat_ex(&inner_file_info, mode);

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "stat meta file fail. ret: %d", ret);
  }
  else if (0 == inner_file_info.flag_)
  {
    // load meta
    // read segmenthead, get meta info
    if ((ret = load_meta_head()) == TFS_SUCCESS)
    {
      wrap_file_info(file_info, &inner_file_info);
    }
    else
    {
      TBSYS_LOG(ERROR, "load meta head fail. ret: %d", ret);
    }
  }
  else // file is delete or conceal
  {
    // TODO:
    // 1. unhide/undelete meta file
    // 2. read meta file head
    // 3. hide/delete meta file
    file_info->file_id_ = inner_file_info.id_;
    file_info->offset_ = static_cast<int32_t>(INVALID_FILE_SIZE);
    file_info->size_ = INVALID_FILE_SIZE;
    file_info->usize_ = INVALID_FILE_SIZE;
    file_info->modify_time_ = static_cast<time_t>(INVALID_FILE_SIZE);
    file_info->create_time_ = static_cast<time_t>(INVALID_FILE_SIZE);
    file_info->flag_ = inner_file_info.flag_;
    file_info->crc_ = static_cast<uint32_t>(INVALID_FILE_SIZE);
  }

  return ret;
}

int TfsLargeFile::close()
{
  return close_ex();
}

int64_t TfsLargeFile::get_file_length()
{
  return local_key_.get_file_size();
}

int TfsLargeFile::unlink(const char* file_name, const char* suffix, int64_t& file_size, const TfsUnlinkType action)
{
  int ret = TFS_SUCCESS;

  // only DELETE action support now
  if (UNDELETE == action)
  {
    TBSYS_LOG(ERROR, "UNDELETE action not support for large file now. action: %d", action);
    ret = TFS_ERROR;
  }
  else if (DELETE != action)    // CONCEAL || REVEAL
  {
    if ((ret = open_ex(file_name, suffix, T_UNLINK)) != TFS_SUCCESS) // just do action over meta file
    {
      TBSYS_LOG(ERROR, "unlink open meta file fail, action: %d, ret: %d", action, ret);
    }
    else
    {
      meta_seg_->unlink_action_ = action;
      get_meta_segment(0, NULL, 0);

      if ((ret = process(FILE_PHASE_UNLINK_FILE)) != TFS_SUCCESS) // do action over meta file
      {
        TBSYS_LOG(ERROR, "unlink file fail, action: %d, ret: %d", action, ret);
      }
    }
  }
  else if ((ret = open(file_name, suffix, T_UNLINK)) != TFS_SUCCESS) // DELETE
  {
    TBSYS_LOG(ERROR, "unlink to read meta file fail. action: %d, ret: %d", action, ret);
  }
  else
  {
    meta_seg_->unlink_action_ = action;
    get_meta_segment(0, NULL, 0);

    // unlink meta file first
    if ((ret = unlink_process()) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "large file unlink meta fail, action: %d, ret: %d", action, ret);
    }
    else
    {
      // seg_info != NULL
      file_size = meta_seg_->seg_info_.size_ + local_key_.get_file_size();

      LocalKey::SEG_INFO_LIST& seg_list = local_key_.get_seg_info();
      LocalKey::SEG_INFO_CONST_ITER sit = seg_list.begin();
      for ( ; sit != seg_list.end(); ++sit)
      {
        destroy_seg();
        SegmentData* seg_data = new SegmentData();
        seg_data->seg_info_ = *sit;
        seg_data->unlink_action_ = action;
        processing_seg_list_.push_back(seg_data);
        unlink_process();
      }
    }
  }

  return ret;
}

int64_t TfsLargeFile::get_segment_for_read(const int64_t offset, char* buf, const int64_t count)
{
  int ret = 0;
  destroy_seg();
#ifdef TFS_TEST
  UNUSED(offset);
  UNUSED(buf);
  std::map<uint32_t, VUINT64>::iterator iter = tfs_session_->block_ds_map_.begin();
  for (int i = 0; tfs_session_->block_ds_map_.end() != iter && i < 3 ; i++, iter++)
  {
    SegmentData* seg_data = new SegmentData();
    seg_data->seg_info_.block_id_ = iter->first;
    seg_data->seg_info_.file_id_ = 1 + rand() % 32768;
    processing_seg_list_.push_back(seg_data);
  }
  ret = count;
#else
  ret = local_key_.get_segment_for_read(offset, buf, count, processing_seg_list_);
#endif
  return ret;
}

int64_t TfsLargeFile::get_segment_for_write(const int64_t offset, const char* buf, const int64_t count)
{
  destroy_seg();
  return local_key_.get_segment_for_write(offset, buf, count, processing_seg_list_);
}

int TfsLargeFile::read_process(int64_t& read_size, const InnerFilePhase read_file_phase)
{
  int ret = get_block_info(processing_seg_list_, flags_);
  if (TFS_SUCCESS == ret)
  {
    ret = read_process_ex(read_size, read_file_phase);
  }
  else
  {
    TBSYS_LOG(ERROR, "get block info fail, ret: %d", ret);
  }
  return ret;
}

int TfsLargeFile::write_process()
{
  int ret = TFS_SUCCESS;
  // get block info fail, must exit.
  if ((ret = get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "batch get block info error, count: %zd, flag: %d", processing_seg_list_.size(), flags_);
  }
  else
  {
    // create file
    if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_CREATE_FILE)))
    {
      TBSYS_LOG(ERROR, "create file name fail, ret: %d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "create file success, ret: %d", ret);
      // write data
      if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_WRITE_DATA)))
      {
        TBSYS_LOG(ERROR, "write data fail, ret: %d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "write data success, ret: %d", ret);
        // close file
        if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_CLOSE_FILE)))
        {
          TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "close file success, ret: %d", ret);
        }
      }
    }
  }

  return ret;
}

int32_t TfsLargeFile::finish_write_process(const int status)
{
  int32_t count = 0;
  int ret = TFS_ERROR;
  SEG_DATA_LIST_ITER it = processing_seg_list_.begin();

  // just for unnecessary iteration and vector erase operation, cope with different status.
  // a little duplicated code

  if (TFS_SUCCESS == status)
  {
    count = processing_seg_list_.size();
    for (; it != processing_seg_list_.end(); it++)
    {
      if ((ret = local_key_.add_segment((*it)->seg_info_)) != TFS_SUCCESS) // should never happen
      {
        break;
      }
      else if ((*it)->delete_flag_)
      {
        tbsys::gDelete(*it);
      }
    }
    processing_seg_list_.clear();
  }
  else
  {
    while (it != processing_seg_list_.end())
    {
      if (SEG_STATUS_ALL_OVER == (*it)->status_) // all over
      {
        if ((ret = local_key_.add_segment((*it)->seg_info_)) != TFS_SUCCESS)
        {
          break;
        }
        else
        {
          if ((*it)->delete_flag_)
          {
            tbsys::gDelete(*it);
          }
          it = processing_seg_list_.erase(it);
          count++;
        }
      }
      else
      {
        // reset crc here
        (*it)->seg_info_.crc_ = 0;
        // Todo: set file_id_ to zero depend on update flag
        (*it)->seg_info_.file_id_= 0;
        it++;
      }
    }
  }

  // maybe move from here
  if (TFS_SUCCESS == ret && count > 0)
  {
    ret = local_key_.save();
  }

  return (ret != TFS_SUCCESS) ? 0 : count;
}

int TfsLargeFile::close_process()
{
  int ret = TFS_ERROR;
  if (offset_ <= 0)
  {
    TBSYS_LOG(ERROR, "close tfs file fail: no data write ever");
    local_key_.remove();        // ignore
  }
  else if ((ret = upload_key()) != TFS_SUCCESS) // upload key data
  {
    TBSYS_LOG(ERROR, "close tfs file fail: upload key fail, ret: %d", ret);
  }
  return ret;
}

int TfsLargeFile::unlink_process()
{
  int ret = TFS_SUCCESS;
  if (1 != processing_seg_list_.size())
  {
    TBSYS_LOG(ERROR, "unlink file fail, processing seg list illegal size: %zd", processing_seg_list_.size());
    ret = TFS_ERROR;
  }
  else
  {
    if ((ret = get_block_info(*processing_seg_list_[0], T_UNLINK)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "unlink get block info fail, ret: %d", ret);
    }
    else if ((ret = process(FILE_PHASE_UNLINK_FILE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "unlink file fail, ret: %d", ret);
    }
  }
  return ret;
}

int TfsLargeFile::wrap_file_info(TfsFileStat* file_stat, FileInfo* file_info)
{
  if (file_stat != NULL && file_info != NULL)
  {
    file_stat->file_id_ = file_info->id_;
    file_stat->offset_ = file_info->offset_;
    file_stat->size_ = local_key_.get_file_size();
    // usize = real_size + meta_size
    file_stat->usize_ = file_info->usize_ + local_key_.get_file_size();
    file_stat->modify_time_ = file_info->modify_time_;
    file_stat->create_time_ = file_info->create_time_;
    file_stat->flag_ = file_info->flag_;
    file_stat->crc_ = file_info->crc_;
  }
  return TFS_SUCCESS;
}

int TfsLargeFile::upload_key()
{
  int ret = TFS_SUCCESS;
  if ((ret = local_key_.validate(offset_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "local key validate fail, ret: %d", ret);
  }
  else
  {
    int32_t size = local_key_.get_data_size();
    char* buf = new char[size];
    local_key_.dump(buf, size);
    // TODO .. open large with mainname
    if ((ret = open_ex(NULL, meta_suffix_, T_WRITE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "upload key fail, open file fail, ret: %d", ret);
    }
    else
    {
      get_meta_segment(0, buf, size, false);

      // TODO: retry need?
      if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, write data fail, ret: %d", ret);
      }
      else if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, close file fail, ret: %d", ret);
      }
    }

    if (TFS_SUCCESS == ret)
    {
      // remove fail, then data can be gc
      if ((ret = local_key_.remove()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, remove local key fail, ret: %d", ret);
      }
    }
    tbsys::gDeleteA(buf);
  }
  return ret;
}

int TfsLargeFile::load_meta(int32_t flags)
{
  int ret = TFS_SUCCESS;

  if (flags & T_UNLINK)         // unlink mode, set force flag to read hidden file
  {
    meta_seg_->read_flag_ = READ_DATA_OPTION_FLAG_FORCE;
  }

  int64_t size = MAX_META_SIZE;
  int64_t read_size = 0;
  char* seg_buf = new char[size];

  get_meta_segment(0, seg_buf, size, false);

  // read all meta file once at best
  // should succeed in nearly all cases
  int retry_count = ClientConfig::client_retry_count_;
  do {
    ret = read_process_ex(read_size, FILE_PHASE_READ_FILE_V2);
    if (TFS_SUCCESS != ret)
    {
      get_block_info(*meta_seg_, flags_);
      meta_seg_->reset_status();
    }
  } while (TFS_SUCCESS != ret && ClientConfig::client_retry_flag_ && --retry_count);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "read meta file fail, ret: %d", ret);
  }
#ifndef TFS_TEST
  else if (meta_seg_->file_info_->size_ > size) // meta file is large than MAX_META_SIZE
  {
    TBSYS_LOG(WARN, "load meta file size large than MAX_META_SIZE: %d > %"PRI64_PREFIX"d",
              meta_seg_->file_info_->size_, size);

    int64_t remain_size = meta_seg_->file_info_->size_ - size;
    char* extra_seg_buf = new char[meta_seg_->file_info_->size_];

    meta_seg_->reset_status();
    get_meta_segment(size, extra_seg_buf + size, remain_size, false);

    // reread to get remain meta file
    if ((ret = read_process_ex(read_size, FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "reread meta file fail. remain size: %"PRI64_PREFIX"d, ret: %d",
                remain_size, ret);
      tbsys::gDelete(extra_seg_buf);
    }
    else
    {
      memcpy(extra_seg_buf, seg_buf, size);

      tbsys::gDeleteA(seg_buf);
      seg_buf = extra_seg_buf;
    }
  }

  if (TFS_SUCCESS == ret)
  {
    if ((ret = local_key_.load(seg_buf, meta_seg_->file_info_->size_)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "construct meta file info fail, ret: %d", ret);
    }
    else if ((ret = local_key_.validate()) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "local key validate fail when read file, ret: %d", ret);
    }
  }
#endif

  tbsys::gDeleteA(seg_buf);

  return ret;
}

int TfsLargeFile::load_meta_head()
{
  int ret = TFS_SUCCESS;

  char seg_buf[sizeof(SegmentHead)];
  int32_t size = sizeof(SegmentHead);
  int64_t ret_size = 0;

  //set meta_seg
  meta_seg_->reset_status();
  get_meta_segment(0, seg_buf, size, false);

  if ((ret = read_process_ex(ret_size, FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read meta head fail, ret: %"PRI64_PREFIX"d", ret_size);
    ret = TFS_ERROR;
  }
  else
  {
    local_key_.load_head(seg_buf, ret_size);
  }

  return ret;
}
