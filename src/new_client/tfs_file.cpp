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
#include <map>

#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "tfs_file.h"
#include "bg_task.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

TfsFile::TfsFile() : flags_(-1), file_status_(TFS_FILE_OPEN_NO), eof_(TFS_FILE_EOF_FLAG_NO),
                     offset_(0), meta_seg_(NULL), option_flag_(common::TFS_FILE_DEFAULT_OPTION),
                     tfs_session_(NULL)
{
}

TfsFile::~TfsFile()
{
  destroy_seg();
  tbsys::gDelete(meta_seg_);
}

void TfsFile::destroy_seg()
{
  for (size_t i = 0; i < processing_seg_list_.size(); i++)
  {
    if (processing_seg_list_[i]->delete_flag_)
    {
      tbsys::gDelete(processing_seg_list_[i]);
    }
  }
  processing_seg_list_.clear();
}

int TfsFile::get_block_info(SegmentData& seg_data, const int32_t flags)
{
  int ret = TFS_SUCCESS;
  if ((ret = tfs_session_->get_block_info(seg_data, flags)) == TFS_SUCCESS)
  {
    seg_data.status_ = SEG_STATUS_OPEN_OVER;
    if ((flags & (T_READ | T_STAT)) != 0) // read
    {
      seg_data.set_pri_ds_index();
    }
  }
  return ret;
}

int TfsFile::get_block_info(SEG_DATA_LIST& seg_data_list, const int32_t flags)
{
  return tfs_session_->get_block_info(seg_data_list, flags);
}

int TfsFile::open_ex(const char* file_name, const char* suffix, const int32_t flags)
{
  int ret = TFS_SUCCESS;

  if (NULL == tfs_session_)
  {
    TBSYS_LOG(ERROR, "session is not initialized");
    ret = TFS_ERROR;
  }
  else if (0 == (flags & T_WRITE) && (NULL == file_name || file_name[0] == '\0'))
  {
    TBSYS_LOG(ERROR, "null tfs name for flag: %d", flags);
    ret = TFS_ERROR;
  }
  else
  {
    if (-1 == flags_)
    {
      flags_ = flags;
    }

    tbsys::gDelete(meta_seg_);
    meta_seg_ = new SegmentData();
    meta_seg_->delete_flag_ = false;

    fsname_.set_name(file_name, suffix, tfs_session_->get_cluster_id());

    if (!fsname_.is_valid())
    {
      TBSYS_LOG(ERROR, "invalid tfs file name. tfsname: %s, suffix: %s, flag: %d",
                file_name, suffix, flags);
      ret = TFS_ERROR;
    }
    else
    {
      meta_seg_->seg_info_.block_id_ = fsname_.get_block_id();
      meta_seg_->seg_info_.file_id_ = fsname_.get_file_id();

      if ((ret = get_block_info(*meta_seg_, flags)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "tfs open fail: get block info fail, blockid: %u, fileid: %"
                  PRI64_PREFIX "u, mode: %d, ret: %d", meta_seg_->seg_info_.block_id_, meta_seg_->seg_info_.file_id_, flags, ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "tfs open success: get block info success, blockid: %u, fileid: %"
                  PRI64_PREFIX "u, mode: %d, ret: %d", meta_seg_->seg_info_.block_id_, meta_seg_->seg_info_.file_id_, flags, ret);

        if (flags_ & T_WRITE)
        {
          get_meta_segment(0, NULL, 0);
#ifdef TFS_TEST
          srand(time(NULL) + rand());
          meta_seg_->seg_info_.file_id_ = 1 + (rand() % 32768);
#else
          if ((ret = process(FILE_PHASE_CREATE_FILE)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "create file name fail, ret: %d", ret);
          }
#endif
        }
        if (TFS_SUCCESS == ret)
        {
          fsname_.set_block_id(meta_seg_->seg_info_.block_id_);
          fsname_.set_file_id(meta_seg_->seg_info_.file_id_);
          fsname_.set_suffix(suffix);
          meta_seg_->seg_info_.file_id_ = fsname_.get_file_id();

          offset_ = 0;
          eof_ = TFS_FILE_EOF_FLAG_NO;
          file_status_ = TFS_FILE_OPEN_YES;

        }

        // check if force_read
        if ((flags & T_READ) && (flags_ & T_FORCE))
        {
            meta_seg_->read_flag_ = READ_DATA_OPTION_FLAG_FORCE;
        }
      }
    }
  }

  return ret;
}

int64_t TfsFile::read_ex(void* buf, const int64_t count, const int64_t offset,
                         const bool modify, const InnerFilePhase read_file_phase)
{
  int ret = TFS_SUCCESS;
  int64_t read_size = 0;

  if (file_status_ != TFS_FILE_OPEN_YES)
  {
    TBSYS_LOG(ERROR, "read fail, file status: %d is not open yes.", file_status_);
    ret = EXIT_NOT_OPEN_ERROR;
  }
  else if (modify && TFS_FILE_EOF_FLAG_YES == eof_)
  {
    TBSYS_LOG(DEBUG, "read file reach end");
  }
  else if (flags_ & T_WRITE)
  {
    TBSYS_LOG(ERROR, "read fail, file open without read flag: %d", flags_);
    ret = EXIT_NOT_PERM_OPER;
  }
  else if (NULL == buf || count < 0 || offset < 0)
  {
    TBSYS_LOG(ERROR, "invalid read buffer or count. buffer: %p, count: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d",
              buf, count, offset);
    ret = EXIT_PARAMETER_ERROR;
  }
  else if (count > 0)
  {
    int64_t check_size = 0, cur_size = 0, seg_count = 0, retry_count = 0;

    while (check_size < count)
    {
      if ((cur_size = get_segment_for_read(offset + check_size, reinterpret_cast<char*>(buf) + check_size,
                                           count - check_size)) < 0)
      {
        TBSYS_LOG(ERROR, "get segment for read fail, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d",
                  offset + check_size, count - check_size);
        ret = EXIT_GENERAL_ERROR;
        break;
      }
      else if (0 == cur_size)
      {
        TBSYS_LOG(DEBUG, "file read reach end, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d",
                  offset + check_size, cur_size);
        eof_ = TFS_FILE_EOF_FLAG_YES;
        break;
      }
      else
      {
        seg_count = processing_seg_list_.size();
        retry_count = ClientConfig::client_retry_count_;
        do
        {
          ret = read_process(read_size, read_file_phase);
        } while (ret != TFS_SUCCESS && ClientConfig::client_retry_flag_ && --retry_count);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read data fail, ret: %d, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, segment count: %"PRI64_PREFIX"d",
                    ret, offset + check_size, cur_size, seg_count);
          ret = EXIT_GENERAL_ERROR;
          // read fail, must exit
          break;
        }

        if (TFS_FILE_EOF_FLAG_YES == eof_) // reach end
        {
          TBSYS_LOG(DEBUG, "file read reach end, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d", offset + check_size, cur_size);
          break;
        }
        check_size = read_size;
      }
    }

    if (TFS_SUCCESS == ret && modify)
    {
      offset_ += read_size;
    }
  }

  return (TFS_SUCCESS == ret) ? read_size : ret;
}

int64_t TfsFile::write_ex(const void* buf, const int64_t count, const int64_t offset, const bool modify)
{
  //do not consider the update operation now
  int ret = TFS_SUCCESS;
  int64_t check_size = 0;
  if (file_status_ == TFS_FILE_OPEN_NO) //if file status is TFS_FILE_WRITE_ERROR, continue write
  {
    TBSYS_LOG(ERROR, "write fail: file not open");
    ret = EXIT_NOT_OPEN_ERROR;
  }
  else if (!(flags_ & T_WRITE))
  {
    TBSYS_LOG(ERROR, "write fail: file open without write flag");
    ret = EXIT_NOT_PERM_OPER;
  }
  else if (NULL == buf || count < 0 || offset < 0)
  {
    TBSYS_LOG(ERROR, "invalid write buffer or count. buffer: %p, count: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d",
              buf, count, offset);
    ret = EXIT_PARAMETER_ERROR;
  }
  else if (count > 0)
  {
    int64_t cur_size = 0, seg_count = 0, retry_count = 0;

    while (check_size < count)
    {
      if ((cur_size =
           get_segment_for_write(offset + check_size, reinterpret_cast<const char*>(buf) + check_size,
                                 count - check_size)) < 0)
      {
        TBSYS_LOG(ERROR, "get segment error, ret: %"PRI64_PREFIX"d", cur_size);
        ret = EXIT_GENERAL_ERROR;
        break;
      }

      seg_count = processing_seg_list_.size();
      if (0 == seg_count)
      {
        TBSYS_LOG(INFO, "data already written, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d", offset + check_size, cur_size);
      }
      else
      {
        retry_count = ClientConfig::client_retry_count_;
        do
        {
          ret = write_process();
          finish_write_process(ret);
        } while (ret != TFS_SUCCESS && ClientConfig::client_retry_flag_ && --retry_count);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write fail, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, segment count total: %"PRI64_PREFIX"d, ret: %d",
                    offset + check_size, cur_size, seg_count, ret);
          ret = EXIT_GENERAL_ERROR;
          break;
        }
        else
        {
          TBSYS_LOG(DEBUG, "write success, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, segment count: %"PRI64_PREFIX"d",
                    offset + check_size, cur_size, seg_count);
        }
      }

      check_size += cur_size;
    }

    if (TFS_SUCCESS == ret && modify)
    {
      offset_ += check_size;
    }
  }

  if (TFS_SUCCESS != ret)
  {
    if (EXIT_NOT_OPEN_ERROR != ret && EXIT_NOT_PERM_OPER != ret)
    {
      file_status_ = TFS_FILE_WRITE_ERROR;
    }
  }
  else
  {
    file_status_ = TFS_FILE_OPEN_YES;
  }

  return (TFS_SUCCESS == ret) ? check_size : ret;
}

int64_t TfsFile::lseek_ex(const int64_t offset, const int whence)
{
  int64_t ret = INVALID_FILE_SIZE;
  if (TFS_FILE_OPEN_YES != file_status_)
  {
    TBSYS_LOG(ERROR, "lseek fail, file status: %d is not open yet", file_status_);
    ret = EXIT_NOT_OPEN_ERROR;
  }
  else if ((flags_ & (T_READ|T_WRITE)) == 0)
  {
    TBSYS_LOG(ERROR, "lseek fail, file open without read flag: %d", flags_);
    ret = EXIT_NOT_PERM_OPER;
  }
  else
  {
    switch (whence)
    {
    case T_SEEK_SET:
      if (offset < 0)
      {
        TBSYS_LOG(ERROR, "wrong offset seek_set, %"PRI64_PREFIX"d", offset);
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        offset_ = offset;
        ret = offset_;
      }
      break;
    case T_SEEK_CUR:
      if (offset_ + offset < 0)
      {
        TBSYS_LOG(ERROR, "wrong offset seek_cur, %"PRI64_PREFIX"d", offset);
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        offset_ += offset;
        ret = offset_;
      }
      break;
    default:
      TBSYS_LOG(ERROR, "unknown seek flag: %d", whence);
      break;
    }
  }
  return ret;
}

int64_t TfsFile::pread_ex(void* buf, const int64_t count, const int64_t offset)
{
  return read_ex(buf, count, offset, false);
}

int64_t TfsFile::pwrite_ex(const void* buf, const int64_t count, const int64_t offset)
{
  return write_ex(buf, count, offset, false);
}

int TfsFile::fstat_ex(FileInfo* file_info, const TfsStatType mode)
{
  int ret = TFS_SUCCESS;
  if (TFS_FILE_OPEN_YES != file_status_)
  {
    TBSYS_LOG(ERROR, "stat fail, file status: %d is not open yes", file_status_);
    ret = EXIT_NOT_OPEN_ERROR;
  }
  else if (0 == (flags_ & (T_STAT | T_READ)))
  {
    TBSYS_LOG(ERROR, "stat fail, file open without stat flag: %d", flags_);
    ret = EXIT_NOT_PERM_OPER;
  }
  else if (NULL == file_info)
  {
    TBSYS_LOG(ERROR, "stat fail, output file info null");
    ret = EXIT_GENERAL_ERROR;
  }
  else
  {
    FileInfo* save_file_info = meta_seg_->file_info_;
    meta_seg_->file_info_ = file_info;
    meta_seg_->stat_mode_ = mode;

    meta_seg_->reset_status();
    get_meta_segment(0, NULL, 0);  // just stat

    int retry_count = ClientConfig::client_retry_count_;
    do
    {
      ret = stat_process();
    } while (ret != TFS_SUCCESS && ClientConfig::client_retry_flag_ && --retry_count);

    // recovery backup
    meta_seg_->file_info_ = save_file_info;
  }

  return ret;
}

int TfsFile::close_ex()
{
  int ret = file_status_ == TFS_FILE_OPEN_NO ? EXIT_NOT_OPEN_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS != ret)//file not open
  {
    TBSYS_LOG(INFO, "close tfs file, buf file status is not open");
  }
  else
  {
    if (!(flags_ & T_WRITE))
    {
      //TBSYS_LOG(INFO, "close tfs file successful");
    }
    else// write mode
    {
#ifndef TFS_TEST
      if (TFS_FILE_WRITE_ERROR == file_status_ || offset_ <= 0)
      {
        option_flag_ |= TFS_FILE_CLOSE_FLAG_WRITE_DATA_FAILED;
      }
      ret = close_process();
#endif
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
      }
      if (file_status_ == TFS_FILE_WRITE_ERROR)
      {
        ret = EXIT_NOT_PERM_OPER;
        TBSYS_LOG(WARN, "occur tfs file write error, close. file status: %d", file_status_);
      }
    }
  }

  if (TFS_SUCCESS == ret && 0 != offset_)
  {
    if (flags_ & T_WRITE) // XXX: have been done in open_ex?
    {
      fsname_.set_file_id(meta_seg_->seg_info_.file_id_);
      fsname_.set_block_id(meta_seg_->seg_info_.block_id_);
    }

    file_status_ = TFS_FILE_OPEN_NO;
    offset_ = 0;
  }
  return ret;
}

const char* TfsFile::get_file_name(const bool simple)
{
  if (true == simple)
  {
    fsname_.set_suffix((uint32_t)0);
  }

  if (flags_ & T_LARGE)
  {
    return fsname_.get_name(true);
  }
  return fsname_.get_name();
}

void TfsFile::set_session(TfsSession* tfs_session)
{
  tfs_session_ = tfs_session;
}

void TfsFile::set_option_flag(OptionFlag option_flag)
{
  option_flag_ |= option_flag;
}

int64_t TfsFile::get_meta_segment(const int64_t offset, const char* buf, const int64_t count, const bool force_check)
{
  int64_t ret = count;
  destroy_seg();
  if (NULL == meta_seg_)
  {
    TBSYS_LOG(ERROR, "meta segment null error");
    ret = EXIT_GENERAL_ERROR;
  }
  else if (count > ClientConfig::segment_size_ && force_check) // not force check
  {
    int64_t cur_size = ClientConfig::segment_size_, check_size = 0;
    while (check_size < count)
    {
      if (check_size + ClientConfig::segment_size_ <= count)
      {
        cur_size = ClientConfig::segment_size_;
      }
      else
      {
        cur_size = count - check_size;
      }

      SegmentData* seg_data = new SegmentData(*meta_seg_);
      seg_data->seg_info_.size_ = cur_size;
      seg_data->buf_ = const_cast<char*>(buf) + check_size;
      seg_data->inner_offset_ = offset + check_size; // dummy ?
      processing_seg_list_.push_back(seg_data);

      check_size += cur_size;
    }
  }
  else
  {
    meta_seg_->seg_info_.size_ = count;
    meta_seg_->buf_ = const_cast<char*>(buf);
    meta_seg_->inner_offset_ = offset;
    processing_seg_list_.push_back(meta_seg_);
  }
  return ret;
}

int TfsFile::process(const InnerFilePhase file_phase)
{
  int ret = EXIT_ALL_SEGMENT_ERROR;
  int32_t size = processing_seg_list_.size();
  if (UINT16_MAX < static_cast<uint32_t>(size))
  {
    TBSYS_LOG(ERROR, "cannot process more than %hu process seq, your request %d", UINT16_MAX, size);
  }
  else
  {
    NewClient* client = NULL;
    int32_t req_size = 0;
    client = NewClientManager::get_instance().create_client();
    send_id_index_map_.clear();
    if (NULL != client)
    {
      for (uint16_t i = 0; i < static_cast<uint16_t>(size); ++i)
      {
        if (processing_seg_list_[i]->status_ == phase_status[file_phase].pre_status_)
        {
          if ((ret = do_async_request(file_phase, client, i)) != TFS_SUCCESS)
          {
            // just continue
            TBSYS_LOG(ERROR, "request %hu fail, status: %d, define status: %d",
                      i, processing_seg_list_[i]->status_, phase_status[file_phase].pre_status_);
            if (CACHE_HIT_LOCAL == processing_seg_list_[i]->cache_hit_)
            {
              tfs_session_->remove_local_block_cache(processing_seg_list_[i]->seg_info_.block_id_);
            }
#ifdef WITH_TAIR_CACHE
            else if (CACHE_HIT_REMOTE == processing_seg_list_[i]->cache_hit_)
            {
              tfs_session_->remove_local_block_cache(processing_seg_list_[i]->seg_info_.block_id_);
              tfs_session_->remove_remote_block_cache(processing_seg_list_[i]->seg_info_.block_id_);
            }
#endif
            processing_seg_list_[i]->cache_hit_ = CACHE_HIT_NONE;
          }
          else
          {
            req_size++;
          }
        }
      }

      TBSYS_LOG(DEBUG, "send packet. request size: %d, successful request size: %d",
                size, req_size);
      // all request fail
      if (0 == req_size)
      {
        if (size > 1)
          ret = EXIT_ALL_SEGMENT_ERROR;
      }
      else
      {
        // wait for response
        client->wait(ClientConfig::batch_timeout_);
        // process failed response
        process_fail_response(client);
        // process successed response
        ret = process_success_response(file_phase, client);
      }

      send_id_index_map_.clear();
      NewClientManager::get_instance().destroy_client(client);
    }
  }

  return ret;
}

int TfsFile::process_fail_response(NewClient* client)
{
  NewClient::RESPONSE_MSG_MAP* fail_res_map = client->get_fail_response();

  if (NULL != fail_res_map)
  {
    for (NewClient::RESPONSE_MSG_MAP_ITER mit = fail_res_map->begin();
         mit != fail_res_map->end(); ++mit)
    {
      map<uint8_t, uint16_t>::iterator index_it =
        send_id_index_map_.find(mit->first);
      if (send_id_index_map_.end() == index_it)
      {
        TBSYS_LOG(ERROR, "get invalid index id failed response. client id: %p, index id: %hhu", client, mit->first);
      }
      else
      {
        DUMP_SEGMENTDATA(processing_seg_list_[index_it->second], ERROR, "fail get resp. index id: %hhu", mit->first);
        TBSYS_LOG(ERROR, "fail to get response from server: %s, remove block cache: %u",
                  tbsys::CNetUtil::addrToString(mit->second.first).c_str(),
                  processing_seg_list_[index_it->second]->seg_info_.block_id_);
        if (CACHE_HIT_LOCAL == processing_seg_list_[index_it->second]->cache_hit_)
        {
          tfs_session_->remove_local_block_cache(
            processing_seg_list_[index_it->second]->seg_info_.block_id_);
        }
#ifdef WITH_TAIR_CACHE
        else if (CACHE_HIT_REMOTE == processing_seg_list_[index_it->second]->cache_hit_)
        {
          tfs_session_->remove_local_block_cache(
            processing_seg_list_[index_it->second]->seg_info_.block_id_);
          tfs_session_->remove_remote_block_cache(
            processing_seg_list_[index_it->second]->seg_info_.block_id_);
        }
#endif
        processing_seg_list_[index_it->second]->cache_hit_ = CACHE_HIT_NONE;
      }
    }
  }
  return TFS_SUCCESS;
}

int TfsFile::process_success_response(const InnerFilePhase file_phase, NewClient* client)
{
  int ret = EXIT_ALL_SEGMENT_ERROR;
  NewClient::RESPONSE_MSG_MAP* res_map = client->get_success_response();

  if (NULL == res_map)
  {
    TBSYS_LOG(ERROR, "get respose list fail.");
  }
  else
  {
    int32_t resp_size = res_map->size();
    TBSYS_LOG(DEBUG, "get success response. client id: %p, request size: %zd, get response size: %d",
              client, processing_seg_list_.size(), resp_size);

    if (0 == resp_size)
    {
      TBSYS_LOG(ERROR, "get zero response. client id: %p, phase: %d.", client, file_phase);
      for (map<uint8_t, uint16_t>::iterator index_it = send_id_index_map_.begin();
           index_it != send_id_index_map_.end(); ++index_it)
      {
        DUMP_SEGMENTDATA(processing_seg_list_[index_it->second], ERROR, "no resp. client: %p, index id: %hhu", client, index_it->first);
      }
    }
    else
    {
      for (NewClient::RESPONSE_MSG_MAP_ITER mit = res_map->begin();
           mit != res_map->end(); ++mit)
      {
        map<uint8_t, uint16_t>::iterator index_it =
          send_id_index_map_.find(mit->first);
        if (send_id_index_map_.end() == index_it)
        {
          TBSYS_LOG(ERROR, "get invalid index id response. client id: %p, index id: %hhu", client, mit->first);
          resp_size--;
        }
        else
        {
          ret = do_async_response(file_phase, dynamic_cast<common::BasePacket*>(mit->second.second),
                                  index_it->second);
          if (TFS_SUCCESS != ret)
          {
            // just continue next one
            TBSYS_LOG(ERROR, "response fail, ret: %d, index id: %hhu, seq id: %hhu",
                      ret, index_it->first, index_it->second);
            resp_size--;
          }
        }
      }
    }

    if (processing_seg_list_.size() > 1)
      ret = (0 == resp_size) ? EXIT_ALL_SEGMENT_ERROR :
        (resp_size != static_cast<int32_t>(processing_seg_list_.size())) ? EXIT_GENERAL_ERROR : ret;
  }

  return ret;
}

int TfsFile::do_async_request(const InnerFilePhase file_phase, NewClient* client, const uint16_t index)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
  case FILE_PHASE_CREATE_FILE:
    ret = async_req_create_file(client, index);
    break;
  case FILE_PHASE_READ_FILE:
    ret = async_req_read_file(client, index);
    break;
  case FILE_PHASE_READ_FILE_V2:
    ret = async_req_read_file_v2(client, index);
    break;
  case FILE_PHASE_WRITE_DATA:
    ret = async_req_write_data(client, index);
    break;
  case FILE_PHASE_CLOSE_FILE:
    ret = async_req_close_file(client, index);
    break;
  case FILE_PHASE_STAT_FILE:
    ret = async_req_stat_file(client, index);
    break;
  case FILE_PHASE_UNLINK_FILE:
    ret = async_req_unlink_file(client, index);
    break;
  default:
    TBSYS_LOG(ERROR, "unknow file phase, phase: %d", file_phase);
    ret = TFS_ERROR;
    break;
  }

  SegmentData* seg_data = processing_seg_list_[index];
  if (ret != TFS_SUCCESS)
  {
    DUMP_SEGMENTDATA(seg_data, ERROR, "do request fail. client: %p, index: %hu, phase: %d, ret: %d",
                     client, index, file_phase, ret);
  }
  else
  {
    DUMP_SEGMENTDATA(seg_data, DEBUG, "do request success. client: %p, index: %hu, phase: %d, ret: %d",
                     client, index, file_phase, ret);
  }

  return ret;
}

int TfsFile::do_async_response(const InnerFilePhase file_phase, common::BasePacket* packet, const uint16_t index)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
  case FILE_PHASE_CREATE_FILE:
    ret = async_rsp_create_file(packet, index);
    break;
  case FILE_PHASE_READ_FILE:
    ret = async_rsp_read_file(packet, index);
    break;
  case FILE_PHASE_READ_FILE_V2:
    ret = async_rsp_read_file_v2(packet, index);
    break;
  case FILE_PHASE_WRITE_DATA:
    ret = async_rsp_write_data(packet, index);
    break;
  case FILE_PHASE_CLOSE_FILE:
    ret = async_rsp_close_file(packet, index);
    break;
  case FILE_PHASE_STAT_FILE:
    ret = async_rsp_stat_file(packet, index);
    break;
  case FILE_PHASE_UNLINK_FILE:
    ret = async_rsp_unlink_file(packet, index);
    break;
  default:
    TBSYS_LOG(ERROR, "unknow file phase, phase: %d", file_phase);
    ret = TFS_ERROR;
    break;
  }

  SegmentData* seg_data = processing_seg_list_[index];
  if (ret != TFS_SUCCESS)
  {
    DUMP_SEGMENTDATA(seg_data, ERROR, "do response fail. index: %hu, phase: %d, ret: %d",
                     index, file_phase, ret);
  }
  else
  {
    seg_data->status_ = phase_status[file_phase].status_;
    DUMP_SEGMENTDATA(seg_data, DEBUG, "do response success. index: %hu, phase: %d, ret: %d",
                     index, file_phase, ret);
  }

  return ret;
}

int TfsFile::async_req_create_file(NewClient* client, const uint16_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  CreateFilenameMessage cf_message;
  cf_message.set_block_id(seg_data->seg_info_.block_id_);
  cf_message.set_file_id(seg_data->seg_info_.file_id_);

  TBSYS_LOG(DEBUG, "create file start, client: %p, index: %hu, blockid: %u, fileid: %"PRI64_PREFIX"u",
            client, index, seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_);

  if (0 == seg_data->ds_.size())
  {
    TBSYS_LOG(ERROR, "create file fail: ds list is empty. blockid: %u, fileid: %" PRI64_PREFIX "u",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_);
  }
  else
  {
    uint8_t send_id;
    ret = client->post_request(seg_data->get_write_pri_ds(), &cf_message, send_id);
    send_id_index_map_[send_id] = index;
  }

  if (TFS_SUCCESS != ret)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }

  return ret;
}

int TfsFile::async_rsp_create_file(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_ERROR;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];
  if (NULL == rsp)
  {
    // maybe timeout, remove block cache
    remove_flag = true;
    TBSYS_LOG(ERROR, "create file name rsp null");
    ret = EXIT_TIMEOUT_ERROR;
  }
  else if (RESP_CREATE_FILENAME_MESSAGE != rsp->getPCode())
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      ret = msg->get_status();
      TBSYS_LOG(ERROR, "create file name fail. get error msg: %s, ret: %d, from: %s",
                msg->get_error(), ret, tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
      if (EXIT_NO_LOGICBLOCK_ERROR == ret)
      {
        remove_flag = true;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "create file name fail: unexpected message recieved");
    }
  }
  else
  {
    RespCreateFilenameMessage* msg = dynamic_cast<RespCreateFilenameMessage*>(rsp);
    if (0 == msg->get_file_id())
    {
      TBSYS_LOG(ERROR, "create file name fail. fileid: 0");
    }
    else
    {
      seg_data->seg_info_.file_id_ = msg->get_file_id();
      seg_data->write_file_number_ = msg->get_file_number();
      TBSYS_LOG(DEBUG, "create file name rsp. blockid: %u, fileid: %"PRI64_PREFIX"u, filenumber: %"PRI64_PREFIX"u",
                seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, seg_data->write_file_number_);
      ret = TFS_SUCCESS;
    }
  }

  if (remove_flag)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }
  return ret;
}

int TfsFile::async_req_write_data(NewClient* client, const uint16_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];

  WriteDataMessage wd_message;
  wd_message.set_file_number(seg_data->write_file_number_);
  wd_message.set_block_id(seg_data->seg_info_.block_id_);
  wd_message.set_file_id(seg_data->seg_info_.file_id_);
  wd_message.set_offset(seg_data->inner_offset_);
  wd_message.set_length(seg_data->seg_info_.size_);
  wd_message.set_ds_list(seg_data->ds_);
  wd_message.set_data(seg_data->buf_);

  TBSYS_LOG(DEBUG, "tfs write data start, blockid: %u, fileid: %"PRI64_PREFIX"u, size: %d, offset: %"PRI64_PREFIX"d",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, seg_data->seg_info_.size_, seg_data->seg_info_.offset_);
  // no not need to estimate the ds number is zero
  uint8_t send_id;
  int ret = client->post_request(seg_data->get_write_pri_ds(), &wd_message, send_id);
  if (TFS_SUCCESS != ret)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }
  else
  {
    send_id_index_map_[send_id] = index;
  }

  return ret;
}

int TfsFile::async_rsp_write_data(common::BasePacket* rsp, const uint16_t index)
{
#ifdef TFS_TEST
  UNUSED(rsp);
  int ret = TFS_SUCCESS;
#else
  int ret = TFS_ERROR;
  bool remove_flag = false;
#endif
  SegmentData* seg_data = processing_seg_list_[index];
#ifdef TFS_TEST
  TBSYS_LOG(DEBUG, "tfs write data success, offset: %"PRI64_PREFIX"d, size: %d",
                  seg_data->seg_info_.offset_, seg_data->seg_info_.size_);
#else
  if (NULL == rsp)
  {
    remove_flag = true;
    TBSYS_LOG(ERROR, "write data rsp null. dsip: %s",
              tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == msg->get_status())
      {
        // crc will be calculated once
        int32_t& crc_ref = seg_data->seg_info_.crc_;
        crc_ref = Func::crc(crc_ref, seg_data->buf_, seg_data->seg_info_.size_);
        ret = TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "tfs write data success, crc: %u, offset: %"PRI64_PREFIX"d, size: %d",
                  crc_ref, seg_data->seg_info_.offset_, seg_data->seg_info_.size_);
      }
      else
      {
        ret = msg->get_status();
        TBSYS_LOG(ERROR, "tfs write data, ret: %d, get error msg: %s, dsip: %s",
                  ret, msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          remove_flag = true;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "tfs write data, get error msg type: %d, dsip: %s",
                rsp->getPCode(), tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
    }
  }


  if (remove_flag)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }
#endif
  return ret;
}

//close file for write
int TfsFile::async_req_close_file(NewClient* client, const uint16_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  CloseFileMessage cf_message;
  cf_message.set_file_number(seg_data->write_file_number_);
  cf_message.set_block_id(seg_data->seg_info_.block_id_);
  cf_message.set_file_id(seg_data->seg_info_.file_id_);
  // set flag
  cf_message.set_option_flag(option_flag_);

  cf_message.set_ds_list(seg_data->ds_);
  cf_message.set_crc(seg_data->seg_info_.crc_);

  // no not need to estimate the ds number is zero
  uint8_t send_id;
  int ret = client->post_request(seg_data->get_write_pri_ds(), &cf_message, send_id);
  if (TFS_SUCCESS != ret)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }
  else
  {
    send_id_index_map_[send_id] = index;
  }

  return ret;
}

int TfsFile::async_rsp_close_file(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_ERROR;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];
  if (NULL == rsp)
  {
    remove_flag = true;
    TBSYS_LOG(ERROR, "tfs file close, send msg to dataserver time out, blockid: %u, fileid: %"
              PRI64_PREFIX "u, dsip: %s",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == msg->get_status())
      {
        ret = TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "tfs file close success, dsip: %s",
                  tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
      }
      else
      {
        ret = msg->get_status();
        TBSYS_LOG(ERROR, "tfs file close, ret: %d, get error msg: %s, dsip: %s",
                  ret, msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          remove_flag = true;
        }
      }
    }
  }

  if (remove_flag)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_fail_, 1);
  }
  else
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::write_success_, 1);
  }
  return ret;
}

int TfsFile::async_req_read_file(NewClient* client, const uint16_t index,
                                SegmentData& seg_data, common::BasePacket& rd_message)
{
  int ret = TFS_ERROR;

  if (seg_data.pri_ds_index_ >= 0)
  {
    int32_t ds_size = seg_data.ds_.size();
    TBSYS_LOG(DEBUG, "req read file, blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, ds size: %d",
              seg_data.seg_info_.block_id_, seg_data.seg_info_.file_id_,
              seg_data.seg_info_.offset_, seg_data.seg_info_.size_, ds_size);

    // need check ?
    if (0 != seg_data.seg_info_.file_id_ && 0 != ds_size)
    {
      int32_t retry_count = seg_data.get_orig_pri_ds_index() - seg_data.pri_ds_index_;
      retry_count = retry_count <= 0 ? ds_size + retry_count : retry_count;

      bool remove_cache = false;
      // try until post success
      uint8_t send_id = 0;
      while (retry_count-- > 0 &&
             (ret = client->post_request(seg_data.get_read_pri_ds(),
                           &rd_message, send_id)) != TFS_SUCCESS)
      {
        if (ret != EXIT_SENDMSG_ERROR)
        {
          break;
        }

        TBSYS_LOG(ERROR, "post read file req fail. blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s. try: %d",
                  seg_data.seg_info_.block_id_, seg_data.seg_info_.file_id_,
                  tbsys::CNetUtil::addrToString(seg_data.get_read_pri_ds()).c_str(), retry_count);
        seg_data.set_pri_ds_index(seg_data.pri_ds_index_ + 1);
        remove_cache = true;
      }
      if (TFS_SUCCESS == ret)
      {
        send_id_index_map_[send_id] = index;
      }

      if (remove_cache)
      {
        if (CACHE_HIT_LOCAL == seg_data.cache_hit_)
        {
          tfs_session_->remove_local_block_cache(seg_data.seg_info_.block_id_);
        }
#ifdef WITH_TAIR_CACHE
        else if (CACHE_HIT_REMOTE == seg_data.cache_hit_)
        {
          tfs_session_->remove_local_block_cache(seg_data.seg_info_.block_id_);
          tfs_session_->remove_remote_block_cache(seg_data.seg_info_.block_id_);
        }
#endif
        seg_data.cache_hit_ = CACHE_HIT_NONE;
      }

      if (0 == retry_count) // try last ds
      {
        seg_data.pri_ds_index_ = PRI_DS_TRY_ALL_OVER;
      }
      else
      {
        seg_data.set_pri_ds_index(seg_data.pri_ds_index_ + 1);
      }
    }
  }
  return ret;
}

int TfsFile::async_req_read_file(NewClient* client, const uint16_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  ReadDataMessage rd_message;
  rd_message.set_block_id(seg_data->seg_info_.block_id_);
  rd_message.set_file_id(seg_data->seg_info_.file_id_);
  rd_message.set_offset(seg_data->inner_offset_);
  rd_message.set_length(seg_data->seg_info_.size_);
  rd_message.set_flag(seg_data->read_flag_);

  int ret = async_req_read_file(client, index, *seg_data, rd_message);
  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_fail_, 1);
  }

  return ret;
}

int TfsFile::async_rsp_read_file(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_ERROR;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];

  int ret_len = -1;
#ifndef TFS_TEST
  if (NULL == rsp)
  {
    remove_flag = true;
    TBSYS_LOG(ERROR, "tfs file read fail, send msg to dataserver time out, blockid: %u, fileid: %"
              PRI64_PREFIX "u, dsip: %s",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (RESP_READ_DATA_MESSAGE == rsp->getPCode())
    {
      RespReadDataMessage* msg = dynamic_cast<RespReadDataMessage*>(rsp);
      ret_len = msg->get_length();
#else
      UNUSED(rsp);
      bool bRet = get_process_result(seg_data->seg_info_.block_id_, seg_data->ds_);
      if (bRet)
      {
        ret_len = 1024;
        eof_ = TFS_FILE_EOF_FLAG_YES;
      }
      else
        ret_len = EXIT_NO_LOGICBLOCK_ERROR;
#endif
      if (ret_len >= 0)
      {
#ifndef TFS_TEST
        if (ret_len < seg_data->seg_info_.size_)
        {
          eof_ = TFS_FILE_EOF_FLAG_YES;
        }

        if (ret_len > 0)
        {
          memcpy(seg_data->buf_, msg->get_data(), ret_len);
        }
#endif
        seg_data->seg_info_.size_ = ret_len; //use seg_info_.size as return len
        ret = TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "rsp read file, blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, inneroffset: %d, size: %d",
                  seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
                  seg_data->seg_info_.offset_, seg_data->inner_offset_, seg_data->seg_info_.size_);
      }
      else
      {
        if (EXIT_NO_LOGICBLOCK_ERROR == ret_len)
        {
          remove_flag = true;
        }
        TBSYS_LOG(ERROR, "tfs read fail from dataserver: %s, ret len: %d",
                  tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str(), ret_len);
        //set errno
        ret = ret_len;
      }
#ifndef TFS_TEST
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      TBSYS_LOG(ERROR, "tfs read, get status msg: %s", msg->get_error());
      if (STATUS_MESSAGE_ACCESS_DENIED == msg->get_status())
      {
        // if access denied, return directly
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "message type is error.");
    }
  }
#endif

  if (remove_flag)
  {
    if (CACHE_HIT_LOCAL == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    }
#ifdef WITH_TAIR_CACHE
    if (CACHE_HIT_REMOTE == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
      tfs_session_->remove_remote_block_cache(seg_data->seg_info_.block_id_);
    }
#endif
    seg_data->cache_hit_ = CACHE_HIT_NONE;
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_fail_, 1);
  }
  else
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_success_, 1);
  }
  return ret;
}

int TfsFile::async_req_read_file_v2(NewClient* client, const uint16_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  ReadDataMessageV2 rd_message;
  rd_message.set_block_id(seg_data->seg_info_.block_id_);
  rd_message.set_file_id(seg_data->seg_info_.file_id_);
  rd_message.set_offset(seg_data->inner_offset_);
  rd_message.set_length(seg_data->seg_info_.size_);
  rd_message.set_flag(seg_data->read_flag_);

  int ret = async_req_read_file(client, index, *seg_data, rd_message);
  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_fail_, 1);
  }
  return ret;
}

int TfsFile::async_rsp_read_file_v2(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_ERROR;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];

  int ret_len = -1;
#ifndef TFS_TEST
  if (NULL == rsp)
  {
    remove_flag = true;
    TBSYS_LOG(ERROR, "tfs file read fail, send msg to dataserver time out, blockid: %u, fileid: %"
              PRI64_PREFIX "u, dsip: %s",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (RESP_READ_DATA_MESSAGE_V2 == rsp->getPCode())
    {
      RespReadDataMessageV2* msg = dynamic_cast<RespReadDataMessageV2*>(rsp);
      ret_len = msg->get_length();
#else
      UNUSED(rsp);
      bool bRet = get_process_result(seg_data->seg_info_.block_id_, seg_data->ds_);
      if (bRet)
      {
        ret_len = 1024;
        eof_ = TFS_FILE_EOF_FLAG_YES;
        ret = TFS_SUCCESS;
      }
      else
        ret_len = EXIT_NO_LOGICBLOCK_ERROR;
#endif

      // read fail
      if (ret_len < 0)
      {
        if (EXIT_NO_LOGICBLOCK_ERROR == ret_len)
        {
          remove_flag = true;
        }
        TBSYS_LOG(ERROR, "tfs file read fail from dataserver: %s, ret len: %d",
                  tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str(), ret_len);
        //set errno
        ret = ret_len;
      }
#ifndef TFS_TEST
      else
      {
        ret = TFS_SUCCESS;
        // check fileinfo
        if (0 == seg_data->inner_offset_ && NULL == msg->get_file_info())
        {
          TBSYS_LOG(WARN, "tfs read, get file info error, blockid: %u, fileid: %"PRI64_PREFIX"u, form dataserver: %s",
                    seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
                    tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
          ret = TFS_ERROR;
        }
        else
        {
          if (NULL != msg->get_file_info())
          {
            if (NULL == seg_data->file_info_) // for stat, file_info_ is ready
            {
              seg_data->file_info_ = new FileInfo();
            }
            memcpy(seg_data->file_info_, msg->get_file_info(), FILEINFO_SIZE);
          }
          if (NULL != seg_data->file_info_ && seg_data->file_info_->id_ != seg_data->seg_info_.file_id_)
          {
            TBSYS_LOG(WARN, "tfs read, blockid: %u, return fileid: %"
                      PRI64_PREFIX"u, require fileid: %"PRI64_PREFIX"u not match, from dataserver: %s",
                      seg_data->seg_info_.block_id_, seg_data->file_info_->id_, seg_data->seg_info_.file_id_,
                      tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
            ret = EXIT_FILE_INFO_ERROR;
          }
        }

        // check length, ret_len >= 0
        if (TFS_SUCCESS == ret)
        {
          if (ret_len < seg_data->seg_info_.size_)
          {
            eof_ = TFS_FILE_EOF_FLAG_YES;
          }

          if (ret_len > 0)
          {
            memcpy(seg_data->buf_, msg->get_data(), ret_len);
          }
#endif
          seg_data->seg_info_.size_ = ret_len; //use seg_info_.size as return len

          TBSYS_LOG(DEBUG, "rsp read file, blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, inneroffset: %d, size: %d",
                    seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
                    seg_data->seg_info_.offset_, seg_data->inner_offset_, seg_data->seg_info_.size_);
#ifndef TFS_TEST
        }
      }
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      TBSYS_LOG(ERROR, "tfs read, get status msg: %s", msg->get_error());
      if (STATUS_MESSAGE_ACCESS_DENIED == msg->get_status())
      {
        // if access denied, return directly
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "message type is error.");
    }
  }
#endif
  if (remove_flag)
  {
    if (CACHE_HIT_LOCAL == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    }
#ifdef WITH_TAIR_CACHE
    else if (CACHE_HIT_REMOTE == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
      tfs_session_->remove_remote_block_cache(seg_data->seg_info_.block_id_);
    }
#endif
    seg_data->cache_hit_ = CACHE_HIT_NONE;
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_fail_, 1);
  }
  else
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::read_success_, 1);
  }
  return ret;
}

int TfsFile::async_req_stat_file(NewClient* client, const uint16_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  if (seg_data->pri_ds_index_ >= 0)
  {
    FileInfoMessage stat_message;
    stat_message.set_block_id(seg_data->seg_info_.block_id_);
    stat_message.set_file_id(seg_data->seg_info_.file_id_);
    TBSYS_LOG(DEBUG, "req stat file flag: %d", flags_);
    stat_message.set_mode(seg_data->stat_mode_);

    int32_t ds_size = seg_data->ds_.size();
    if (ds_size > 0)
    {
      int32_t retry_count = seg_data->get_orig_pri_ds_index() - seg_data->pri_ds_index_;
      retry_count = retry_count <= 0 ? ds_size + retry_count : retry_count;

      uint8_t send_id = 0;
      bool remove_cache = false;
      while (retry_count > 0)
      {
        ret = client->post_request(seg_data->get_read_pri_ds(), &stat_message, send_id);
        if (EXIT_SENDMSG_ERROR == ret)
        {
          TBSYS_LOG(ERROR, "post stat file req fail. blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s",
                    seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
                    tbsys::CNetUtil::addrToString(seg_data->get_read_pri_ds()).c_str());
          remove_cache = true;
          seg_data->set_pri_ds_index(seg_data->pri_ds_index_ + 1);
          --retry_count;
        }
        else
        {
          break;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        send_id_index_map_[send_id] = index;
      }
      if (0 == retry_count) // try last ds
      {
        seg_data->pri_ds_index_ = PRI_DS_TRY_ALL_OVER;
      }
      else
      {
        seg_data->set_pri_ds_index(seg_data->pri_ds_index_ + 1);
      }

      if (remove_cache)
      {
        if (CACHE_HIT_LOCAL == seg_data->cache_hit_)
        {
          tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
        }
#ifdef WITH_TAIR_CACHE
        else if (CACHE_HIT_REMOTE == seg_data->cache_hit_)
        {
          tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
          tfs_session_->remove_remote_block_cache(seg_data->seg_info_.block_id_);
        }
#endif
        seg_data->cache_hit_ = CACHE_HIT_NONE;
      }
    }

    if (TFS_SUCCESS != ret)
    {
      BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::stat_fail_, 1);
    }
  }

  return ret;
}

int TfsFile::async_rsp_stat_file(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_SUCCESS;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];

#ifndef TFS_TEST
  if (NULL == rsp)
  {
    TBSYS_LOG(ERROR, "tfs file stat fail, send msg to dataserver time out, blockid: %u, fileid: %"
              PRI64_PREFIX "u, dsip: %s",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
    remove_flag = true;
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (RESP_FILE_INFO_MESSAGE != rsp->getPCode())
    {
      if (STATUS_MESSAGE == rsp->getPCode())
      {
        ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
        TBSYS_LOG(ERROR, "stat file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d, erorr msg: %s",
                  seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ret, dynamic_cast<StatusMessage*>(rsp)->get_error());
#else
      UNUSED(rsp);
      bool bRet = get_process_result(seg_data->seg_info_.block_id_, seg_data->ds_);
      if (bRet)
      {
        ret = TFS_SUCCESS;
      }
      else
      {
        ret = EXIT_NO_LOGICBLOCK_ERROR;
        TBSYS_LOG(ERROR, "stat file fail from dataserver: %s, ret: %d",
                  tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str(), ret);
      }
#endif
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          remove_flag = true;
        }
#ifndef TFS_TEST
      }
      else
      {
        ret = EXIT_UNKNOWN_MSGTYPE;
        TBSYS_LOG(ERROR, "stat file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, erorr msg: %s",
                  seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, "msg type error");
      }
    }
    else
    {
      RespFileInfoMessage *msg = dynamic_cast<RespFileInfoMessage*> (rsp);
      if (NULL != msg->get_file_info())
      {
        if (msg->get_file_info()->id_ != seg_data->seg_info_.file_id_)
        {
          TBSYS_LOG(ERROR, "tfs stat fail. msg fileid: %"PRI64_PREFIX"u, require fileid: %"PRI64_PREFIX"u not match",
                    msg->get_file_info()->id_, seg_data->seg_info_.file_id_);
          ret = EXIT_FILE_INFO_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          if (NULL == seg_data->file_info_)
          {
            seg_data->file_info_ = new FileInfo();
          }
          memcpy(seg_data->file_info_, msg->get_file_info(), FILEINFO_SIZE);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "tfs stat fail. blockid: %u, fileid: %"PRI64_PREFIX"u is not exist, server: %s",
                  seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
                  tbsys::CNetUtil::addrToString(seg_data->get_last_read_pri_ds()).c_str());
        ret = TFS_ERROR;
      }
    }
  }
#endif

  if (remove_flag)
  {
    if (CACHE_HIT_LOCAL == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    }
#ifdef WITH_TAIR_CACHE
    else if (CACHE_HIT_REMOTE == seg_data->cache_hit_)
    {
      tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
      tfs_session_->remove_remote_block_cache(seg_data->seg_info_.block_id_);
    }
#endif
    seg_data->cache_hit_ = CACHE_HIT_NONE;
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::stat_fail_, 1);
  }
  else
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::stat_success_, 1);
  }

  return ret;
}

int TfsFile::async_req_unlink_file(NewClient* client, const uint16_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  UnlinkFileMessage uf_message;
  uf_message.set_block_id(seg_data->seg_info_.block_id_);
  uf_message.set_file_id(seg_data->seg_info_.file_id_);
  uf_message.set_ds_list(seg_data->ds_);
  uf_message.set_unlink_type(static_cast<int32_t>(seg_data->unlink_action_)); // action
  uf_message.set_option_flag(option_flag_);

  // no not need to estimate the ds number is zero
  uint8_t send_id;
  int ret = client->post_request(seg_data->get_write_pri_ds(), &uf_message, send_id);
  if (TFS_SUCCESS != ret)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::unlink_fail_, 1);
  }
  else
  {
    send_id_index_map_[send_id] = index;
  }

  return ret;
}

int TfsFile::async_rsp_unlink_file(common::BasePacket* rsp, const uint16_t index)
{
  int ret = TFS_SUCCESS;
  bool remove_flag = false;
  SegmentData* seg_data = processing_seg_list_[index];

  if (NULL == rsp)
  {
    TBSYS_LOG(ERROR, "tfs file unlink file fail, send msg to dataserver time out, blockid: %u, fileid: %"
              PRI64_PREFIX "u, dsip: %s",
              seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->get_write_pri_ds()).c_str());
    remove_flag = true;
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK != msg->get_status())
      {
        ret = msg->get_status();
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          remove_flag = true;
        }
        TBSYS_LOG(WARN, "block_id: %u, file_id: %"PRI64_PREFIX"u, error: %s, status: %d",
                  seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, msg->get_error(), msg->get_status());
      }
      else
      {
        const char* data = msg->get_error();
        if (NULL != data)
        {
          int64_t file_size = strtoll(data, reinterpret_cast<char**>(NULL), 0);
          if (file_size > 0)    // compatibile, no return file size, not error.
          {
            seg_data->seg_info_.size_ = file_size;
          }
        }
      }
    }
    else // unknow msg type
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "unlink file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, erorr msg: %s, msg type: %d",
                seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, "msg type error", rsp->getPCode());
    }
  }

  if (remove_flag)
  {
    tfs_session_->remove_local_block_cache(seg_data->seg_info_.block_id_);
  }

  if (TFS_SUCCESS != ret)
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::unlink_fail_, 1);
  }
  else
  {
    BgTask::get_stat_mgr().update_entry(StatItem::client_access_stat_, StatItem::unlink_success_, 1);
  }

  return ret;
}

int TfsFile::read_process_ex(int64_t& read_size, const InnerFilePhase read_file_phase)
{
  int ret = TFS_ERROR;
  // segList.size() != 0
  // just use first ds list size to be retry times. maybe random ..
  int32_t retry_count = processing_seg_list_[0]->ds_.size();
  do
  {
    ret = process(read_file_phase);
    finish_read_process(ret, read_size);
  } while (ret != TFS_SUCCESS && --retry_count > 0);

  return ret;
}

//calculate the size of read process and delete the success one from processing seg list
int32_t TfsFile::finish_read_process(int status, int64_t& read_size)
{
  int32_t count = 0;
  SEG_DATA_LIST_ITER it = processing_seg_list_.begin();

  if (TFS_SUCCESS == status)
  {
    count = processing_seg_list_.size();
    for ( ; it != processing_seg_list_.end(); ++it)
    {
      read_size += (*it)->seg_info_.size_;
      if ((*it)->delete_flag_)
      {
        tbsys::gDelete(*it);
      }
    }
    processing_seg_list_.clear();
  }
  else if (status != EXIT_ALL_SEGMENT_ERROR)
  {
    while (it != processing_seg_list_.end())
    {
      if (SEG_STATUS_ALL_OVER == (*it)->status_) // all over
      {
        read_size += (*it)->seg_info_.size_;
        if ((*it)->delete_flag_)
        {
          tbsys::gDelete(*it);
        }
        it = processing_seg_list_.erase(it);
        count++;
      }
      else
      {
        it++;
      }
    }
  }
  return count;
}

int TfsFile::stat_process()
{
  int ret = TFS_ERROR;
  int32_t retry_count = processing_seg_list_[0]->ds_.size();
  do
  {
    ret = process(FILE_PHASE_STAT_FILE);
  } while (TFS_SUCCESS != ret && --retry_count > 0);

  if (TFS_SUCCESS != ret)
  {
    get_block_info(*meta_seg_, flags_);
  }
  meta_seg_->reset_status();

  return ret;
}
#ifdef TFS_TEST
bool TfsFile::get_process_result(uint32_t block_id, VUINT64& ds)
{
  std::map<uint32_t, VUINT64> & block_ds_map = tfs_session_->block_ds_map_;
  std::map<uint32_t, VUINT64>::iterator iter = block_ds_map.find(block_id);
  if (block_ds_map.end() != iter)
  {
    VUINT64 & real_ds_list = iter->second;
    uint32_t ds_count = ds.size();
    if (ds_count == real_ds_list.size())
    {
      uint32_t i = 0;
      for (; i < ds_count; i++)
      {
        if (ds[i] != real_ds_list[i])
          return false;
      }
      if (ds_count == i)
        return true;
    }
  }
  // should never happen
  return false;
}
#endif
