/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_console.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */

#include <algorithm>

#include "tbsys.h"
#include "Memory.hpp"

#include "common/func.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/read_data_message.h"
#include "message/write_data_message.h"
#include "message/client_cmd_message.h"
#include "dataserver/dataserver_define.h"
#include "dataserver/visit_stat.h"
#include "new_client/tfs_client_impl.h"
#include "new_client/fsname.h"

#include "block_console.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::dataserver;

const int64_t TranBlock::TRAN_BUFFER_SIZE = 8 * 1024 * 1024;
const int64_t TranBlock::WAIT_TIME_OUT = 5000000;
const int64_t TranBlock::RETRY_TIMES = 2;

BlockConsole::BlockConsole() :
  input_file_ptr_(NULL), succ_file_ptr_(NULL), fail_file_ptr_(NULL)
{
  input_blockids_.clear();
  succ_blockids_.clear();
  fail_blockids_.clear();
  dest_ds_ids_.clear();
  memset(&stat_param_, 0, sizeof(StatParam));
}

BlockConsole::~BlockConsole()
{
  if (NULL != input_file_ptr_)
  {
    fclose(input_file_ptr_);
  }
  if (NULL != succ_file_ptr_)
  {
    fclose(succ_file_ptr_);
  }
  if (NULL != fail_file_ptr_)
  {
    fclose(fail_file_ptr_);
  }
}

int BlockConsole::initialize(const string& ts_input_blocks_file, const std::string& dest_ds_ip_file)
{
  int ret = TFS_SUCCESS;
  FILE* dest_ds_file_ptr = NULL;
  if (access(ts_input_blocks_file.c_str(), R_OK) < 0 || access(dest_ds_ip_file.c_str(), R_OK) < 0)
  {
    TBSYS_LOG(ERROR, "access input block file: %s, ds file: %s fail, error: %s\n",
        ts_input_blocks_file.c_str(), dest_ds_ip_file.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else if ((input_file_ptr_ = fopen(ts_input_blocks_file.c_str(), "r")) == NULL ||
      (dest_ds_file_ptr = fopen(dest_ds_ip_file.c_str(), "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "open input block file: %s, ds file: %s fail, error: %s\n",
        ts_input_blocks_file.c_str(), dest_ds_ip_file.c_str(), strerror(errno));
    ret = TFS_ERROR;
  }
  else
  {
    // load dest ds
    uint64_t ds_id = 0;
    char ds_ip[SPEC_LEN] = {'\0'};
    while (fscanf(dest_ds_file_ptr, "%s", ds_ip) != EOF)
    {
      ds_id = Func::get_host_ip(ds_ip);
      if (0 == ds_id)
      {
        TBSYS_LOG(ERROR, "input ds ip: %s is illegal.\n", ds_ip);
      }
      else
      {
        dest_ds_ids_.insert(ds_id);
        TBSYS_LOG(DEBUG, "input ds ip: %s, ds id: %"PRI64_PREFIX"u succ.\n", ds_ip, ds_id);
      }
    }

    if (dest_ds_ids_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "ds file: %s size: %d is illegal.\n",
          dest_ds_ip_file.c_str(), static_cast<int32_t>(dest_ds_ids_.size()));
      ret = TFS_ERROR;
    }
    cur_ds_sit_ = dest_ds_ids_.begin();
  }
  if (NULL != dest_ds_file_ptr)
  {
    fclose(dest_ds_file_ptr);
  }

  if (TFS_SUCCESS == ret)
  {
    string ts_succ_blocks_file = ts_input_blocks_file + string(".succ");
    string ts_fail_blocks_file = ts_input_blocks_file + string(".fail");

    if ((succ_file_ptr_ = fopen(ts_succ_blocks_file.c_str(), "a+")) == NULL)
    {
      TBSYS_LOG(ERROR, "open succ output file: %s, error: %s\n", ts_succ_blocks_file.c_str(), strerror(errno));
      ret = TFS_ERROR;
    }
    else if ((fail_file_ptr_ = fopen(ts_fail_blocks_file.c_str(), "a+")) == NULL)
    {
      TBSYS_LOG(ERROR, "open fail output file: %s, error: %s\n", ts_fail_blocks_file.c_str(), strerror(errno));
      ret = TFS_ERROR;
    }
    else
    {
      // load input blocks
      uint32_t blockid = 0;
      while (fscanf(input_file_ptr_, "%u", &blockid) != EOF)
      {
        input_blockids_.insert(blockid);
        ++stat_param_.total_;
      }

      // load succ blocks
      while (fscanf(succ_file_ptr_, "%u", &blockid) != EOF)
      {
        succ_blockids_.insert(blockid);
        atomic_inc(&stat_param_.copy_success_);
      }

      // load fail blocks
      while (fscanf(succ_file_ptr_, "%u", &blockid) != EOF)
      {
        fail_blockids_.insert(blockid);
        atomic_inc(&stat_param_.copy_failure_);
      }

      ret = locate_cur_pos();
    }
  }

  return ret;
}

int BlockConsole::get_transfer_param(uint32_t& blockid, uint64_t& ds_id)
{
  int ret = TFS_SUCCESS;
  tbutil::Mutex::Lock lock(mutex_);
  // get block
  if (cur_sit_ == input_blockids_.end())
  {
    TBSYS_LOG(ERROR, "transfer block finish.\n");
    ret = TRANSFER_FINISH;
  }
  else
  {
    blockid = *cur_sit_;
  }

  // get ds
  if (TFS_SUCCESS == ret)
  {
    ++cur_sit_;
    if (dest_ds_ids_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "ds size: %d is illegal.\n",
          static_cast<int32_t>(dest_ds_ids_.size()));
      ret = TFS_ERROR;
    }
    else
    {
      if (cur_ds_sit_ == dest_ds_ids_.end())
      {
        cur_ds_sit_ = dest_ds_ids_.begin();
      }

      ds_id = *cur_ds_sit_;
      ++cur_ds_sit_;
    }
  }

  return ret;
}

int BlockConsole::finish_transfer_block(const uint32_t blockid, const int32_t transfer_ret)
{
  FILE* file_ptr = NULL;
  tbutil::Mutex::Lock lock(mutex_);
  if (TFS_SUCCESS == transfer_ret)
  {
    succ_blockids_.insert(blockid);
    atomic_inc(&stat_param_.copy_success_);
    file_ptr = succ_file_ptr_;
  }
  else
  {
    fail_blockids_.insert(blockid);
    atomic_inc(&stat_param_.copy_failure_);
    file_ptr = fail_file_ptr_;
  }

  TBSYS_LOG(INFO, "begin write blockid: %d to %s file", blockid, TFS_SUCCESS == transfer_ret ? "succ" : "fail");
  int ret = TFS_SUCCESS;
  if ((ret = fprintf(file_ptr, "%d\n", blockid)) < 0)
  {
    TBSYS_LOG(ERROR, "failed to write blockid: %d to %s file", blockid, TFS_SUCCESS == transfer_ret ? "succ" : "fail");
    ret = TFS_ERROR;
  }

  fflush(file_ptr);

  return ret;
}

int BlockConsole::locate_cur_pos()
{
  int ret = TFS_SUCCESS;
  // get current copy id
  uint32_t last_blockid = 0;
  if (succ_blockids_.size() > 0)
  {
    std::set<uint32_t>::iterator sit = succ_blockids_.end();
    --sit;
    last_blockid = *(sit);
    cur_sit_ = input_blockids_.find(last_blockid);

    // move to next one
    if (cur_sit_ != input_blockids_.end())
    {
      ++cur_sit_;
    }
  }
  else // brand-new
  {
    cur_sit_ = input_blockids_.begin();
  }

  if (cur_sit_ == input_blockids_.end())
  {
    TBSYS_LOG(ERROR, "can not find blockid: %u\n", last_blockid);
    ret = TFS_ERROR;
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TranBlock::TranBlock(const uint32_t blockid, const std::string& dest_ns_addr,
    const uint64_t dest_ds_addr, const int64_t traffic, TfsSession* src_session, TfsSession* dest_session) :
    dest_ns_addr_(dest_ns_addr), dest_ds_id_(dest_ds_addr),
    cur_offset_(0), total_tran_size_(0), traffic_(traffic), src_session_(src_session), dest_session_(dest_session)
{
  seg_data_.seg_info_.block_id_ = blockid;
  seg_data_.ds_.clear();
  file_set_.clear();
  concealed_files_.clear();
}

TranBlock::~TranBlock()
{
  seg_data_.ds_.clear();
  file_set_.clear();
  concealed_files_.clear();
}

int TranBlock::run()
{
  int ret = TFS_SUCCESS;
  while (true)
  {
    if ((ret = get_src_ds()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = read_index()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = read_data()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = recombine_data()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = check_dest_blk()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = write_data()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = write_index()) != TFS_SUCCESS)
    {
      break;
    }
    ret = check_integrity();
    break;
  }

  return ret;
}

int TranBlock::get_src_ds()
{
  int ret = src_session_->get_block_info(seg_data_, T_READ);
  if (TFS_SUCCESS == ret)
  {
    if (seg_data_.ds_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "get rds fail. block list size is %d, blockid: %u", static_cast<int32_t>(seg_data_.ds_.size()), seg_data_.seg_info_.block_id_);
      ret = TFS_ERROR;
    }
  }
  return ret;
}

int TranBlock::read_index()
{
  GetServerStatusMessage gfl_msg;
  gfl_msg.set_status_type(GSS_BLOCK_FILE_INFO);
  gfl_msg.set_return_row(seg_data_.seg_info_.block_id_);

  // fetch data from the first ds
  int index = 0;
  int ret = TFS_ERROR;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* rsp = NULL;
    if ((ret = send_msg_to_server(seg_data_.ds_[index], client, &gfl_msg, rsp)) == TFS_SUCCESS)
    {
      if (rsp == NULL)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %u, rsp is null.",
            tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_);
      }
      else
      {
        if (BLOCK_FILE_INFO_MESSAGE == rsp->getPCode())
        {
          BlockFileInfoMessage *bfi_msg = dynamic_cast<BlockFileInfoMessage*>(rsp);
          FILE_INFO_LIST* tmp_file_vector = bfi_msg->get_fileinfo_list();
          for (FILE_INFO_LIST::iterator fit = tmp_file_vector->begin(); fit != tmp_file_vector->end(); ++fit)
          {
            FileInfo finfo = (*fit);
            file_set_.insert(finfo);
          }

          total_tran_size_ += file_set_.size() * FILEINFO_SIZE;
        }
        else
        {
          TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %u, unknow msg type: %d",
              tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, rsp->getPCode());
          ret = TFS_ERROR;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %u, ret: %d.",
            tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, ret);
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  else
  {
    TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %u, create client failed.",
        tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_);
  }

  return ret;
}

int TranBlock::read_data()
{
  int index = 0;
  bool eof_flag = false;
  int ret = TFS_SUCCESS;
  int64_t read_size = std::min(traffic_, TRAN_BUFFER_SIZE);
  int64_t micro_sec = 1000 * 1000;

  while (!eof_flag)
  {
    int64_t remainder_retrys = RETRY_TIMES;
    TIMER_START();
    while (remainder_retrys > 0)
    {
      ReadRawDataMessage rrd_msg;
      rrd_msg.set_block_id(seg_data_.seg_info_.block_id_);
      rrd_msg.set_offset(cur_offset_);
      rrd_msg.set_length(read_size);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp = NULL;
      if(TFS_SUCCESS != send_msg_to_server(seg_data_.ds_[index], client, &rrd_msg, rsp))
      {
        rsp = NULL;
      }

      // fetch data from the first ds
      if (NULL == rsp)
      {
        TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d",
            tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, cur_offset_, remainder_retrys, ret);
      }
      else if (RESP_READ_RAW_DATA_MESSAGE == rsp->getPCode())
      {
        RespReadRawDataMessage* rsp_rrd_msg = dynamic_cast<RespReadRawDataMessage*>(rsp);
        int len = rsp_rrd_msg->get_length();
        if (len >= 0)
        {
          if (len < read_size || len == 0)
          {
            eof_flag = true;
          }
          if (len > 0)
          {
            TBSYS_LOG(DEBUG, "read raw data from ds: %s succ, blockid: %u, offset: %d, len: %d, data: %p",
                tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, cur_offset_, len, rsp_rrd_msg->get_data());
            src_content_buf_.writeBytes(rsp_rrd_msg->get_data(), len);
            cur_offset_ += len;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d",
              tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, cur_offset_, remainder_retrys, len);
          ret = len;
        }
      }
      else //unknow type
      {
        TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, unknow msg type: %d",
            tbsys::CNetUtil::addrToString(seg_data_.ds_[index]).c_str(), seg_data_.seg_info_.block_id_, cur_offset_, remainder_retrys, rsp->getPCode());
        ret = TFS_ERROR;
      }

      NewClientManager::get_instance().destroy_client(client);
      if (NULL == rsp)
      {
        --remainder_retrys;
        continue;
      }
      else
      {
        break;
      }
    }
    TIMER_END();

    if (TFS_SUCCESS != ret)
    {
      break;
    }

    // restrict speed
    int64_t speed = 0;
    int64_t d_value = micro_sec - TIMER_DURATION();
    if (d_value > 0)
    {
      usleep(d_value);
      speed = read_size * 1000 * 1000 / TIMER_DURATION();
    }

    TBSYS_LOG(DEBUG, "read data, cost time: %"PRI64_PREFIX"d, read size: %"PRI64_PREFIX"d, speed: %"PRI64_PREFIX"d, need sleep time: %"PRI64_PREFIX"d",
        TIMER_DURATION(), read_size, speed, d_value);
  }

  total_tran_size_ += src_content_buf_.getDataLen();
  return ret;
}

int TranBlock::recombine_data()
{
  int ret = TFS_SUCCESS;
  if (file_set_.size() <= 0)
  {
    TBSYS_LOG(ERROR, "reform data failed, blockid: %u, offset: %d, file list size: %d",
        seg_data_.seg_info_.block_id_, cur_offset_, static_cast<int32_t>(file_set_.size()));
    ret = TFS_ERROR;
  }
  else
  {
    FileInfoSetIter vit = file_set_.begin();
    for ( ; vit != file_set_.end(); ++vit)
    {
      const FileInfo& finfo = (*vit);
      //TBSYS_LOG(DEBUG, "file info. blockid: %u, fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, usize: %d, offset: %d",
      //    seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_, finfo.size_, finfo.usize_, finfo.offset_);
      // skip deleted file
      if (0 != (finfo.flag_ & (FI_DELETED | FI_INVALID)))
      {
        TBSYS_LOG(INFO, "file deleted, skip it. blockid: %u, fileid: %"PRI64_PREFIX"u, flag: %d",
            seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_);
        continue;
      }
      else if (0 != (finfo.flag_ & FI_CONCEAL)) // record conceal file
      {
        TBSYS_LOG(INFO, "file concealed. blockid: %u, fileid: %"PRI64_PREFIX"u, flag: %d",
            seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_);
        concealed_files_.insert(finfo.id_);
      }

      // skip illegal file
      if (0 == finfo.id_)
      {
        TBSYS_LOG(ERROR, "fileid illegal, skip it. blockid: %u, fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, offset: %d",
            seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_, finfo.size_, finfo.offset_);
        continue;
      }

      // compare file info
      FileInfo* data_finfo = reinterpret_cast<FileInfo*>(src_content_buf_.getData() + finfo.offset_);
      if (memcmp(&finfo, data_finfo, sizeof(FILEINFO_SIZE)) != 0)
      {
        if (finfo.id_ != data_finfo->id_)
        {
          TBSYS_LOG(ERROR, "file id conflict in same offset, serious error. blockid: %u, info fileid: %"PRI64_PREFIX"u, offset: %d, data fileid: %"PRI64_PREFIX"u, offset: %d, total len: %d",
              seg_data_.seg_info_.block_id_, finfo.id_, finfo.offset_, data_finfo->id_, data_finfo->offset_, src_content_buf_.getDataLen());
        }
        else if (finfo.modify_time_ != data_finfo->modify_time_)
        {
          TBSYS_LOG(WARN, "file modify time conflict in same offset, file updated after get meta info. blockid: %u, fileid: %"PRI64_PREFIX"u <> %"PRI64_PREFIX"d, info modify time: %d, data modify time: %d",
              seg_data_.seg_info_.block_id_, finfo.id_, data_finfo->id_, finfo.modify_time_, data_finfo->modify_time_);
        }
        else
        {
          TBSYS_LOG(WARN, "file info conflict in same offset, skip it. blockid: %u, info fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, usize: %d, offset: %d, create time: %d, modify time: %d, crc: %u, data fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, usize: %d, offset: %d, create time: %d, modify time: %d, crc: %u, total len: %d",
              seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_, finfo.size_, finfo.usize_,
              finfo.offset_, finfo.create_time_, finfo.modify_time_, finfo.crc_,
              data_finfo->id_, data_finfo->flag_, data_finfo->size_, data_finfo->usize_,
              data_finfo->offset_, data_finfo->create_time_, data_finfo->modify_time_,
              data_finfo->crc_, src_content_buf_.getDataLen()
              );
        }

        continue;
      }

      uint32_t data_crc = 0;
      data_crc = Func::crc(data_crc, src_content_buf_.getData() + finfo.offset_ + FILEINFO_SIZE, finfo.size_);
      if (data_crc != finfo.crc_)
      {
        TBSYS_LOG(ERROR, "file crc conflict, serious error. blockid: %u, fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, usize: %d, offset: %d, create time: %d, modify time: %d, crc: %u, data crc: %u",
            seg_data_.seg_info_.block_id_, finfo.id_, finfo.flag_, finfo.size_, finfo.usize_,
            finfo.offset_, finfo.create_time_, finfo.modify_time_, finfo.crc_, data_crc);
        continue;
      }

      FileInfo new_info;
      new_info.id_ = finfo.id_;
      new_info.offset_ = dest_content_buf_.getDataLen();
      new_info.size_ = finfo.size_ + FILEINFO_SIZE;
      new_info.usize_ = new_info.size_;
      new_info.modify_time_ = finfo.modify_time_;
      new_info.create_time_ = finfo.create_time_;
      // maybe have concealed file
      new_info.flag_ = finfo.flag_;
      new_info.crc_ = finfo.crc_;


      // write to dest buf
      dest_content_buf_.writeBytes(&new_info, FILEINFO_SIZE);
      dest_content_buf_.writeBytes(src_content_buf_.getData() + finfo.offset_ + FILEINFO_SIZE, finfo.size_);

      RawMeta dest_meta;
      dest_meta.set_file_id(new_info.id_);
      dest_meta.set_offset(new_info.offset_);
      dest_meta.set_size(new_info.size_);
      dest_raw_meta_.push_back(dest_meta);
    }
  }

  // set block info
  dest_block_info_.block_id_ = seg_data_.seg_info_.block_id_;
  dest_block_info_.version_ = dest_raw_meta_.size();
  dest_block_info_.file_count_ = dest_raw_meta_.size();
  dest_block_info_.size_ = dest_content_buf_.getDataLen();
  dest_block_info_.del_file_count_ = 0;
  dest_block_info_.del_size_ = 0;
  dest_block_info_.seq_no_ = dest_raw_meta_.size();

  return ret;
}

int TranBlock::check_dest_blk()
{
  SegmentData dest_seg_data;
  int ret = dest_session_->get_block_info(dest_seg_data, T_READ);
  VUINT64 dest_ds = dest_seg_data.ds_;
  if (TFS_SUCCESS == ret)
  {
    int32_t ds_size = static_cast<int32_t>(dest_ds.size());
    if (ds_size > 0)
    {
      TBSYS_LOG(ERROR, "block exists in dest cluster. block list size is %d, blockid: %u", ds_size, seg_data_.seg_info_.block_id_);
      int i = 0;
      for (i = 0; i < ds_size; i++)
      {
        if ((ret = rm_block_from_ns(dest_ds[i])) != TFS_SUCCESS)
        {
          break;
        }
        if ((ret = rm_block_from_ds(dest_ds[i])) != TFS_SUCCESS)
        {
          break;
        }
      }
      rm_block_from_ns(dest_ds[0]);
    }
  }
  else
  {
    ret = TFS_SUCCESS;
  }
  return ret;
}

int TranBlock::write_data()
{
  int block_len = dest_content_buf_.getDataLen();
  int cur_write_offset = 0;
  int cur_len = 0;
  int ret = TFS_SUCCESS;

  while (block_len > 0)
  {
    int64_t remainder_retrys = RETRY_TIMES;
    while (remainder_retrys > 0)
    {
      cur_len = std::min(static_cast<int64_t>(block_len), TRAN_BUFFER_SIZE);

      WriteRawDataMessage req_wrd_msg;
      req_wrd_msg.set_block_id(seg_data_.seg_info_.block_id_);
      req_wrd_msg.set_offset(cur_write_offset);
      req_wrd_msg.set_length(cur_len);
      req_wrd_msg.set_data(dest_content_buf_.getData());

      //new block
      if (0 == cur_write_offset)
      {
        req_wrd_msg.set_new_block(1);
      }

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp = NULL;
      if(TFS_SUCCESS != send_msg_to_server(dest_ds_id_, client, &req_wrd_msg, rsp))
      {
        rsp = NULL;
      }
      if (NULL == rsp)
      {
        TBSYS_LOG(ERROR, "write data failed, blockid: %u, dest_ds: %s, cur_write_offset: %d, cur_len: %d, ret: %d",
            seg_data_.seg_info_.block_id_, tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), cur_write_offset, cur_len, ret);
        ret = TFS_ERROR;
      }
      else
      {
        if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          if (STATUS_MESSAGE_OK != sm->get_status())
          {
            TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %u, offset: %d, ret: %d",
                tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), seg_data_.seg_info_.block_id_, cur_write_offset, sm->get_status());
            ret = TFS_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %u, offset: %d, unkonw msg type",
              tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), seg_data_.seg_info_.block_id_, cur_write_offset);
          ret = TFS_ERROR;
        }

        dest_content_buf_.drainData(cur_len);
        block_len = dest_content_buf_.getDataLen();
        cur_write_offset += cur_len;
        NewClientManager::get_instance().destroy_client(client);

        if (NULL == rsp)
        {
          --remainder_retrys;
          continue;
        }
        else
        {
          if (cur_len != TRAN_BUFFER_SIZE)
          {
            // quit
            block_len = 0;
          }
          TBSYS_LOG(INFO, "write raw data to ds: %s successful, blockid: %u",
              tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), seg_data_.seg_info_.block_id_);
          break;
        }
      }
    }

    if (TFS_SUCCESS != ret)
    {
      break;
    }
  }

  return ret;
}

int TranBlock::write_index()
{
  int ret = TFS_SUCCESS;
  WriteInfoBatchMessage req_wib_msg;
  req_wib_msg.set_block_id(seg_data_.seg_info_.block_id_);
  req_wib_msg.set_offset(0);
  req_wib_msg.set_length(dest_raw_meta_.size());
  req_wib_msg.set_raw_meta_list(&dest_raw_meta_);
  req_wib_msg.set_block_info(&dest_block_info_);
  req_wib_msg.set_cluster(COPY_BETWEEN_CLUSTER);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if(TFS_SUCCESS != send_msg_to_server(dest_ds_id_, client, &req_wib_msg, rsp))
  {
    rsp = NULL;
  }

  if (NULL == rsp)
  {
    TBSYS_LOG(ERROR, "write index failed, blockid: %u, length: %zd, ret: %d",
        seg_data_.seg_info_.block_id_, dest_raw_meta_.size(), ret);
    ret = TFS_ERROR;
  }
  else
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK != sm->get_status())
      {
        TBSYS_LOG(ERROR, "write raw index to ds: %s fail, blockid: %u, file count: %d, ret: %d",
            tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), seg_data_.seg_info_.block_id_, static_cast<int32_t>(dest_raw_meta_.size()), sm->get_status());
        ret = TFS_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "write raw index to ds: %s fail, blockid: %u, file count: %d, unkonw msg type",
          tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), seg_data_.seg_info_.block_id_, static_cast<int32_t>(dest_raw_meta_.size()));
      ret = TFS_ERROR;
    }
  }
  NewClientManager::get_instance().destroy_client(client);

  return ret;
}

int TranBlock::check_integrity()
{
  char data[MAX_READ_SIZE];
  int ret = TFS_SUCCESS;
  RawMetaVecIter vit = dest_raw_meta_.begin();
  if ((ret = TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "TfsClientImpl::Instance().initialize error");
  }
  else
  {
    for ( ; vit != dest_raw_meta_.end(); ++vit)
    {
      int fd = 0;
      FSName fname_helper;
      fname_helper.set_block_id(seg_data_.seg_info_.block_id_);
      fname_helper.set_file_id(vit->get_file_id());
      if (concealed_files_.find(vit->get_file_id()) != concealed_files_.end())
      {
        TBSYS_LOG(INFO, "file concealed, check integrity skip it. blockid: %u, fileid: %"PRI64_PREFIX"u",
            seg_data_.seg_info_.block_id_, vit->get_file_id());
        continue;
      }
      else if ((fd = TfsClientImpl::Instance()->open(fname_helper.get_name(), NULL, dest_ns_addr_.c_str(), T_READ)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "read data from dest fail, blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
            seg_data_.seg_info_.block_id_, vit->get_file_id(), ret);
      }
      else
      {
        TfsFileStat dest_info;
        uint32_t crc = 0;
        int32_t total_size = 0;
        data[0] = '\0';
        while (true)
        {
          int32_t read_len = TfsClientImpl::Instance()->readv2(fd, data, MAX_READ_SIZE, &dest_info);
          if (read_len < 0)
          {
            TBSYS_LOG(ERROR, "read data from dest fail, blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                 seg_data_.seg_info_.block_id_, vit->get_file_id(), read_len);
            ret = TFS_ERROR;
            break;
          }
          if (read_len == 0)
          {
            break;
          }

          total_size += read_len;
          crc = Func::crc(crc, data, read_len);
          if (read_len < MAX_READ_SIZE)
          {
            break;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          //FindFileId::set_file_id(vit->get_file_id());
          FileInfoSetIter fit = find_if(file_set_.begin(), file_set_.end(), FindFileId(vit->get_file_id()));
          if (fit == file_set_.end())
          {
            TBSYS_LOG(ERROR, "read file is not in src file list, fatal error. blockid: %u, fileid: %"PRI64_PREFIX"u",
                seg_data_.seg_info_.block_id_, vit->get_file_id());
            ret = TFS_ERROR;
          }
          else
          {
            if (crc != (*fit).crc_ || total_size != (*fit).size_ || crc != dest_info.crc_ || total_size != dest_info.size_)
            {
              TBSYS_LOG(ERROR, "blockid: %u, meta crc: %u, size: %d, data crc: %u, size: %d, dest crc: %u, size: %"PRI64_PREFIX"d\n",
                 seg_data_.seg_info_.block_id_, (*fit).crc_, (*fit).size_, crc, total_size, dest_info.crc_, dest_info.size_);
              ret = TFS_ERROR;
            }
          }
        }
      }
      TfsClientImpl::Instance()->close(fd);

      if (TFS_SUCCESS != ret)
      {
        atomic_inc(&stat_param_.copy_failure_);
      }
      else
      {
        atomic_inc(&stat_param_.copy_success_);
      }
    }

    TBSYS_LOG(INFO, "blockid: %u check integrity, total: %"PRI64_PREFIX"d, success: %d, fail: %d",
        seg_data_.seg_info_.block_id_, stat_param_.total_, atomic_read(&stat_param_.copy_success_), atomic_read(&stat_param_.copy_failure_));
    if (atomic_read(&stat_param_.copy_failure_) != 0)
    {
      ret = TFS_ERROR;
    }
  }

  return ret;
}
int TranBlock::rm_block_from_ns(uint64_t ds_id)
{
  int ret = TFS_SUCCESS;
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_value1(ds_id);
  req_cc_msg.set_value3(seg_data_.seg_info_.block_id_);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if((ret = send_msg_to_server(Func::get_host_ip(dest_ns_addr_.c_str()), client, &req_cc_msg, rsp)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "send remove block from ns command failed. block_id: %u, ret: %d", seg_data_.seg_info_.block_id_, ret);
    rsp = NULL;
  }
  else
  {
    if (rsp != NULL && STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == sm->get_status())
      {
        TBSYS_LOG(INFO, "remove block from ns success, ds_addr: %s, blockid: %u",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_);
      }
      else
      {
        TBSYS_LOG(ERROR, "remove block from ns fail, ds_addr: %s, blockid: %u, ret: %d",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_, sm->get_status());
        ret = TFS_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "remove block from ns: %s fail, blockid: %u, unkonw msg type",
          tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_);
      ret = TFS_ERROR;
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}
int TranBlock::rm_block_from_ds(uint64_t ds_id)
{
  int ret = TFS_SUCCESS;
  RemoveBlockMessage req_rb_msg;
  req_rb_msg.set(seg_data_.seg_info_.block_id_);
  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if((ret = send_msg_to_server(ds_id, client, &req_rb_msg, rsp)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "send remove block from ds command failed. block_id: %u, ret: %d", seg_data_.seg_info_.block_id_, ret);
    rsp = NULL;
  }
  else
  {
    if (rsp != NULL && STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == sm->get_status())
      {
        TBSYS_LOG(INFO, "remove block from dest ds success, ds_addr: %s, blockid: %u",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_);
      }
      else
      {
        TBSYS_LOG(ERROR, "remove block from dest ds fail, ds_addr: %s, blockid: %u, ret: %d",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_, sm->get_status());
        ret = TFS_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %u, unkonw msg type",
          tbsys::CNetUtil::addrToString(ds_id).c_str(), seg_data_.seg_info_.block_id_);
      ret = TFS_ERROR;
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}
