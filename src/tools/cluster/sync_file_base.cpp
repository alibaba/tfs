/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_base.cpp 1542 2012-06-27 02:02:37Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "sync_file_base.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

SyncFileBase::SyncFileBase() : src_ns_addr_(), dest_ns_addr_()
{
}

SyncFileBase::~SyncFileBase()
{
}

void SyncFileBase::initialize(const string& src_ns_addr, const string& dest_ns_addr)
{
  src_ns_addr_ = src_ns_addr;
  dest_ns_addr_ = dest_ns_addr;
}

int SyncFileBase::cmp_file_info(const uint32_t block_id, const FileInfo& source_file_info, SyncAction& sync_action, const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;
  TfsFileStat source_buf;
  memset(&source_buf, 0, sizeof(source_buf));

  fileinfo_to_filestat(source_file_info, source_buf);
  FSName fsname(block_id, source_buf.file_id_, TfsClientImpl::Instance()->get_cluster_id(src_ns_addr_.c_str()));
  string file_name = string(fsname.get_name());

  cmp_file_info_ex(file_name, source_buf, sync_action, force, modify_time);
  TBSYS_LOG(INFO, "cmp file finished. filename: %s, action_set: %s, ret: %d", file_name.c_str(), sync_action.dump().c_str(), ret);
  return ret;
}

int SyncFileBase::cmp_file_info(const string& file_name, SyncAction& sync_action, const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;
  TfsFileStat source_buf;
  memset(&source_buf, 0, sizeof(source_buf));

  ret = get_file_info(src_ns_addr_, file_name, source_buf);
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get source file info (%s) failed, ret: %d", file_name.c_str(), ret);
  }
  else
  {
    cmp_file_info_ex(file_name, source_buf, sync_action, force, modify_time);
  }
  TBSYS_LOG(INFO, "cmp file finished. filename: %s, action_set: %s, ret: %d", file_name.c_str(), sync_action.dump().c_str(), ret);
  return ret;
}

int SyncFileBase::do_action(const string& file_name, const SyncAction& sync_action)
{
  int ret = TFS_SUCCESS;
  const ACTION_VEC& action_vec = sync_action.action_;
  ACTION_VEC_ITER iter = action_vec.begin();
  int32_t index = 0;
  for (; iter != action_vec.end(); iter++)
  {
    ActionInfo action = (*iter);
    ret = do_action_ex(file_name, action);
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(INFO, "tfs file(%s) do (%d)th action(%d) success", file_name.c_str(), index, action);
      index++;
    }
    else
    {
      TBSYS_LOG(ERROR, "tfs file(%s) do (%d)th action(%d) failed, ret: %d", file_name.c_str(), index, action, ret);
      break;
    }
  }
  if ((index > sync_action.trigger_index_) && (index <= sync_action.force_index_))
  {
    int tmp_ret = do_action_ex(file_name, action_vec[sync_action.force_index_]);
    TBSYS_LOG(ERROR, "former operation error occures, file(%s) must do force action(%d), ret: %d",
        file_name.c_str(), action_vec[sync_action.force_index_], tmp_ret);
    ret = TFS_ERROR;
  }
  TBSYS_LOG(INFO, "do action finished. file_name: %s", file_name.c_str());
  return ret;
}

void SyncFileBase::fileinfo_to_filestat(const FileInfo& file_info, TfsFileStat& buf)
{
  buf.file_id_ = file_info.id_;
  buf.offset_ = file_info.offset_;
  buf.size_ = file_info.size_;
  buf.usize_ = file_info.usize_;
  buf.modify_time_ = file_info.modify_time_;
  buf.create_time_ = file_info.create_time_;
  buf.flag_ = file_info.flag_;
  buf.crc_ = file_info.crc_;
}

int SyncFileBase::get_file_info(const string& tfs_client, const string& file_name, TfsFileStat& buf)
{
  int ret = TFS_SUCCESS;

  int fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, tfs_client.c_str(), T_READ);
  if (fd < 0)
  {
    ret = TFS_ERROR;
    TBSYS_LOG(WARN, "open file(%s) failed, ret: %d", file_name.c_str(), ret);
  }
  else
  {
    ret = TfsClientImpl::Instance()->fstat(fd, &buf, FORCE_STAT);
    if (ret != TFS_SUCCESS)
    {
      TBSYS_LOG(WARN, "stat file(%s) failed, ret: %d", file_name.c_str(), ret);
    }
    TfsClientImpl::Instance()->close(fd);
  }
  return ret;
}

int SyncFileBase::cmp_file_info_ex(const string& file_name, const TfsFileStat& source_buf, SyncAction& sync_action, const bool force, const int32_t modify_time)
{
  int ret = TFS_ERROR;

  TfsFileStat dest_buf;
  memset(&dest_buf, 0, sizeof(dest_buf));

  ret = get_file_info(dest_ns_addr_, file_name, dest_buf);
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(WARN, "get dest file info failed. filename: %s, ret: %d", file_name.c_str(), ret);
  }

  TBSYS_LOG(INFO, "file(%s): flag--(%d -> %d), crc--(%u -> %u), size--(%"PRI64_PREFIX"d -> %"PRI64_PREFIX"d)",
      file_name.c_str(), source_buf.flag_, ((ret == TFS_SUCCESS)? dest_buf.flag_:-1), source_buf.crc_, dest_buf.crc_, source_buf.size_, dest_buf.size_);

  // 1. dest file exists and is new file, just skip.
  if (ret == TFS_SUCCESS && dest_buf.modify_time_ > modify_time)
  {
    TBSYS_LOG(WARN, "dest file(%s) has been modifyed!!! %d->%d", file_name.c_str(), dest_buf.modify_time_, modify_time);
  }
  // 2. source file exists, dest file is not exist or diff from source, rewrite file.
  else if (ret != TFS_SUCCESS || ((dest_buf.size_ != source_buf.size_) || (dest_buf.crc_ != source_buf.crc_))) // dest file exist
  {
    if (ret == TFS_SUCCESS && (! force))
    {
      TBSYS_LOG(WARN, "crc conflict!! source crc: %d -> dest crc: %d, force: %d", source_buf.crc_, dest_buf.crc_, force);
    }
    else
    {
      // if source file is normal, just rewrite it
      if (source_buf.flag_ == 0)
      {
        sync_action.push_back(WRITE_ACTION);
      }
      // if source file has been hided, reveal it and then rewrite
      else if (source_buf.flag_ == 4)
      {
        sync_action.push_back(UNHIDE_SOURCE);
        sync_action.push_back(WRITE_ACTION);
        sync_action.push_back(HIDE_SOURCE);
        sync_action.push_back(HIDE_DEST);
        sync_action.trigger_index_ = 0;
        sync_action.force_index_ = 2;
      }
      else if (source_buf.flag_ == 1 && force)
      {
        sync_action.push_back(UNDELE_SOURCE);
        sync_action.push_back(WRITE_ACTION);
        sync_action.push_back(DELETE_SOURCE);
        sync_action.push_back(DELETE_DEST);
        sync_action.trigger_index_ = 0;
        sync_action.force_index_ = 2;
      }
    }
  }
  // 3. source file is the same to dest file, but in diff stat
  else if (source_buf.flag_ != dest_buf.flag_)
  {
    TBSYS_LOG(WARN, "file info flag conflict!! source flag: %d -> dest flag: %d)", source_buf.flag_, dest_buf.flag_);
    change_stat(source_buf.flag_, dest_buf.flag_, sync_action);
  }
  return TFS_SUCCESS;
}

void SyncFileBase::change_stat(const int32_t source_flag, const int32_t dest_flag, SyncAction& sync_action)
{
  ACTION_VEC action;
  int32_t tmp_flag = dest_flag;
  if (source_flag != dest_flag)
  {
    if ((tmp_flag & DELETE_FLAG) != 0)
    {
      sync_action.push_back(UNDELE_DEST);
      tmp_flag &= ~DELETE_FLAG;
    }

    if ((source_flag & HIDE_FLAG) != (tmp_flag & HIDE_FLAG))
    {
      if ((source_flag & HIDE_FLAG) != 0)
      {
        sync_action.push_back(HIDE_DEST);
      }
      else
      {
        sync_action.push_back(UNHIDE_DEST);
      }
    }

    if ((source_flag & DELETE_FLAG) != (tmp_flag & DELETE_FLAG))
    {
      if ((source_flag & DELETE_FLAG) != 0)
      {
        sync_action.push_back(DELETE_DEST);
      }
      else
      {
        sync_action.push_back(UNDELE_DEST);
      }
    }
  }
}

int SyncFileBase::do_action_ex(const string& file_name, const ActionInfo& action)
{
  int64_t file_size = 0;
  int ret = TFS_SUCCESS;
  switch (action)
  {
    case WRITE_ACTION:
      ret = copy_file(file_name);
      break;
    case HIDE_SOURCE:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, src_ns_addr_.c_str(), CONCEAL);
      usleep(20000);
      break;
    case DELETE_SOURCE:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, src_ns_addr_.c_str(), DELETE);
      usleep(20000);
      break;
    case UNHIDE_SOURCE:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, src_ns_addr_.c_str(), REVEAL);
      usleep(20000);
      break;
    case UNDELE_SOURCE:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, src_ns_addr_.c_str(), UNDELETE);
      usleep(20000);
      break;
    case HIDE_DEST:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr_.c_str(), CONCEAL);
      usleep(20000);
      break;
    case DELETE_DEST:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr_.c_str(), DELETE);
      usleep(20000);
      break;
    case UNHIDE_DEST:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr_.c_str(), REVEAL);
      usleep(20000);
      break;
    case UNDELE_DEST:
      ret = TfsClientImpl::Instance()->unlink(file_size, file_name.c_str(), NULL, dest_ns_addr_.c_str(), UNDELETE);
      usleep(20000);
      break;
    default:
      break;
  }
  return ret;
}

int SyncFileBase::copy_file(const string& file_name)
{
  int ret = TFS_SUCCESS;
  char data[MAX_READ_DATA_SIZE];
  int32_t rlen = 0;

  int source_fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, src_ns_addr_.c_str(), T_READ);
  if (source_fd < 0)
  {
    TBSYS_LOG(ERROR, "open source tfsfile fail when copy file, filename: %s", file_name.c_str());
    ret = TFS_ERROR;
  }
  int dest_fd = TfsClientImpl::Instance()->open(file_name.c_str(), NULL, dest_ns_addr_.c_str(), T_WRITE | T_NEWBLK);
  if (dest_fd < 0)
  {
    TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, filename: %s", file_name.c_str());
    ret = TFS_ERROR;
  }
  if (TFS_SUCCESS == ret)
  {
    if ((ret = TfsClientImpl::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
    }
    else
    {
      for (;;)
      {
        rlen = TfsClientImpl::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);
        if (rlen < 0)
        {
          TBSYS_LOG(ERROR, "read tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
          ret = TFS_ERROR;
          break;
        }
        if (rlen == 0)
        {
          break;
        }

        if (TfsClientImpl::Instance()->write(dest_fd, data, rlen) != rlen)
        {
          TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d", file_name.c_str(), rlen);
          ret = TFS_ERROR;
          break;
        }

        if (rlen < MAX_READ_DATA_SIZE)
        {
          break;
        }
      }
    }
  }
  if (source_fd > 0)
  {
    TfsClientImpl::Instance()->close(source_fd);
  }
  if (dest_fd > 0)
  {
    if (TFS_SUCCESS != TfsClientImpl::Instance()->close(dest_fd))
    {
      ret = TFS_ERROR;
      TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s", file_name.c_str());
    }
  }
  return ret;
}

