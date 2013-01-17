/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_backup.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "tbsys.h"

#include "common/func.h"
#include "common/directory_op.h"
#include "new_client/fsname.h"
#include "logic_block.h"
#include "blockfile_manager.h"
#include "sync_base.h"
#include "sync_backup.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;

    SyncBackup::SyncBackup() : tfs_client_(NULL)
    {
      src_addr_[0] = '\0';
      dest_addr_[0] = '\0';
    }

    SyncBackup::~SyncBackup()
    {
    }

    int SyncBackup::do_sync(const SyncData *sf)
    {
      int ret = TFS_ERROR;
      switch (sf->cmd_)
      {
      case OPLOG_INSERT:
        ret = copy_file(sf->block_id_, sf->file_id_);
        break;
      case OPLOG_REMOVE:
        ret = remove_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      case OPLOG_RENAME:
        ret = rename_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      }
      return ret;
    }

    int SyncBackup::copy_file(const uint32_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::remove_file(const uint32_t, const uint64_t, const int32_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::rename_file(const uint32_t, const uint64_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::remote_copy_file(const uint32_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    TfsMirrorBackup::TfsMirrorBackup(SyncBase& sync_base, const char* src_addr, const char* dest_addr):
        sync_base_(sync_base), do_sync_mirror_thread_(0)
    {
      if (NULL != src_addr &&
          strlen(src_addr) > 0 &&
          NULL != dest_addr &&
          strlen(dest_addr) > 0)
      {
        strcpy(src_addr_, src_addr);
        strcpy(dest_addr_, dest_addr);
      }
    }

    TfsMirrorBackup::~TfsMirrorBackup()
    {
    }

    bool TfsMirrorBackup::init()
    {
      bool ret = (strlen(src_addr_) > 0 && strlen(dest_addr_) > 0) ? true : false;
      if (ret)
      {
        tfs_client_ = TfsClientImpl::Instance();
        ret =
          tfs_client_->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false) == TFS_SUCCESS ?
          true : false;
        TBSYS_LOG(INFO, "TfsSyncMirror init. source ns addr: %s, destination ns addr: %s", src_addr_, dest_addr_);
        if (do_sync_mirror_thread_ == 0)
          do_sync_mirror_thread_ = new DoSyncMirrorThreadHelper(sync_base_);
      }
      return ret;
    }

    void TfsMirrorBackup::destroy()
    {
      if (0 != do_sync_mirror_thread_)
      {
        do_sync_mirror_thread_->join();
        do_sync_mirror_thread_ = 0;
      }
    }

    int TfsMirrorBackup::do_sync(const SyncData *sf)
    {
      int ret = TFS_ERROR;
      switch (sf->cmd_)
      {
      case OPLOG_INSERT:
        ret = copy_file(sf->block_id_, sf->file_id_);
        break;
      case OPLOG_REMOVE:
        ret = remove_file(sf->block_id_, sf->file_id_, static_cast<TfsUnlinkType>(sf->old_file_id_));
        break;
      case OPLOG_RENAME:
        ret = rename_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      }

      for (int i = 0; i < SYSPARAM_DATASERVER.max_sync_retry_count_ && TFS_SUCCESS != ret; i++)
      {
        if (i > 0)
        {
          int wait_time = i * i;
          if (wait_time > SYSPARAM_DATASERVER.max_sync_retry_interval_)
          {
            wait_time = SYSPARAM_DATASERVER.max_sync_retry_interval_;
          }
          sleep(wait_time);
        }
        ret = sync_stat(sf->block_id_, sf->file_id_);
        TBSYS_LOG(INFO, "sync_stat block_id: %u, file_id: %"PRI64_PREFIX"u, action: %d, ret: %d, retry_count: %d",
          sf->block_id_, sf->file_id_, sf->cmd_, ret, i + 1);
      }

      return ret;
    }

    int TfsMirrorBackup::remote_copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int dest_fd = -1;
        FSName fsname(block_id, file_id);
        int src_fd = tfs_client_->open(fsname.get_name(), NULL, src_addr_, T_READ | T_FORCE);
        if (src_fd < 0)
        {
          // if the block is missing, need not sync
          if (file_not_exist(ret))
          {
            ret = TFS_SUCCESS;
            TBSYS_LOG(DEBUG, "tfs file: %s, blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                      fsname.get_name(), block_id, file_id, src_addr_, ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "%s open src read fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                      fsname.get_name(), block_id, file_id, src_fd);
            ret = src_fd;
          }
        }
        else // open src file success
        {
          TfsFileStat file_stat;
          ret = tfs_client_->fstat(src_fd, &file_stat, FORCE_STAT);
          if (TFS_SUCCESS != ret) // src file stat fail
          {
            // if block not found or the file is deleted, need not sync
            if (file_not_exist(ret))
            {
              TBSYS_LOG(ERROR, "%s stat src file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                        fsname.get_name(), block_id, file_id, ret);
            }
          }
          else // src file stat ok
          {
            dest_fd = tfs_client_->open(fsname.get_name(), NULL, dest_addr_, T_WRITE|T_NEWBLK);
            if (dest_fd < 0)
            {
              TBSYS_LOG(ERROR, "%s open dest write fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                      fsname.get_name(), block_id, file_id, dest_fd);
              ret = TFS_ERROR;  // maybe ignored if not reset error type
            }
            else // source file stat ok, destination file open ok
            {
              tfs_client_->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
              ret = file_stat.size_ <= 0 ? TFS_ERROR : TFS_SUCCESS;
              if (TFS_SUCCESS == ret)
              {
                char data[MAX_READ_SIZE];
                int64_t total_length = 0;
                uint32_t crc = 0;
                int32_t length = 0;
                int32_t write_length = 0;
                while (total_length < file_stat.size_
                      && TFS_SUCCESS == ret)
                {
                  length = tfs_client_->read(src_fd, data, MAX_READ_SIZE);
                  if (length <= 0)
                  {
                    ret = EXIT_READ_FILE_ERROR;
                    TBSYS_LOG(ERROR, "%s read src tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, length: %d",
                       fsname.get_name(), block_id, file_id, length);
                  }
                  else // read src file success
                  {
                    write_length = tfs_client_->write(dest_fd, data, length);
                    if (write_length != length)
                    {
                      ret = EXIT_WRITE_FILE_ERROR;
                      TBSYS_LOG(ERROR, "%s write dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, length: %d, write_length: %d",
                          fsname.get_name(), block_id, file_id, length, write_length);
                    }
                    else
                    {
                      crc = Func::crc(crc, data, length);
                      total_length += length;
                    }
                  } // read src file success
                } // while (total_length < file_stat.st_size && TFS_SUCCESS == ret)

                // write successful & check file size & check crc
                if (TFS_SUCCESS == ret)
                {
                  ret = total_length == file_stat.size_ ? TFS_SUCCESS : EXIT_SYNC_FILE_ERROR;//check file size
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "file size error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d",
                        fsname.get_name(), block_id, file_id, crc, file_stat.crc_, total_length, file_stat.size_);
                  }
                  else
                  {
                    ret = crc != file_stat.crc_ ? EXIT_CHECK_CRC_ERROR : TFS_SUCCESS;//check crc
                    if (TFS_SUCCESS != ret)
                    {
                      TBSYS_LOG(ERROR, "crc error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d",
                          fsname.get_name(), block_id, file_id, crc, file_stat.crc_, total_length, file_stat.size_);
                    }
                  }
                } // write successful & check file size & check crc
              } // source file stat ok, destination file open ok, size normal
            } // source file stat ok, destination file open ok
          } // source file stat ok
        } // src file open ok

        if (src_fd > 0)
        {
          // close source file
          tfs_client_->close(src_fd);

          if (dest_fd > 0)
          {
            // close destination file anyway
            int tmp_ret = tfs_client_->close(dest_fd);
            if (tmp_ret != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "%s close dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u. ret: %d",
                  fsname.get_name(), block_id, file_id, tmp_ret);
              ret = tmp_ret;
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "tfs remote copy file %s success to dest: %s. blockid: %u, fileid: %" PRI64_PREFIX "u",
              fsname.get_name(), dest_addr_, block_id, file_id);
        }
        else
        {
          // if the file is deleted, need not sync
          if (file_not_exist(ret))
          {
            ret = TFS_SUCCESS;
            TBSYS_LOG(DEBUG, "blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                block_id, file_id, src_addr_, ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "tfs remote copy file %s fail to dest: %s. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                fsname.get_name(), dest_addr_, block_id, file_id, ret);
          }
        }
      }
      return ret;
    }

    int TfsMirrorBackup::copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
      if (TFS_SUCCESS == ret)
      {
        LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
        if (NULL == logic_block)
        {
          return remote_copy_file(block_id, file_id);
        }

        FSName fsname(block_id, file_id);

        char data[MAX_READ_SIZE];
        uint32_t crc  = 0;
        int32_t offset = 0;
        int32_t write_length = 0;
        int32_t length = MAX_READ_SIZE;
        int64_t total_length = 0;
        FileInfo finfo;

        ret = logic_block->read_file(file_id, data, length, offset, READ_DATA_OPTION_FLAG_FORCE/*READ_DATA_OPTION_FLAG_NORMAL*/);//read first data & fileinfo
        if (TFS_SUCCESS != ret)
        {
          // if file is local deleted or not exists in local, need not sync
          if (file_not_exist(ret))
          {
            TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                block_id, file_id, offset, ret);
          }
        }
        else
        {
          if (length <= FILEINFO_SIZE)
          {
            ret = EXIT_READ_FILE_SIZE_ERROR;
            TBSYS_LOG(ERROR,
                "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, read len: %d < sizeof(FileInfo):%d, ret: %d",
                block_id, file_id, length, FILEINFO_SIZE, ret);
          }
          else
          {
            memcpy(&finfo, data, FILEINFO_SIZE);
            // open dest tfs file
            int dest_fd = tfs_client_->open(fsname.get_name(), NULL, dest_addr_, T_WRITE|T_NEWBLK);
            if (dest_fd < 0)
            {
              TBSYS_LOG(ERROR, "open dest tfsfile: %s(block_id: %u, file_id: %"PRI64_PREFIX"u) fail. ret: %d",
                        fsname.get_name(), fsname.get_block_id(), fsname.get_file_id(), dest_fd);
              ret = TFS_ERROR;
            }
            else
            {
              // set no sync flag avoid the data being sync in the backup cluster again
              tfs_client_->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
              write_length = tfs_client_->write(dest_fd, (data + FILEINFO_SIZE), (length - FILEINFO_SIZE));
              if (write_length != (length - FILEINFO_SIZE))
              {
                ret = EXIT_WRITE_FILE_ERROR;
                TBSYS_LOG(ERROR,
                        "write dest tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d <> %d, file size: %d",
                        block_id, file_id, write_length, (length - FILEINFO_SIZE), finfo.size_);
              }
              else
              {
                total_length = length - FILEINFO_SIZE;
                finfo.size_ -= FILEINFO_SIZE;
                offset += length;
                crc = Func::crc(crc, (data + FILEINFO_SIZE), (length - FILEINFO_SIZE));
              }

              if (TFS_SUCCESS == ret)
              {
                while (total_length < finfo.size_
                      && TFS_SUCCESS == ret)
                {
                  length = ((finfo.size_ - total_length) > MAX_READ_SIZE) ? MAX_READ_SIZE : finfo.size_ - total_length;
                  ret = logic_block->read_file(file_id, data, length, offset, READ_DATA_OPTION_FLAG_NORMAL);//read data
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                        block_id, file_id, offset, ret);
                  }
                  else
                  {
                    write_length = tfs_client_->write(dest_fd, data, length);
                    if (write_length != length )
                    {
                      ret = EXIT_WRITE_FILE_ERROR;
                      TBSYS_LOG(ERROR,
                          "write dest tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d <> %d, file size: %d",
                          block_id, file_id, write_length, length, finfo.size_);
                    }
                    else
                    {
                      total_length += length;
                      offset += length;
                      crc = Func::crc(crc, data, length);
                    }
                  }
                }
              }

              //write successful & check file size & check crc
              if (TFS_SUCCESS == ret)
              {
                ret = total_length == finfo.size_ ? TFS_SUCCESS : EXIT_SYNC_FILE_ERROR; // check file size
                if (TFS_SUCCESS != ret)
                {
                   TBSYS_LOG(ERROR, "file size error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %d",
                         fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_length, finfo.size_);
                }
                else
                {
                  ret = crc != finfo.crc_ ? EXIT_CHECK_CRC_ERROR : TFS_SUCCESS; // check crc
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "crc error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %d",
                            fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_length, finfo.size_);
                  }
                }
              }

              // close destination tfsfile anyway
              int32_t iret = tfs_client_->close(dest_fd);
              if (TFS_SUCCESS != iret)
              {
                TBSYS_LOG(ERROR, "close dest tfsfile fail, but write data %s . blockid: %u, fileid :%" PRI64_PREFIX "u",
                         TFS_SUCCESS == ret ? "successful": "fail",block_id, file_id);
              }
            }
          }
        }

        if (TFS_SUCCESS != ret)
        {
          // if file is local deleted or not exists, need not sync
          if (file_not_exist(ret))
          {
             ret = TFS_SUCCESS;
             TBSYS_LOG(DEBUG, "blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                       block_id, file_id, src_addr_, ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "tfs mirror copy file fail to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u, ret: %d",
                      dest_addr_, block_id, file_id, ret);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "tfs mirror copy file success to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u",
                    dest_addr_, block_id, file_id);
        }
      }

      return ret;
    }

    int TfsMirrorBackup::remove_file(const uint32_t block_id, const uint64_t file_id,
                                     const TfsUnlinkType action)
    {
      int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
      if (TFS_SUCCESS == ret)
      {
        FSName fsname(block_id, file_id);

        int64_t file_size = 0;
        ret = tfs_client_->unlink(file_size, fsname.get_name(), NULL, dest_addr_, action, TFS_FILE_NO_SYNC_LOG);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "tfs mirror remove file %s(block_id: %u, file_id: %"PRI64_PREFIX"u) fail to dest: %s.\
                    blockid: %u, fileid: %"PRI64_PREFIX"u, action: %d, ret: %d",
                    fsname.get_name(), fsname.get_block_id(), fsname.get_file_id(), dest_addr_, block_id, file_id, action, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "tfs mirror remove file success to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u, action: %d",
                    dest_addr_, block_id, file_id, action);
        }
      }

      return ret;
    }

    int TfsMirrorBackup::rename_file(const uint32_t block_id, const uint64_t file_id,
                                     const uint64_t old_file_id)
    {
      UNUSED(block_id);
      UNUSED(file_id);
      UNUSED(old_file_id);
      // FSName fsname(block_id, file_id);
      // int ret = tfs_client->rename(block_id, old_file_id, file_id);
      // if (TFS_SUCCESS != ret)
      // {
      //   TBSYS_LOG(ERROR, "unlink failure: %s\n", tfs_client->get_error_message());
      // }

      // TBSYS_LOG(
      //     INFO,
      //     "tfs mirror rename file. blockid: %d, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d.\n",
      //     block_id, file_id, old_file_id);
      // return ret;
      return TFS_SUCCESS;
    }

    void TfsMirrorBackup::DoSyncMirrorThreadHelper::run()
    {
      sync_base_.run_sync_mirror();
    }

    int TfsMirrorBackup::get_file_info(const char* nsip, const char* file_name, TfsFileStat& buf)
    {
      int ret = TFS_SUCCESS;

      int fd = tfs_client_->open(file_name, NULL, nsip, T_READ | T_FORCE);
      if (fd < 0)
      {
        ret = fd;
        TBSYS_LOG(WARN, "open file(%s) failed, ret: %d", file_name, ret);
      }
      else
      {
        ret = tfs_client_->fstat(fd, &buf, FORCE_STAT);
        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(WARN, "stat file(%s) failed, ret: %d", file_name, ret);
        }
        tfs_client_->close(fd);
      }
      return ret;
    }

    bool TfsMirrorBackup::file_not_exist(int ret)
    {
      return (EXIT_BLOCK_NOT_FOUND == ret) ||  // ns cannot find block
         (EXIT_NO_BLOCK == ret)            ||  // ns cannot find or create block
         (EXIT_NO_LOGICBLOCK_ERROR == ret) ||  // ds cannot find logic block
         (EXIT_META_NOT_FOUND_ERROR == ret);   // ds cannot find file in index
    }

    int TfsMirrorBackup::sync_stat(const uint32_t block_id, const uint64_t file_id)
    {
      int ret = TFS_SUCCESS;

      FSName fsname(block_id, file_id);
      TfsFileStat src_stat;
      TfsFileStat dest_stat;
      ret = get_file_info(src_addr_, fsname.get_name(), src_stat);
      if (file_not_exist(ret))   // not exsit in source, just ignore
      {
        ret = TFS_SUCCESS;
      }
      else if (TFS_SUCCESS == ret)
      {
        dest_stat.flag_ = 0;
        ret = get_file_info(dest_addr_, fsname.get_name(), dest_stat);
        if (file_not_exist(ret)) // not exsit in dest
        {
          ret = copy_file(block_id, file_id);
        }

        if (TFS_SUCCESS == ret)  // just sync stat;
        {
          if (src_stat.flag_ != dest_stat.flag_)
          {
            int64_t file_size = 0;
            int action = (src_stat.flag_ << 4);  // sync flag
            ret = tfs_client_->unlink(file_size, fsname.get_name(), NULL, dest_addr_,
                static_cast<TfsUnlinkType>(action), TFS_FILE_NO_SYNC_LOG);
          }
        }
      }

      return ret;
    }
  }
}
