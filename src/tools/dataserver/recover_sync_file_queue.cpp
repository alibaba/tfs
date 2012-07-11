/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: recover_sync_file_queue.cpp 553 2011-06-24 08:47:47Z duanfei@taobao.com $
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <signal.h>
#include <Monitor.h>
#include <Mutex.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/file_queue.h"
#include "common/directory_op.h"
#include "message/client_cmd_message.h"
#include "new_client/fsname.h"
#include "dataserver/version.h"
#include "dataserver/blockfile_manager.h"
#include "dataserver/sync_backup.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

char* g_conf_file = NULL;
TfsClientImpl* g_tfs_client = NULL;
string g_mirror_dir = "";
string g_server_index;
bool g_block_file_system_inited = false;
char g_src_addr[MAX_ADDRESS_LENGTH];
bool g_stop = false;
tbutil::Monitor<tbutil::Mutex> g_retry_wait_monitor;
tbutil::Mutex g_init_block_file_system_mutex;

struct ThreadParam
{
  int32_t thread_index_;
  char dest_addr_[MAX_ADDRESS_LENGTH];
};

int init_block_file_system()
{
  int ret = SYSPARAM_FILESYSPARAM.initialize(g_server_index);
  if (TFS_SUCCESS == ret)
  {
    cout << "mount name: " << SYSPARAM_FILESYSPARAM.mount_name_
      << " max mount size: " << SYSPARAM_FILESYSPARAM.max_mount_size_
      << " base fs type: " << SYSPARAM_FILESYSPARAM.base_fs_type_
      << " superblock reserve offset: " << SYSPARAM_FILESYSPARAM.super_block_reserve_offset_
      << " main block size: " << SYSPARAM_FILESYSPARAM.main_block_size_
      << " extend block size: " << SYSPARAM_FILESYSPARAM.extend_block_size_
      << " block ratio: " << SYSPARAM_FILESYSPARAM.block_type_ratio_
      << " file system version: " << SYSPARAM_FILESYSPARAM.file_system_version_
      << " hash slot ratio: " << SYSPARAM_FILESYSPARAM.hash_slot_ratio_
      << endl;

    // load super block & load block file, create logic block map associate stuff
    ret = BlockFileManager::get_instance()->bootstrap(SYSPARAM_FILESYSPARAM);
    if (TFS_SUCCESS == ret)
    {
      // query super block
      SuperBlock super_block;
      ret = BlockFileManager::get_instance()->query_super_block(super_block);
      if (TFS_SUCCESS == ret)
      {
        // display super block
        super_block.display();
        g_block_file_system_inited = true;
      }
      else
      {
        fprintf(stderr, "query superblock fail. ret: %d\n",ret);
      }
    }
    else
    {
      fprintf(stderr, "blockfile manager boot fail. ret: %d\n",ret);
    }

  }
  else
  {
    fprintf(stderr, "SysParam::loadFileSystemParam failed: %s\n", g_conf_file);
  }

  return ret;
}

int remote_copy_file(const char* dest_addr, const uint32_t block_id, const uint64_t file_id)
{
  if (NULL == dest_addr)
  {
    g_stop = true;
    return TFS_ERROR;
  }
  int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
  if (TFS_SUCCESS == ret)
  {
    FSName fsname(block_id, file_id);
    int dest_fd = -1;
    int src_fd = g_tfs_client->open(fsname.get_name(), NULL, g_src_addr, T_READ);
    if (src_fd <= 0)
    {
      // if the block is missing, need not sync
      if (EXIT_BLOCK_NOT_FOUND == src_fd)
      {
        ret = TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "tfs file: %s, blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                  fsname.get_name(), block_id, file_id, g_src_addr, ret);

      }
      else
      {
        TBSYS_LOG(DEBUG, "%s open src read fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                  fsname.get_name(), block_id, file_id, src_fd);
        ret = src_fd;
      }
    }
    else // open src file success
    {
      TfsFileStat file_stat;
      ret = g_tfs_client->fstat(src_fd, &file_stat);
      if (TFS_SUCCESS != ret)
      {
        // if block not found or the file is deleted, need not sync
        if (EXIT_NO_LOGICBLOCK_ERROR != ret &&
            EXIT_META_NOT_FOUND_ERROR != ret &&
            EXIT_FILE_STATUS_ERROR != ret)
        {
          TBSYS_LOG(DEBUG, "%s stat src file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                    fsname.get_name(), block_id, file_id, ret);
        }
      }
      else // src file stat ok
      {
        dest_fd = g_tfs_client-> open(fsname.get_name(), NULL, dest_addr, T_WRITE|T_NEWBLK);
        if (dest_fd <= 0)
        {
          TBSYS_LOG(DEBUG, "%s open dest write fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                  fsname.get_name(), block_id, file_id, dest_fd);
          ret = dest_fd;
        }
        else // source file stat ok, destination file open ok
        {
          g_tfs_client->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
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
              length = g_tfs_client->read(src_fd, data, MAX_READ_SIZE);
              if (length <= 0)
              {
                ret = EXIT_READ_FILE_ERROR;
                TBSYS_LOG(DEBUG, "%s read src tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, length: %d",
                   fsname.get_name(), block_id, file_id, length);
              }
              else
              {
                write_length = g_tfs_client->write(dest_fd, data, length);
                if (write_length != length)
                {
                  ret = EXIT_WRITE_FILE_ERROR;
                  TBSYS_LOG(DEBUG, "%s write dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, length: %d, write_length: %d",
                      fsname.get_name(), block_id, file_id, length, write_length);
                }
                else
                {
                  crc = Func::crc(crc, data, length);
                  total_length += length;
                }
              } // read src file success
            } // while (total_length < file_stat.st_size && TFS_SUCCESS == ret)

            //write successful & check file size & check crc
            if (TFS_SUCCESS == ret)
            {
              ret = total_length == file_stat.size_ ? TFS_SUCCESS : EXIT_SYNC_FILE_ERROR;//check file size
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(DEBUG, "file size error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d",
                    fsname.get_name(), block_id, file_id, crc, file_stat.crc_, total_length, file_stat.size_);
              }
              else
              {
                ret = crc != file_stat.crc_ ? EXIT_CHECK_CRC_ERROR : TFS_SUCCESS;//check crc
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(DEBUG, "crc error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %"PRI64_PREFIX"d <> %"PRI64_PREFIX"d",
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
      g_tfs_client->close(src_fd);
      if (dest_fd > 0)
      {
        // close destination file anyway
        int tmp_ret = g_tfs_client->close(dest_fd);
        if (tmp_ret != TFS_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "%s close dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u. ret: %d",
              fsname.get_name(), block_id, file_id, tmp_ret);
          ret = tmp_ret;
        }
      }
    }

    if (TFS_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "tfs remote copy file success: %s to dest: %s, blockid: %u, fileid: %" PRI64_PREFIX "u",
          fsname.get_name(), dest_addr, block_id, file_id);
    }
    else
    {
      // if block not found or the file is deleted, need not sync
      if (EXIT_NO_LOGICBLOCK_ERROR == ret ||
          EXIT_META_NOT_FOUND_ERROR == ret ||
          EXIT_FILE_STATUS_ERROR == ret)
      {
        ret = TFS_SUCCESS;
        TBSYS_LOG(DEBUG, "tfs file: %s, blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                  fsname.get_name(), block_id, file_id, g_src_addr, ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "tfs remote copy file fail: %s to dest: %s, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
            fsname.get_name(), dest_addr, block_id, file_id, ret);
      }
    }
  }
  return ret;
}

int copy_file(const char* dest_addr, const uint32_t block_id, const uint64_t file_id)
{
  if (NULL == dest_addr)
  {
    g_stop = true;
    return TFS_ERROR;
  }
  int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
  if (TFS_SUCCESS == ret)
  {
    // use the file in the src cluster as copy src
    ret = remote_copy_file(dest_addr, block_id, file_id);
    if (TFS_SUCCESS == ret)
      return TFS_SUCCESS;

    // if remote copy fail, and the local disk is available, then use local disk data as copy src
    g_init_block_file_system_mutex.lock();
    if (!g_block_file_system_inited)
    {
      ret = init_block_file_system();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "remote copy failed, and init block file system also failed.");
        return ret;
      }
    }
    g_init_block_file_system_mutex.unlock();

    // local copy available
    LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
    if (NULL == logic_block)
    {
      return TFS_ERROR;
    }

    char data[MAX_READ_SIZE];
    uint32_t crc  = 0;
    int32_t offset = 0;
    int32_t write_length = 0;
    int32_t length = MAX_READ_SIZE;
    int64_t total_length = 0;
    FSName fsname(block_id, file_id);
    FileInfo finfo;

    logic_block->rlock();
    ret = logic_block->read_file(file_id, data, length, offset, READ_DATA_OPTION_FLAG_FORCE);//read first data & fileinfo
    if (TFS_SUCCESS != ret)
    {
      // if file is local deleted or not exists, need not sync
      if (EXIT_FILE_INFO_ERROR != ret && EXIT_META_NOT_FOUND_ERROR != ret)
      {
        TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
            block_id, file_id, offset, ret);
      }
    }
    else
    {
      if (length < FILEINFO_SIZE)
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
        int dest_fd = g_tfs_client->open(fsname.get_name(), NULL, dest_addr, T_WRITE|T_NEWBLK);
        if (dest_fd <= 0)
        {
          TBSYS_LOG(ERROR, "open dest tfsfile: %s(block_id: %u, file_id: %"PRI64_PREFIX"u) fail. ret: %d",
                    fsname.get_name(), fsname.get_block_id(), fsname.get_file_id(), dest_fd);
          ret = TFS_ERROR;
        }
        else
        {
          // set no sync flag avoid the data being sync in the backup cluster again
          g_tfs_client->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
          write_length = g_tfs_client->write(dest_fd, (data + FILEINFO_SIZE), (length - FILEINFO_SIZE));
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
                write_length = g_tfs_client->write(dest_fd, data, length);
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
          int32_t iret = g_tfs_client->close(dest_fd);
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "close dest tfsfile fail, but write data %s . blockid: %u, fileid :%" PRI64_PREFIX "u",
                     TFS_SUCCESS == ret ? "successful": "fail",block_id, file_id);
          }
        }
      }
    }
    logic_block->rlock();
    if (TFS_SUCCESS != ret)
    {
      // if file is local deleted or not exists, need not sync
      if (EXIT_FILE_INFO_ERROR == ret || EXIT_META_NOT_FOUND_ERROR == ret)
      {
         ret = TFS_SUCCESS;
         TBSYS_LOG(DEBUG, "tfs file: %s, blockid: %u, fileid: %" PRI64_PREFIX "u not exists in src: %s, ret: %d, need not sync",
                   fsname.get_name(), block_id, file_id, g_src_addr, ret);

      }
      else
      {
        TBSYS_LOG(ERROR, "tfs mirror copy file fail to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u, ret: %d",
                  dest_addr, block_id, file_id, ret);
      }
    }
    else
    {
      TBSYS_LOG(INFO, "tfs mirror copy file success to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u",
                dest_addr, block_id, file_id);
    }
  }
  return ret;
}


int remove_file(const char* dest_addr, const uint32_t block_id,
                const uint64_t file_id, const TfsUnlinkType action)
{
  if (NULL == dest_addr)
  {
    g_stop = true;
    return TFS_ERROR;
  }
  int32_t ret = block_id > 0 ? TFS_SUCCESS : EXIT_BLOCKID_ZERO_ERROR;
  if (TFS_SUCCESS == ret)
  {
    FSName fsname(block_id, file_id);

    int64_t file_size = 0;
    ret = g_tfs_client->unlink(file_size, fsname.get_name(), NULL, dest_addr, action, TFS_FILE_NO_SYNC_LOG);
    if (TFS_SUCCESS != ret)
    {
      // if block not found or the action is done
      if (EXIT_NO_BLOCK == ret ||
          EXIT_META_NOT_FOUND_ERROR == ret)
      {
        if (CONCEAL == action)
        {
          ret = copy_file(dest_addr, block_id, file_id);
          if (TFS_SUCCESS == ret)
          {
            ret = g_tfs_client->unlink(file_size, fsname.get_name(), NULL, dest_addr, action, TFS_FILE_NO_SYNC_LOG);
          }
        }
        else if (REVEAL == action)
        {
          ret = copy_file(dest_addr, block_id, file_id);
        }
        else // DELETE or UNDELETE
        {
          ret = TFS_SUCCESS;
          TBSYS_LOG(DEBUG, "blockid: %u, fileid: %" PRI64_PREFIX "u not exists in dest: %s, ret: %d, need not sync",
                    block_id, file_id, dest_addr, ret);
        }
      }
      else if (EXIT_FILE_STATUS_ERROR == ret)
      {
        ret = TFS_SUCCESS;
      }
    }
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "tfs mirror remove file %s(block_id: %u, file_id: %"PRI64_PREFIX"u) fail to dest: %s.\
                blockid: %u, fileid: %"PRI64_PREFIX"u, action: %d, ret: %d",
                fsname.get_name(), fsname.get_block_id(), fsname.get_file_id(), dest_addr, block_id, file_id, action, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "tfs mirror remove file success to dest: %s. blockid: %d, fileid: %"PRI64_PREFIX"u, action: %d",
                dest_addr, block_id, file_id, action);
    }
  }

  return ret;
}

int rename_file(const char* dest_addr, const uint32_t block_id, const uint64_t file_id,
                                 const uint64_t old_file_id)
{
  UNUSED(dest_addr);
  UNUSED(block_id);
  UNUSED(file_id);
  UNUSED(old_file_id);
  return TFS_SUCCESS;
}

int do_sync(const char* dest_addr, const char* data, const int32_t len)
{
  if (NULL == dest_addr)
  {
    g_stop = true;
    return TFS_ERROR;
  }
  if (NULL == data || len != sizeof(SyncData))
  {
    TBSYS_LOG(WARN, "SYNC_ERROR: data null or len error, %d <> %zd", len, sizeof(SyncData));
    return TFS_ERROR;
  }
  SyncData* sf = reinterpret_cast<SyncData*>(const_cast<char*>(data));

  int ret = TFS_ERROR;
  int32_t retry_count = 0;
  int32_t retry_time = 0;
  // endless retry if fail
  do
  {
    switch (sf->cmd_)
    {
    case OPLOG_INSERT:
      ret = copy_file(dest_addr, sf->block_id_, sf->file_id_);
      break;
    case OPLOG_REMOVE:
      ret = remove_file(dest_addr, sf->block_id_, sf->file_id_, static_cast<TfsUnlinkType>(sf->old_file_id_));
      break;
    case OPLOG_RENAME:
      ret = rename_file(dest_addr, sf->block_id_, sf->file_id_, sf->old_file_id_);
      break;
    }
    if (TFS_SUCCESS != ret)
    {
      // for invalid block, not retry; also to the already removed file
      if (EXIT_BLOCKID_ZERO_ERROR == ret)
        break;
      TBSYS_LOG(WARN, "sync error! cmd: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d, retry_count: %d",
          sf->cmd_, sf->block_id_, sf->file_id_, sf->old_file_id_, ret, retry_count);
      retry_count++;
      int32_t wait_second = retry_count * retry_count; // 1, 4, 9, ...
      wait_second -= (time(NULL) - retry_time);
      if (wait_second > 0)
      {
        g_retry_wait_monitor.lock();
        g_retry_wait_monitor.timedWait(tbutil::Time::seconds(wait_second));
        g_retry_wait_monitor.unlock();
      }
      /*if (g_stop)
      {
        return TFS_ERROR;
      }*/
      retry_time = time(NULL);
    }
  }while (TFS_SUCCESS != ret);

  return ret;
}

int recover_second_queue(FileQueue* file_queue)
{
  // 1.check if secondqueue exist
  std::string queue_path = g_mirror_dir + "/secondqueue";
  if (!DirectoryOp::is_directory(queue_path.c_str()))
  {
    return TFS_SUCCESS;
  }

  // 2.init FileQueue
  FileQueue* second_file_queue = new FileQueue(g_mirror_dir, "secondqueue");
  int ret = second_file_queue->load_queue_head();
  if (TFS_SUCCESS == ret)
  {
    ret = second_file_queue->initialize();
    if (TFS_SUCCESS == ret)
    {
      // 3.move QueueItems from the 2nd queue to the 1st queue
      int32_t item_count = 0;
      while (!second_file_queue->empty())
      {
        QueueItem* item = NULL;
        item = second_file_queue->pop(0);
        if (NULL != item)
        {
          file_queue->push(&(item->data_[0]), item->length_);
          free(item);
          item = NULL;
          item_count++;
        }
      }
      TBSYS_LOG(DEBUG, "recover %d queue items from the second file queue success", item_count);

      // 4.delete FileQueue
      tbsys::gDelete(second_file_queue);

      // 5.remove secondqueue directory
      ret = DirectoryOp::delete_directory_recursively(queue_path.c_str(), true) ? TFS_SUCCESS : TFS_ERROR;
    }
  }
  return ret;
}

void* thread_func(void *args)
{
  ThreadParam *thread_param = static_cast<ThreadParam*>(args);
  int sync_success_count = 0;

  // init file queue
  uint64_t dest_ns_id = Func::get_host_ip(thread_param->dest_addr_);
  char file_queue_name[20];
  sprintf(file_queue_name, "queue_%"PRI64_PREFIX"u", dest_ns_id);
  FileQueue* file_queue = new FileQueue(g_mirror_dir, file_queue_name);
  int ret = file_queue->load_queue_head();
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "Sync thread %d load queue head failed!\n", thread_param->thread_index_);
    return (void*)NULL;
  }

  ret = file_queue->initialize();
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "Sync thread %d inite file queue failed!\n", thread_param->thread_index_);
    return (void*)NULL;
  }

  // for compatibility, the first thread need to recover the second filequeue(the retry one) if exists
  if (0 == thread_param->thread_index_)
  {
    ret = recover_second_queue(file_queue);
    if (TFS_SUCCESS != ret)
    {
      fprintf(stderr, "Sync thread %d recover second file queue failed!\n", thread_param->thread_index_);
      return (void*)NULL;
    }
  }

  // start to sync
  while (!g_stop && !file_queue->empty())
  {
    QueueItem* item = file_queue->pop(0);
    if (NULL != item)
    {
      if (TFS_SUCCESS == do_sync(thread_param->dest_addr_, &item->data_[0], item->length_))
      {
        file_queue->finish(0);
        sync_success_count++;
      }
      free(item);
    }
  }
  if (file_queue->empty())
  {
    fprintf(stderr, "Sync thread %d sync file queue successful! Sync success count: %d\n", thread_param->thread_index_, sync_success_count);
  }
  else
  {
    fprintf(stderr, "Sync thread %d stopped. Sync success count: %d\n", thread_param->thread_index_, sync_success_count);
  }
  tbsys::gDelete(file_queue);
  return (void*)NULL;
}



static void sign_handler(const int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "receive sig: %d\n", sig);
    g_stop = true;
    g_retry_wait_monitor.lock();
    g_retry_wait_monitor.notifyAll();
    g_retry_wait_monitor.unlock();

    fprintf(stderr, "wait for sync thread stop....\n");
      break;
  }
}

int main(int argc,char* argv[])
{
  int32_t help_info = 0;
  int32_t i;
  bool set_log_level = false;

  while ((i = getopt(argc, argv, "f:i:nvh")) != EOF)
  {
    switch (i)
    {
      case 'f':
        g_conf_file = optarg;
        break;
      case 'i':
        g_server_index = optarg;
        break;
      case 'n':
        set_log_level = true;
        break;
      case 'v':
        fprintf(stderr, "recover sync file queue tool, version: %s\n", Version::get_build_description());
        return 0;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == g_conf_file || 0 == g_server_index.size() || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i index -n -h -v\n", argv[0]);
    fprintf(stderr, "  -f ds.conf\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -n set log level to DEBUG\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("DEBUG");
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  signal(SIGINT, sign_handler);
  signal(SIGTERM, sign_handler);

  // load config
  TBSYS_CONFIG.load(g_conf_file);
  int32_t ret = SYSPARAM_DATASERVER.initialize(g_conf_file, g_server_index);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "SysParam::loadDataServerParam failed: %s\n", g_conf_file);
    return ret;
  }

  //get work directory
  string work_dir = "";
  const char* root_work_dir = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_WORK_DIR, "/tmp");
  if (NULL != root_work_dir)
  {
    work_dir = string(root_work_dir) + "/dataserver_" + g_server_index;
  }
  ret = work_dir.empty() ? EXIT_GENERAL_ERROR: TFS_SUCCESS;
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "can not find work dir!");
    return ret;
  }
  g_mirror_dir = work_dir + string("/mirror");
  if (!DirectoryOp::is_directory(g_mirror_dir.c_str()))
  {
    ret = EXIT_GENERAL_ERROR;
    TBSYS_LOG(ERROR, "directory %s not exist, error: %s", g_mirror_dir.c_str(), strerror(errno));
  }

  // init TfsClient
  g_tfs_client = TfsClientImpl::Instance();
  ret = g_tfs_client->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "init tfs client fail, ret: %d\n", ret);
    return ret;
  }

  // init file queue
  int backup_type = SYSPARAM_DATASERVER.tfs_backup_type_;
  if (backup_type != SYNC_TO_TFS_MIRROR)
  {
    fprintf(stderr, "Backup type:%d not supported!\n", backup_type);
    return ret;
  }

  // sync to tfs
  if (SYSPARAM_DATASERVER.local_ns_ip_.length() > 0 &&
      SYSPARAM_DATASERVER.local_ns_port_ != 0 &&
      SYSPARAM_DATASERVER.slave_ns_ip_.length() > 0)
  {
    // get local ns ip
    snprintf(g_src_addr, MAX_ADDRESS_LENGTH, "%s:%d",
             SYSPARAM_DATASERVER.local_ns_ip_.c_str(), SYSPARAM_DATASERVER.local_ns_port_);

    // get slave ns ip
    vector<std::string> slave_ns_ip;
    Func::split_string(SYSPARAM_DATASERVER.slave_ns_ip_.c_str(), '|', slave_ns_ip);

   // new sync thread
    vector<pthread_t> thread_ids;
    vector<ThreadParam*> thread_params;
    pthread_t tid;
    for (uint8_t i = 0; i < slave_ns_ip.size(); i++)
    {
      ThreadParam* thread_param = new ThreadParam;
      thread_param->thread_index_ = i;
      snprintf(thread_param->dest_addr_, MAX_ADDRESS_LENGTH, "%s", slave_ns_ip.at(i).c_str());
      pthread_create(&tid, NULL, thread_func, thread_param);
      thread_ids.push_back(tid);
      thread_params.push_back(thread_param);
    }

    // main thread wait
    for (uint8_t i = 0; i < thread_ids.size(); i++)
    {
      pthread_join(thread_ids[i], NULL);
      delete thread_params.at(i);
    }

  }

  return 0;
}

