/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_repair.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "tbsys.h"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "new_client/fsname.h"
#include "file_repair.h"
#include "dataservice.h"


namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;

    FileRepair::FileRepair() :
      init_status_(false), dataserver_id_(0), tfs_client_(NULL)
    {
      src_addr_[0] = '\0';
    }

    FileRepair::~FileRepair()
    {
    }

    bool FileRepair::init(const uint64_t dataserver_id)
    {
      if (init_status_)
      {
        return true;
      }

      TBSYS_LOG(INFO, "file repair init ns address: %s:%d",
                SYSPARAM_DATASERVER.local_ns_ip_.length() > 0 ? SYSPARAM_DATASERVER.local_ns_ip_.c_str() : "none",
                SYSPARAM_DATASERVER.local_ns_port_);

      int ret = false;
      if (SYSPARAM_DATASERVER.local_ns_ip_.length() > 0 &&
          SYSPARAM_DATASERVER.local_ns_port_ > 0)
      {
        snprintf(src_addr_, MAX_ADDRESS_LENGTH, "%s:%d",
                 SYSPARAM_DATASERVER.local_ns_ip_.c_str(),
                 SYSPARAM_DATASERVER.local_ns_port_);

        dataserver_id_ = dataserver_id;
        init_status_ = true;

        tfs_client_ = TfsClientImpl::Instance();
        ret =
          tfs_client_->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false) == TFS_SUCCESS ?
          true : false;
        if (ret)
        {
          init_status_ = true;
        }
      }

      return ret;
    }

    int FileRepair::fetch_file(const CrcCheckFile& crc_check_record, char* tmp_file)
    {
      if (NULL == tmp_file || !init_status_)
      {
        return TFS_ERROR;
      }

      int ret = TFS_SUCCESS;
      FSName fsname(crc_check_record.block_id_, crc_check_record.file_id_);
      int src_fd = tfs_client_->open(fsname.get_name(), NULL, src_addr_, T_READ);

      if (src_fd <= 0)
      {
        TBSYS_LOG(ERROR, "%s open src tfsfile read fail: %u %"PRI64_PREFIX"u, ret: %d",
                  fsname.get_name(), crc_check_record.block_id_,
                  crc_check_record.file_id_, src_fd);
        ret = TFS_ERROR;
      }
      else
      {
        TfsFileStat file_stat;
        if ((ret = tfs_client_->fstat(src_fd, &file_stat)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "%s stat tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                    fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_, ret);
        }
        else
        {
          int fd = -1;
          get_tmp_file_name(tmp_file, dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir().c_str(), fsname.get_name());

          if ((fd = open(tmp_file, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
          {
            TBSYS_LOG(ERROR, "copy file, open file: %s failed. error: %s", tmp_file, strerror(errno));
            ret = TFS_ERROR;
          }
          else
          {
            char data[MAX_READ_SIZE];
            int32_t rlen = 0;
            int32_t wlen = 0;
            int32_t offset = 0;
            uint32_t crc = 0;

            ret = TFS_ERROR;
            while (1)
            {
              if ((rlen = tfs_client_->read(src_fd, data, MAX_READ_SIZE)) <= 0)
              {
                TBSYS_LOG(ERROR, "%s read src tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                          fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_, rlen);
                break;
              }

              if ((wlen = write_file(fd, data, rlen)) != rlen)
              {
                TBSYS_LOG(ERROR, "%s write file fail: blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d, ret: %d",
                          fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_, rlen, wlen);
                  break;
              }

              crc = Func::crc(crc, data, rlen);
              offset += rlen;

              if (rlen < MAX_READ_SIZE || offset >= file_stat.size_)
              {
                ret = TFS_SUCCESS;
                break;
              }
            }

            if (TFS_SUCCESS == ret)
            {
              if (crc != file_stat.crc_ || crc != crc_check_record.crc_ || offset != file_stat.size_)
              {
                TBSYS_LOG(ERROR,
                          "file %s crc error. blockid: %u, fileid: %"PRI64_PREFIX"u, %u <> %u, checkfile crc: %u, size: %d <> %"PRI64_PREFIX"d",
                          fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_,
                          crc, file_stat.crc_, crc_check_record.crc_, offset, file_stat.size_);
                ret = TFS_ERROR;
              }
            }

            ::close(fd);
          }
        }

        tfs_client_->close(src_fd);
      }

      return ret;
    }

    int FileRepair::repair_file(const CrcCheckFile& crc_check_record, const char* tmp_file)
    {
      if (NULL == tmp_file || !init_status_)
      {
        return TFS_ERROR;
      }

      FSName fsname(crc_check_record.block_id_, crc_check_record.file_id_);
      int ret = tfs_client_->save_file_update(tmp_file, T_DEFAULT, fsname.get_name(), NULL, src_addr_) < 0 ? TFS_ERROR : TFS_SUCCESS;
      // int ret = tfs_client_->save_file(tmp_file, fsname.get_name()) < 0 ? TFS_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "%s repair file fail, blockid: %u fileid: %" PRI64_PREFIX "u, ret: %d",
                  fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "%s repair file success, blockid: %u fileid: %" PRI64_PREFIX "u",
                  fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_);
      }

      return ret;
    }

    void FileRepair::get_tmp_file_name(char* buffer, const char* path, const char* name)
    {
      if (NULL == buffer || NULL == path || NULL == name )
      {
        return;
      }
      snprintf(buffer, MAX_PATH_LENGTH, "%s/tmp/%s", path, name);
    }

    int FileRepair::write_file(const int fd, const char* buffer, const int32_t length)
    {
      int32_t bytes_write = 0;
      int wlen = 0;
      while (bytes_write < length)
      {
        wlen = write(fd, buffer + bytes_write, length - bytes_write);
        if (wlen < 0)
        {
          TBSYS_LOG(ERROR, "file repair failed when write. error desc: %s\n", strerror(errno));
          bytes_write = wlen;
          break;
        }
        else
        {
          bytes_write += wlen;
        }
      }

      return bytes_write;
    }

  }
}
