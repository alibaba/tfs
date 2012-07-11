/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <tbsys.h>
#include <Memory.hpp>
#include "oplog.h"
#include "common/error_msg.h"
#include "common/directory_op.h"
#include "common/parameter.h"

namespace tfs
{
  namespace nameserver
  {
    int OpLogHeader::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, seqno_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, crc_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int16(data, data_len, pos, length_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int8(data, data_len, pos, type_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int8(data, data_len, pos, reserve_);
      }
      return ret;
    }

    int OpLogHeader::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&seqno_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&time_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&crc_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int16(data, data_len, pos, reinterpret_cast<int16_t*>(&length_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int8(data, data_len, pos, &type_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int8(data, data_len, pos, &reserve_);
      }
      return ret;
    }
    int64_t OpLogHeader::length() const
    {
      return common::INT_SIZE * 4;
    }
    int OpLogRotateHeader::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, seqno_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, rotate_seqno_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, rotate_offset_);
      }
      return ret;
    }
    int OpLogRotateHeader::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&seqno_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&rotate_seqno_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &rotate_offset_);
      }
      return ret;
    }
    int64_t OpLogRotateHeader::length() const
    {
      return common::INT_SIZE * 3;
    }
    void BlockOpLog::dump(const int32_t level) const
    {
      if (level <= TBSYS_LOGGER._level)
      {
        std::string bstr;
        std::string sstr;
        print_servers(servers_, sstr);
        print_blocks(blocks_, bstr);
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__,
          "cmd: %s, block_ids: %s version: %u file_count: %u size: %u delfile_count: %u del_size: %u seqno: %u, ds_size: %zd, dataserver: %s",
          cmd_ == common::OPLOG_INSERT ? "insert" : cmd_ == common::OPLOG_REMOVE ? "remove" : cmd_ == common::OPLOG_RELIEVE_RELATION ? "release" : cmd_ == common::OPLOG_RENAME ? "rename" : "update",
          bstr.c_str(), info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_,
          info_.seq_no_, servers_.size(), sstr.c_str());
      }
    }

    int BlockOpLog::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int8(data, data_len, pos, cmd_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = info_.serialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int8(data, data_len, pos, blocks_.size());
      }
      if (common::TFS_SUCCESS == ret)
      {
        std::vector<uint32_t>::const_iterator iter = blocks_.begin();
        for (; iter != blocks_.end(); ++iter)
        {
          ret =  common::Serialization::set_int32(data, data_len, pos, (*iter));
          if (common::TFS_SUCCESS != ret)
            break;
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int8(data, data_len, pos, servers_.size());
      }
      if (common::TFS_SUCCESS == ret)
      {
        std::vector<uint64_t>::const_iterator iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          ret =  common::Serialization::set_int64(data, data_len, pos, (*iter));
          if (common::TFS_SUCCESS != ret)
            break;
        }
      }
      return ret;
    }

    int BlockOpLog::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int8(data, data_len, pos, &cmd_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = info_.deserialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int8_t size = 0;
        ret = common::Serialization::get_int8(data, data_len, pos, &size);
        if (common::TFS_SUCCESS == ret)
        {
          uint32_t block_id = 0;
          for (int8_t i = 0; i < size; ++i)
          {
            ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id));
            if (common::TFS_SUCCESS == ret)
              blocks_.push_back(block_id);
            else
              break;
          }
        }
      }

      if (common::TFS_SUCCESS == ret)
      {
        int8_t size = 0;
        ret = common::Serialization::get_int8(data, data_len, pos, &size);
        if (common::TFS_SUCCESS == ret)
        {
          uint64_t server = 0;
          for (int8_t i = 0; i < size; ++i)
          {
            ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&server));
            if (common::TFS_SUCCESS == ret)
              servers_.push_back(server);
            else
              break;
          }
        }
      }
      return ret;
    }

    int64_t BlockOpLog::length(void) const
    {
      return common::INT8_SIZE + info_.length() + common::INT8_SIZE
            + blocks_.size() * common::INT_SIZE + common::INT8_SIZE
            + servers_.size() * common::INT64_SIZE;
    }

    OpLog::OpLog(const std::string& logname, const int32_t max_log_slot_size) :
      MAX_LOG_SLOTS_SIZE(max_log_slot_size), MAX_LOG_BUFFER_SIZE(max_log_slot_size * MAX_LOG_SIZE), path_(logname), seqno_(
          0), slots_offset_(0), fd_(-1), buffer_(NULL)
    {
      buffer_ = new (std::nothrow) char[max_log_slot_size * MAX_LOG_SIZE];
      assert(NULL != buffer_);
      memset(buffer_, 0, max_log_slot_size * MAX_LOG_SIZE);
      memset(&oplog_rotate_header_, 0, sizeof(oplog_rotate_header_));
    }

    OpLog::~OpLog()
    {
      tbsys::gDeleteA( buffer_);
      if (fd_ > 0)
        ::close( fd_);
    }

    int OpLog::initialize()
    {
      int32_t ret = path_.empty() ? common::EXIT_GENERAL_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        if (!common::DirectoryOp::create_full_path(path_.c_str()))
        {
          TBSYS_LOG(ERROR, "create directory: %s fail...", path_.c_str());
          ret = common::EXIT_GENERAL_ERROR;
        }
        else
        {
          path_ += "/rotateheader.dat";
          fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0600);
          if (fd_ < 0)
          {
            TBSYS_LOG(ERROR, "open file: %s fail: %s", path_.c_str(), strerror(errno));
            ret = common::EXIT_GENERAL_ERROR;
          }
          else
          {
            oplog_rotate_header_.rotate_seqno_ = 1;
            oplog_rotate_header_.rotate_offset_ = 0;
            char buf[oplog_rotate_header_.length()];
            const int64_t length = read(fd_, buf, oplog_rotate_header_.length());
            if (length == oplog_rotate_header_.length())
            {
              int64_t pos = 0;
              oplog_rotate_header_.deserialize(buf, oplog_rotate_header_.length(), pos);
              if (common::TFS_SUCCESS != ret)
              {
                oplog_rotate_header_.rotate_seqno_ = 1;
                oplog_rotate_header_.rotate_offset_ = 0;
              }
            }
          }
        }
      }
      return ret;
    }

    int OpLog::update_oplog_rotate_header(const OpLogRotateHeader& head)
    {
      memcpy(&oplog_rotate_header_, &head, sizeof(head));
      int32_t ret = common::TFS_SUCCESS;
      if (fd_ < 0)
      {
        fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd_ < 0)
        {
          TBSYS_LOG(WARN, "open file: %s fail: %s", path_.c_str(), strerror(errno));
          ret = common::EXIT_GENERAL_ERROR;
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        lseek(fd_, 0, SEEK_SET);
        int64_t pos = 0;
        char buf[oplog_rotate_header_.length()];
        memset(buf, 0, sizeof(buf));
        ret = oplog_rotate_header_.serialize(buf, oplog_rotate_header_.length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          int64_t length = ::write(fd_, buf, oplog_rotate_header_.length());
          if (length != oplog_rotate_header_.length())
          {
            TBSYS_LOG(WARN, "wirte data fail: file: %s, erros: %s...", path_.c_str(), strerror(errno));
            ::close( fd_);
            fd_ = -1;
            fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0600);
            if (fd_ < 0)
            {
              TBSYS_LOG(WARN, "open file: %s fail: %s", path_.c_str(), strerror(errno));
              ret = common::EXIT_GENERAL_ERROR;
            }
            else
            {
              lseek(fd_, 0, SEEK_SET);
              length = ::write(fd_, buf, oplog_rotate_header_.length());
              if (length != oplog_rotate_header_.length())
              {
                TBSYS_LOG(WARN, "wirte data fail: file: %s, erros: %s...", path_.c_str(), strerror(errno));
                ret = common::EXIT_GENERAL_ERROR;
              }
            }
          }
        }
      }
      return ret;
    }

    int OpLog::write(const uint8_t type, const char* const data, const int32_t length)
    {
      int32_t ret = ((NULL == data) || (length <= 0) || (length > OpLog::MAX_LOG_SIZE))
              ? common::EXIT_PARAMETER_ERROR: common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        OpLogHeader header;
        const int64_t offset = slots_offset_ + header.length() + length;
        if (offset > MAX_LOG_BUFFER_SIZE)
        {
          TBSYS_LOG(DEBUG, "(slots_offset_ + size): %"PRI64_PREFIX"d > MAX_LOG_BUFFER_SIZE: %d",
              offset, MAX_LOG_BUFFER_SIZE);
          ret = common::EXIT_SLOTS_OFFSET_SIZE_ERROR;
        }
        else
        {
          header.crc_  = common::Func::crc(0, data, length);
          header.time_ = time(NULL);
          header.length_ = length;
          header.seqno_  = ++seqno_;
          header.type_ = type;
          ret = header.serialize(buffer_, MAX_LOG_BUFFER_SIZE, slots_offset_);
          if (common::TFS_SUCCESS == ret)
          {
            ret = common::Serialization::set_bytes(buffer_, MAX_LOG_BUFFER_SIZE, slots_offset_, data, length);
          }
        }
      }
      return ret;
    }
  }//end namespace nameserver
}//end namespace tfs
