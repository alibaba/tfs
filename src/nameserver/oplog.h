/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog.h 397 2011-06-02 07:23:53Z duanfei@taobao.com $
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
#ifndef TFS_NAMESERVER_OPLOG_H_
#define TFS_NAMESERVER_OPLOG_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <ext/hash_map>
#include <errno.h>
#include <dirent.h>
#include <Mutex.h>

#include "ns_define.h"
#include "common/file_queue.h"

namespace tfs
{
  namespace nameserver
  {
    enum OpLogType
    {
      OPLOG_TYPE_BLOCK_OP = 0x00,
      OPLOG_TYPE_REPLICATE_MSG,
      OPLOG_TYPE_COMPACT_MSG
    };
#pragma pack(1)
    struct OpLogHeader
    {
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int64_t length() const;
      uint32_t seqno_;
      uint32_t time_;
      uint32_t crc_;
      uint16_t length_;
      int8_t  type_;
      int8_t  reserve_;
      char  data_[0];
    };
    struct OpLogRotateHeader
    {
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int64_t length() const;
      uint32_t seqno_;
      int32_t rotate_seqno_;
      int32_t rotate_offset_;
    };
#pragma pack()

    struct BlockOpLog
    {
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int64_t length() const;
      uint32_t seqno_;
      common::BlockInfo info_;
      common::VUINT32 blocks_;
      common::VUINT64 servers_;
      int8_t cmd_;
      void dump(const int32_t level) const;
    };

    class OpLog
    {
      public:
        OpLog(const std::string& path, const int32_t max_log_slot_size = 0x400);
        virtual ~OpLog();
        int initialize();
        int update_oplog_rotate_header(const OpLogRotateHeader& head);
        int write(const uint8_t type, const char* const data, const int32_t length);
        inline void reset()
        {
          slots_offset_ = 0;
        }
        inline const char* const get_buffer() const
        {
          return buffer_;
        }
        inline int64_t get_slots_offset() const
        {
          return slots_offset_;
        }
        inline const OpLogRotateHeader* get_oplog_rotate_header() const
        {
          return &oplog_rotate_header_;
        }
      public:
        static int const MAX_LOG_SIZE = sizeof(OpLogHeader) + common::BLOCKINFO_SIZE + 1 + 64 * common::INT64_SIZE;
        const int MAX_LOG_SLOTS_SIZE;
        const int MAX_LOG_BUFFER_SIZE;
      private:
        OpLogRotateHeader oplog_rotate_header_;
        std::string path_;
        uint64_t seqno_;
        int64_t slots_offset_;
        int32_t fd_;
        char* buffer_;
      private:
        DISALLOW_COPY_AND_ASSIGN( OpLog);
    };
  }//end namespace nameserver
}//end namespace tfs
#endif
