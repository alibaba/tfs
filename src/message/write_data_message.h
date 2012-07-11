/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: write_data_message.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_WRITEDATAMESSAGE_H_
#define TFS_MESSAGE_WRITEDATAMESSAGE_H_
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class WriteDataMessage:  public common::BasePacket
    {
      public:
        WriteDataMessage();
        virtual ~WriteDataMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          write_data_info_.file_id_ = file_id;
        }
        inline uint64_t get_file_id() const
        {
          return write_data_info_.file_id_;
        }
        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }
        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }
        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }
        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }
        inline void set_data(char* const data)
        {
          data_ = data;
        }
        inline const char* const get_data() const
        {
          return data_;
        }
        inline void set_ds_list(const common::VUINT64 &ds)
        {
          ds_ = ds;
        }
        inline common::VUINT64 &get_ds_list()
        {
          return ds_;
        }
        inline void set_server(const common::ServerRole is_server)
        {
          write_data_info_.is_server_ = is_server;
        }
        inline common::ServerRole get_server() const
        {
          return write_data_info_.is_server_;
        }
        inline void set_file_number(const uint64_t file_num)
        {
          write_data_info_.file_number_ = file_num;
        }
        inline uint64_t get_file_number() const
        {
          return write_data_info_.file_number_;
        }
        inline void set_block_version(const int32_t version)
        {
          version_ = version;
        }
        inline int32_t get_block_version() const
        {
          return version_;
        }
        inline void set_lease_id(const uint32_t lease_id)
        {
          lease_id_ = lease_id;
        }
        inline uint32_t get_lease_id() const
        {
          return lease_id_;
        }
        inline common::WriteDataInfo get_write_info() const
        {
          return write_data_info_;
        }
        inline uint32_t has_lease() const
        {
          return (lease_id_ != common::INVALID_LEASE_ID);
        }

      protected:
        common::WriteDataInfo write_data_info_;
        const char* data_;
        mutable common::VUINT64 ds_;
        mutable int32_t version_;
        mutable uint32_t lease_id_;
    };

#ifdef _DEL_001_
    class RespWriteDataMessage :  public common::BasePacket
    {
      public:
      RespWriteDataMessage();
      virtual ~RespWriteDataMessage();
      virtual int serialize(common::Stream& output) const ;
      virtual int deserialize(common::Stream& input);
      virtual int64_t length() const;

      void set_length(const int32_t length)
      {
        length_ = length;
      }

      int32_t get_length() const
      {
        return length_;
      }

      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();
      protected:
      int32_t length_;
    };
#endif

    class WriteRawDataMessage:  public common::BasePacket
    {
      public:
        WriteRawDataMessage();
        virtual ~WriteRawDataMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }
        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }
        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }
        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }
        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }
        inline void set_data(const char* data)
        {
          data_ = data;
        }
        inline const char* const get_data() const
        {
          return data_;
        }
        inline void set_new_block(const int32_t flag)
        {
          flag_ = flag;
        }
        inline int32_t get_new_block() const
        {
          return flag_;
        }
      protected:
        common::WriteDataInfo write_data_info_;
        const char* data_;
        int32_t flag_;
    };

    class WriteInfoBatchMessage:  public common::BasePacket
    {
      public:
        WriteInfoBatchMessage();
        virtual ~WriteInfoBatchMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }
        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }
        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }
        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }
        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }
        inline void set_cluster(const int32_t cluster)
        {
          cluster_ = cluster;
        }
        inline int32_t get_cluster() const
        {
          return cluster_;
        }
        inline void set_raw_meta_list(const common::RawMetaVec* list)
        {
          if (NULL != list)
            meta_list_ = (*list);
        }
        inline const common::RawMetaVec* get_raw_meta_list() const
        {
          return &meta_list_;
        }
        inline void set_block_info(common::BlockInfo* const block_info)
        {
          if (NULL != block_info)
            block_info_ = *block_info;
        }
        inline const common::BlockInfo* get_block_info() const
        {
          return block_info_.block_id_ <= 0 ? NULL : &block_info_;
        }
      protected:
        common::WriteDataInfo write_data_info_;
        common::BlockInfo block_info_;
        common::RawMetaVec meta_list_;
        int32_t cluster_;
    };
  }
}
#endif
