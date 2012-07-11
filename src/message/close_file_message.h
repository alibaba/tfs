/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: close_file_message.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CLOSEFILEMESSAGE_H_
#define TFS_MESSAGE_CLOSEFILEMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class CloseFileMessage: public common::BasePacket
    {
      public:
        CloseFileMessage();
        virtual ~CloseFileMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          close_file_info_.block_id_ = block_id;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          close_file_info_.file_id_ = file_id;
        }
        inline void set_mode(const common::CloseFileServer mode)
        {
          close_file_info_.mode_ = mode;
        }
        inline void set_crc(const uint32_t crc)
        {
          close_file_info_.crc_ = crc;
        }
        inline void set_file_number(const uint64_t file_number)
        {
          close_file_info_.file_number_ = file_number;
        }
        inline void set_ds_list(const common::VUINT64& ds)
        {
          ds_ = ds;
        }
        inline void set_block(common::BlockInfo* const block_info)
        {
          if (NULL != block_info)
            block_ = *block_info;
        }
        inline void set_file_info(common::FileInfo* const file_info)
        {
          if (NULL != file_info)
            file_info_ = *file_info;
        }
        inline void set_option_flag(const int32_t flag)
        {
          option_flag_ = flag;
        }
        inline void set_block_version(const int32_t version)
        {
          version_ = version;
        }
        inline void set_lease_id(const uint32_t lease_id)
        {
          lease_id_ = lease_id;
        }
        inline uint32_t get_block_id() const
        {
          return close_file_info_.block_id_;
        }
        inline uint64_t get_file_id() const
        {
          return close_file_info_.file_id_;
        }
        inline common::CloseFileServer get_mode() const
        {
          return close_file_info_.mode_;
        }
        inline uint32_t get_crc() const
        {
          return close_file_info_.crc_;
        }
        inline uint64_t get_file_number() const
        {
          return close_file_info_.file_number_;
        }
        inline const common::VUINT64& get_ds_list() const
        {
          return ds_;
        }
        inline const common::BlockInfo* get_block() const
        {
          return block_.block_id_ > 0 ? &block_ : NULL;
        }
        inline const common::FileInfo* get_file_info() const
        {
          return file_info_.id_ > 0 ? &file_info_ : NULL;
        }
        inline int32_t get_option_flag() const
        {
          return option_flag_;
        }
        inline int32_t get_block_version() const
        {
          return version_;
        }
        inline uint32_t get_lease_id() const
        {
          return lease_id_;
        }
        inline common::CloseFileInfo get_close_file_info() const
        {
          return close_file_info_;
        }

        inline bool has_lease() const
        {
          return (lease_id_ != common::INVALID_LEASE_ID);
        }

      protected:
        common::CloseFileInfo close_file_info_;
        common::BlockInfo block_;
        common::FileInfo file_info_;
        mutable common::VUINT64 ds_;
        int32_t option_flag_;
        mutable int32_t version_;
        mutable uint32_t lease_id_;
    };
  }
}
#endif
