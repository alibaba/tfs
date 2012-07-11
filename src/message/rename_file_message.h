/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rename_file_message.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_RENAMEFILEMESSAGE_H_
#define TFS_MESSAGE_RENAMEFILEMESSAGE_H_

#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class RenameFileMessage: public common::BasePacket
    {
      public:
        RenameFileMessage();
        virtual ~RenameFileMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          rename_file_info_.block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return rename_file_info_.block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          rename_file_info_.file_id_ = file_id;
        }
        inline uint64_t get_file_id() const
        {
          return rename_file_info_.file_id_;
        }
        inline void set_new_file_id(const uint64_t file_id)
        {
          rename_file_info_.new_file_id_ = file_id;
        }
        inline uint64_t get_new_file_id() const
        {
          return rename_file_info_.new_file_id_;
        }
        inline void set_ds_list(const common::VUINT64 &ds)
        {
          ds_ = ds;
        }
        inline const common::VUINT64& get_ds_list() const
        {
          return ds_;
        }
        inline void set_option_flag(const int32_t flag)
        {
          option_flag_ = flag;
        }
        inline int32_t get_option_flag() const
        {
          return option_flag_;
        }
        inline void set_server()
        {
          rename_file_info_.is_server_ = common::Slave_Server_Role;
        }
        inline common::ServerRole is_server() const
        {
          return rename_file_info_.is_server_;
        }
        inline bool has_lease() const
        {
          return (lease_id_ != common::INVALID_LEASE_ID);
        }
      protected:
        common::RenameFileInfo rename_file_info_;
        int32_t option_flag_;
        mutable common::VUINT64 ds_;
        mutable int32_t version_;
        mutable uint32_t lease_id_;
    };
  }
}
#endif
