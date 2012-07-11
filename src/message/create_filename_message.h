/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: create_filename_message.h 346 2011-05-26 01:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_CREATEFILENAMEMESSAGE_H_
#define TFS_MESSAGE_CREATEFILENAMEMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
    class CreateFilenameMessage: public common::BasePacket 
    {
      public:
        CreateFilenameMessage();
        virtual ~CreateFilenameMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }
        inline uint64_t get_file_id() const
        {
          return file_id_;
        }
      protected:
        uint32_t block_id_;
        uint64_t file_id_;
    };

    class RespCreateFilenameMessage: public common::BasePacket 
    {
      public:
        RespCreateFilenameMessage();
        virtual ~RespCreateFilenameMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_block_id(const uint32_t block_id)
        {
          block_id_ = block_id;
        }
        inline uint32_t get_block_id() const
        {
          return block_id_;
        }
        inline void set_file_id(const uint64_t file_id)
        {
          file_id_ = file_id;
        }
        inline uint64_t get_file_id() const
        {
          return file_id_;
        }
        inline void set_file_number(const uint64_t file_number)
        {
          file_number_ = file_number;
        }
        inline uint64_t get_file_number() const
        {
          return file_number_;
        }
      protected:
        uint32_t block_id_;
        uint64_t file_id_;
        uint64_t file_number_;
    };
  }
}
#endif
