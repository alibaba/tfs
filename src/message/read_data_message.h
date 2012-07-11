/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: read_data_message.h 515 2011-06-17 01:50:58Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_READDATAMESSAGE_H_
#define TFS_MESSAGE_READDATAMESSAGE_H_
#include "common/base_packet.h"
namespace tfs
{
  namespace message
  {
#pragma pack(4)
    struct ReadDataInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      uint64_t file_id_;
      int32_t offset_;
      int32_t length_;
    };
#pragma pack()

    class ReadDataMessage: public common::BasePacket
    {
      public:
        ReadDataMessage();
        virtual ~ReadDataMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_block_id(const uint32_t block_id)
        {
          read_data_info_.block_id_ = block_id;
        }
        uint32_t get_block_id() const
        {
          return read_data_info_.block_id_;
        }
        void set_file_id(const uint64_t file_id)
        {
          read_data_info_.file_id_ = file_id;
        }
        uint64_t get_file_id() const
        {
          return read_data_info_.file_id_;
        }
        void set_offset(const int32_t offset)
        {
          read_data_info_.offset_ = offset;
        }
        int32_t get_offset() const
        {
          return read_data_info_.offset_;
        }
        void set_length(const int32_t length)
        {
          read_data_info_.length_ = length;
        }
        int32_t get_length() const
        {
          return read_data_info_.length_;
        }
        int8_t get_flag() const 
        {
          return flag_;
        }
        void set_flag(const int8_t flag)
        {
          flag_ = flag;
        }
      protected:
        ReadDataInfo read_data_info_;
        int8_t flag_;
    };

    class RespReadDataMessage: public common::BasePacket
    {
      public:
        RespReadDataMessage();
        virtual ~RespReadDataMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        char* alloc_data(const int32_t len);
        inline void set_length(const int32_t len) { length_ = len;}
        inline char* get_data() const { return data_;}
        inline int32_t get_length() const { return length_;}
      protected:
        char* data_;
        int32_t length_;
        bool alloc_;
    };

    class ReadDataMessageV2: public ReadDataMessage 
    {
      public:
        ReadDataMessageV2();
        virtual ~ReadDataMessageV2();
    };

    class RespReadDataMessageV2: public RespReadDataMessage 
    {
      public:
        RespReadDataMessageV2();
        virtual ~RespReadDataMessageV2();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        void set_file_info(common::FileInfo* const file_info)
        {
          if (NULL != file_info)
            file_info_ = *file_info;
        }
        const common::FileInfo* get_file_info() const
        {
          return file_info_.id_ > 0 ? &file_info_ : NULL;
        }
      protected:
        common::FileInfo file_info_;
    };

    class ReadDataMessageV3: public ReadDataMessageV2
    {
      public:
        ReadDataMessageV3();
        virtual ~ReadDataMessageV3();
    };

    class RespReadDataMessageV3: public RespReadDataMessageV2
    {
      public:
      RespReadDataMessageV3();
      virtual ~RespReadDataMessageV3();
    };

    class ReadRawDataMessage: public ReadDataMessage 
    {
      public:
        ReadRawDataMessage();
        virtual ~ReadRawDataMessage();
    };

    class RespReadRawDataMessage: public RespReadDataMessage 
    {
      public:
        RespReadRawDataMessage();
        virtual ~RespReadRawDataMessage();
    };

    class ReadScaleImageMessage: public ReadDataMessage
    {
      public:
        enum ZoomStatus
        {
          ZOOM_NONE = 0,
          ZOOM_SPEC = 1,
          ZOOM_PROP = 2
        };
        struct ZoomData
        {
          int deserialize(const char* data, const int64_t data_len, int64_t& pos);
          int serialize(char* data, const int64_t data_len, int64_t& pos) const;
          int64_t length() const;
          int32_t zoom_width_;
          int32_t zoom_height_;
          int32_t zoom_type_;
        };

        ReadScaleImageMessage();
        virtual ~ReadScaleImageMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        void set_zoom_data(const ZoomData & zoom)
        {
          zoom_ = zoom;
        }
        const ZoomData & get_zoom_data() const
        {
          return zoom_;
        }
      protected:
        ZoomData zoom_;
    };
  }
}
#endif
