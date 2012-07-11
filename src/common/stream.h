/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *
 */

#ifndef TFS_COMMON_STREAM_H_
#define TFS_COMMON_STREAM_H_

#include "internal.h"
#include "buffer.h"
#include "serialization.h"
namespace tfs
{
  namespace common
  {
    class BasePacket;
    class Stream
    {
    friend class BasePacket;
    public:
      Stream();
      explicit Stream(const int64_t length);
      virtual ~Stream();

      char* get_data() const;
      int64_t get_data_length() const;

      char* get_free() const;
      int64_t get_free_length() const;

      inline int drain(const int64_t length) { return buffer_.drain(length);}
      inline int pour(const int64_t length) { return buffer_.pour(length);}

      inline Buffer& get_buffer() { return buffer_;}

      int set_int8(const int8_t value);
      int set_int16(const int16_t value);
      int set_int32(const int32_t value);
      int set_int64(const int64_t value);
      int set_bytes(const void* data, const int64_t length);
      int set_string(const char* str);
      int set_string(const std::string& str);
      template <typename T>
      int set_vint8(const T& value)
      {
        int64_t pos = 0;
        int64_t size = Serialization::get_vint8_length(value);
        if (buffer_.get_free_length() < size)
        {
          buffer_.expand(size);
        }
        int32_t iret = Serialization::set_vint8(buffer_.get_free(), buffer_.get_free_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
        return iret;
      }

      template <typename T>
      int set_vint16(const T& value)
      {
        int64_t pos = 0;
        int64_t size = Serialization::get_vint16_length(value);
        if (buffer_.get_free_length() < size)
        {
          buffer_.expand(size);
        }
        int32_t iret = Serialization::set_vint16(buffer_.get_free(), buffer_.get_free_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
        return iret;
      }

      template <typename T>
      int set_vint32(const T& value)
      {
        int64_t pos = 0;
        int64_t size = Serialization::get_vint32_length(value);
        if (buffer_.get_free_length() < size)
        {
          buffer_.expand(size);
        }
        int32_t iret = Serialization::set_vint32(buffer_.get_free(), buffer_.get_free_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
        return iret;
      }

      template <typename T>
      int set_vint64(const T& value)
      {
        int64_t pos = 0;
        int64_t size = Serialization::get_vint64_length(value);
        if (buffer_.get_free_length() < size)
        {
          buffer_.expand(size);
        }
        int32_t iret = Serialization::set_vint64(buffer_.get_free(), buffer_.get_free_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.pour(size);
        }
        return iret;
      }

      int get_int8(int8_t* value);
      int get_int16(int16_t* value);
      int get_int32(int32_t* value);
      int get_int64(int64_t* value);
      int get_bytes(void* data, const int64_t length);
      int get_string(const int64_t buf_length, char* str, int64_t& real_length);
      int get_string(std::string& str);
      template <typename T>
      int get_vint8(T& value)
      {
        int64_t pos = 0;
        int32_t iret = Serialization::get_vint8(buffer_.get_data(), buffer_.get_data_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(pos);
        }
        return iret;
      }

      template <typename T>
      int get_vint16(T& value)
      {
        int64_t pos = 0;
        int32_t iret = Serialization::get_vint16(buffer_.get_data(), buffer_.get_data_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(pos);
        }
        return iret;
      }

      template <typename T>
      int get_vint32(T& value)
      {
        int64_t pos = 0;
        int32_t iret = Serialization::get_vint32(buffer_.get_data(), buffer_.get_data_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(pos);
        }
        return iret;
      }
      template <typename T>
      int get_vint64(T& value)
      {
        int64_t pos = 0;
        int32_t iret = Serialization::get_vint64(static_cast<const char*>(buffer_.get_data()), buffer_.get_data_length(), pos, value);
        if (TFS_SUCCESS == iret)
        {
          buffer_.drain(pos);
        }
        return iret;
      }
    private:
      void expand(const int64_t length);

      DISALLOW_COPY_AND_ASSIGN(Stream);
      Buffer buffer_;
      void clear();
    };
  }//end namespace comon
}//end namespace tfs

#endif /*TFS_COMMON_STREAM_H_*/
