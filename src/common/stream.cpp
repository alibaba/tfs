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

#include "stream.h"
namespace tfs
{
  namespace common
  {
    Stream::Stream()
    {

    }

    Stream::Stream(const int64_t length)
    {
      if (length > 0)
      {
        buffer_.expand(length);
      }
    }

    Stream::~Stream()
    {

    }

    char* Stream::get_data() const
    {
      return buffer_.get_data();
    }

    int64_t Stream::get_data_length() const
    {
      return buffer_.get_data_length();
    }
    char* Stream::get_free() const
    {
      return buffer_.get_free();
    }
    int64_t Stream::get_free_length() const
    {
      return buffer_.get_free_length();
    }

    void Stream::expand(const int64_t length)
    {
      buffer_.expand(length);
    }

    void Stream::clear()
    {
      buffer_.clear();
    }

    int Stream::set_int8(const int8_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT8_SIZE)
      {
        buffer_.expand(INT8_SIZE);
      }
      int32_t iret = Serialization::set_int8(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT8_SIZE);
      }
      return iret;
    }

    int Stream::set_int16(const int16_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT16_SIZE)
      {
        buffer_.expand(INT16_SIZE);
      }
      int32_t iret = Serialization::set_int16(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT16_SIZE);
      }
      return iret;
    }

    int Stream::set_int32(const int32_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT_SIZE)
      {
        buffer_.expand(INT_SIZE);
      }
      int32_t iret = Serialization::set_int32(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(INT_SIZE);
      }
      return iret;
    }

    int Stream::set_int64(const int64_t value)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < INT64_SIZE)
      {
        buffer_.expand(INT64_SIZE);
      }
      int32_t iret = Serialization::set_int64(buffer_.get_free(), buffer_.get_free_length(), pos, value); 
      if (TFS_SUCCESS == iret)
        buffer_.pour(INT64_SIZE);
      return iret;
    }

    int Stream::set_bytes(const void* data, const int64_t length)
    {
      int64_t pos = 0;
      if (buffer_.get_free_length() < length)
      {
        buffer_.expand(length);
      }
      int32_t iret = Serialization::set_bytes(buffer_.get_free(), buffer_.get_free_length(), pos, data, length); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(length);
      }
      return iret;
    }

    int Stream::set_string(const char* str)
    {
      int64_t pos = 0;
      int64_t length = Serialization::get_string_length(str);
      if (buffer_.get_free_length() < length + INT_SIZE)
      {
        buffer_.expand(length);
      }
      int32_t iret = Serialization::set_string(buffer_.get_free(), buffer_.get_free_length(), pos, str); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(length);
      }
      return iret;
    }

    int Stream::set_string(const std::string& str)
    {
      int64_t pos = 0;
      int64_t length = Serialization::get_string_length(str);
      if (buffer_.get_free_length() < length + INT_SIZE)
      {
        buffer_.expand(length);
      }
      int32_t iret = Serialization::set_string(buffer_.get_free(), buffer_.get_free_length(), pos, str.c_str()); 
      if (TFS_SUCCESS == iret)
      {
        buffer_.pour(length);
      }
      return iret;
    }

    int Stream::get_int8(int8_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int8(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT8_SIZE);
      }
      return iret;
    }

    int Stream::get_int16(int16_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int16(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT16_SIZE);
      }
      return iret;
    }

    int Stream::get_int32(int32_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int32(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT_SIZE);
      }
      return iret;
    }

    int Stream::get_int64(int64_t* value)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_int64(buffer_.get_data(), buffer_.get_data_length(), pos, value);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(INT64_SIZE);
      }
      return iret;
    }

    int Stream::get_bytes(void* data, const int64_t length)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_bytes(buffer_.get_data(), buffer_.get_data_length(), pos, data, length);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(length);
      }
      return iret;
    }

    int Stream::get_string(const int64_t buf_length, char* str, int64_t& real_length)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_string(buffer_.get_data(), buffer_.get_data_length(), pos, buf_length, str, real_length);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(real_length);
      }
      return iret;
    }

    int Stream::get_string(std::string& str)
    {
      int64_t pos = 0;
      int32_t iret = Serialization::get_string(buffer_.get_data(), buffer_.get_data_length(), pos, str);
      if (TFS_SUCCESS == iret)
      {
        buffer_.drain(Serialization::get_string_length(str));
      }
      return iret;
    }
 }//end namespace comon
}//end namespace tfs

