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
 *   duolong <duolong@taobao.com>
 *
 */

#ifndef TFS_COMMON_SERIALIZATION_H_
#define TFS_COMMON_SERIALIZATION_H_
#include "internal.h"

namespace tfs
{
  namespace common
  {
    struct Serialization
    {
      static int64_t get_string_length(const char* str)
      {
        return NULL == str ? INT_SIZE : strlen(str) == 0 ? INT_SIZE : strlen(str) + INT_SIZE + 1;
      }
      static int64_t get_string_length(const std::string& str)
      {
        return str.empty() ? INT_SIZE : str.length() + INT_SIZE + 1;
      }
      template <typename T>
      static int64_t get_vint8_length(const T& value)
      {
        return INT_SIZE + value.size() * INT8_SIZE;
      }
      template <typename T>
      static int64_t get_vint16_length(const T& value)
      {
        return INT_SIZE + value.size() * INT16_SIZE;
      }
      template <typename T>
      static int64_t get_vint32_length(const T& value)
      {
        return INT_SIZE + value.size() * INT_SIZE;
      }
      template <typename T>
      static int64_t get_vint64_length(const T& value)
      {
        return INT_SIZE + value.size() * INT64_SIZE;
      }
      static int get_int8(const char* data, const int64_t data_len, int64_t& pos, int8_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT8_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          *value = data[pos++];
        }
        return iret;
      }

      static int get_int16(const char* data, const int64_t data_len, int64_t& pos, int16_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT16_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT16_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_int32(const char* data, const int64_t data_len, int64_t& pos, int32_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_int64(const char* data, const int64_t data_len, int64_t& pos, int64_t* value)
      {
        int32_t iret = NULL != value && NULL != data && data_len - pos >= INT64_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t tmp = pos += INT64_SIZE;
          *value = static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
          *value <<= 8;
          *value |= static_cast<unsigned char>(data[--tmp]);
        }
        return iret;
      }

      static int get_bytes(const char* data, const int64_t data_len, int64_t& pos, void* buf, const int64_t buf_length)
      {
        int32_t iret = NULL != data && NULL != buf && buf_length > 0 && data_len - pos >= buf_length &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          memcpy(buf, (data+pos), buf_length);
          pos += buf_length;
        }
        return iret;
      }

      static int get_string(const char* data, const int64_t data_len, int64_t& pos, const int64_t str_buf_length, char* str, int64_t& real_str_buf_length)
      {
        int32_t iret = NULL != data &&  data_len - pos >= INT_SIZE  &&  pos >= 0  && NULL != str && str_buf_length > 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          str[0] = '\0';
          real_str_buf_length = 0;
          int32_t length  = 0;
          iret = get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret)
          {
            if (length > 0)
            {
              iret = length <= str_buf_length ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                iret = data_len - pos >= length ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS == iret)
                {
                  memcpy(str, (data+pos), length);
                  pos += length;
                  real_str_buf_length = length - 1;
                }
              }
            }
          }
        }
        return iret;
      }

      static int get_string(const char* data, const int64_t data_len, int64_t& pos, std::string& str)
      {
        int32_t iret = NULL != data &&  data_len - pos >= INT_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length = 0;
          iret = get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret)
          {
            if (length > 0)
            {
              iret = data_len - pos >= length ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                str.assign((data + pos), length - 1);
                pos += length;
              }
            }
            else
            {
              str.clear();
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int get_vint8(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length = 0;
          iret =  Serialization::get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret
              && length > 0)
          {
            iret = data_len - pos >= length * INT8_SIZE ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              int8_t tmp = 0;
              for (int32_t i = 0; i < length; ++i)
              {
                iret = Serialization::get_int8(data, data_len, pos, &tmp);
                if (TFS_SUCCESS == iret)
                  value.push_back(tmp);
                else
                  break;
              }
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int get_vint16(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length = 0;
          iret =  Serialization::get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret
              && length > 0)
          {
            iret = data_len - pos >= length * INT16_SIZE ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              int16_t tmp = 0;
              for (int32_t i = 0; i < length; ++i)
              {
                iret = Serialization::get_int16(data, data_len, pos, &tmp);
                if (TFS_SUCCESS == iret)
                  value.push_back(tmp);
                else
                  break;
              }
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int get_vint32(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length = 0;
          iret =  Serialization::get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret
              && length > 0)
          {
            iret = data_len - pos >= length * INT_SIZE ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              int32_t tmp = 0;
              for (int32_t i = 0; i < length; ++i)
              {
                iret = Serialization::get_int32(data, data_len, pos, &tmp);
                if (TFS_SUCCESS == iret)
                  value.push_back(tmp);
                else
                  break;
              }
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int get_vint64(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int32_t length = 0;
          iret =  Serialization::get_int32(data, data_len, pos, &length);
          if (TFS_SUCCESS == iret
              && length > 0)
          {
            iret = data_len - pos >= length * INT64_SIZE ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              int64_t tmp = 0;
              for (int32_t i = 0; i < length; ++i)
              {
                iret = Serialization::get_int64(data, data_len, pos, &tmp);
                if (TFS_SUCCESS == iret)
                  value.push_back(tmp);
                else
                  break;
              }
            }
          }
        }
        return iret;
      }

      static int set_int8(char* data, const int64_t data_len, int64_t& pos, const int8_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT8_SIZE  &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] = value;
        }
        return iret;
      }

      static int set_int16(char* data, const int64_t data_len, int64_t& pos, const int16_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT16_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] = value & 0xFF;
          data[pos++]= (value >> 8) & 0xFF;
        }
        return iret;
      }

      static int set_int32(char* data, const int64_t data_len, int64_t& pos, const int32_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] =  value & 0xFF;
          data[pos++] =  (value >> 8) & 0xFF;
          data[pos++] =  (value >> 16) & 0xFF;
          data[pos++] =  (value >> 24) & 0xFF;
        }
        return iret;
      }

      static int set_int64(char* data, const int64_t data_len, int64_t& pos, const int64_t value)
      {
        int32_t iret = NULL != data && data_len - pos >= INT64_SIZE &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          data[pos++] =  (value) & 0xFF;
          data[pos++] =  (value >> 8) & 0xFF;
          data[pos++] =  (value >> 16) & 0xFF;
          data[pos++] =  (value >> 24) & 0xFF;
          data[pos++] =  (value >> 32) & 0xFF;
          data[pos++] =  (value >> 40) & 0xFF;
          data[pos++] =  (value >> 48) & 0xFF;
          data[pos++] =  (value >> 56) & 0xFF;
        }
        return iret;
      }

      static int set_string(char* data, const int64_t data_len, int64_t& pos, const std::string& str)
      {
        int32_t iret = NULL != data &&  pos < data_len &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t length = str.empty() ? 0 : str.length() + 1;/** include '\0' length*/
          iret = data_len - pos >= (length + INT_SIZE) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            iret = set_int32(data, data_len, pos, length);
            if (TFS_SUCCESS == iret)
            {
              if (length > 0)
              {
                memcpy((data+pos), str.c_str(), length - 1);
                pos += length;
                data[pos - 1] = '\0';
              }
            }
          }
        }
        return iret;
      }

      static int set_string(char* data, const int64_t data_len, int64_t& pos, const char* str)
      {
        int32_t iret = NULL != data &&  pos < data_len &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          int64_t length = NULL == str ? 0 : strlen(str) == 0 ? 0 : strlen(str) + 1;/** include '\0'**/
          iret = data_len - pos >= (length + INT_SIZE) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            iret = set_int32(data, data_len, pos, length);
            if (TFS_SUCCESS == iret)
            {
              if (length > 0)
              {
                memcpy((data+pos), str, length - 1);
                pos += length;
                data[pos - 1] = '\0';
              }
            }
          }
        }
        return iret;
      }

      static int set_bytes(char* data, const int64_t data_len, int64_t& pos, const void* buf, const int64_t buf_length)
      {
        int32_t iret = NULL != data && buf_length > 0 && NULL != buf && data_len - pos >= buf_length &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          memcpy((data+pos), buf, buf_length);
          pos += buf_length;
        }
        return iret;
      }

      template <typename T>
      static int set_vint8(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= get_vint8_length(value) &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, value.size());
          if (TFS_SUCCESS == iret)
          {
            typename T::const_iterator iter = value.begin();
            for (; iter != value.end(); ++iter)
            {
              iret = Serialization::set_int8(data, data_len, pos, (*iter));
              if (TFS_SUCCESS != iret)
                break;
            }
          }
        }
        return iret;
      }
      template <typename T>
      static int set_vint16(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= get_vint16_length(value) &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, value.size());
          if (TFS_SUCCESS == iret)
          {
            typename T::const_iterator iter = value.begin();
            for (; iter != value.end(); ++iter)
            {
              iret = Serialization::set_int16(data, data_len, pos, (*iter));
              if (TFS_SUCCESS != iret)
                break;
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int set_vint32(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= get_vint32_length(value) &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, value.size());
          if (TFS_SUCCESS == iret)
          {
            typename T::const_iterator iter = value.begin();
            for (; iter != value.end(); ++iter)
            {
              iret = Serialization::set_int32(data, data_len, pos, (*iter));
              if (TFS_SUCCESS != iret)
                break;
            }
          }
        }
        return iret;
      }
      template <typename T>
      static int set_vint64(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        int32_t iret = NULL != data && data_len - pos >= get_vint64_length(value) &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, value.size());
          if (TFS_SUCCESS == iret)
          {
            typename T::const_iterator iter = value.begin();
            for (; iter != value.end(); ++iter)
            {
              iret = Serialization::set_int64(data, data_len, pos, (*iter));
              if (TFS_SUCCESS != iret)
                break;
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int serialize_list(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        int32_t iret = set_int32(data, data_len, pos, value.size());
        if (TFS_SUCCESS == iret)
        {
          typename T::const_iterator iter = value.begin();
          for (; iter != value.end(); ++iter)
          {
            iret = (*iter).serialize(data, data_len, pos);
            if (TFS_SUCCESS != iret)
            {
              break;
            }
          }
        }
        return iret;
      }

      template <typename T>
      static int deserialize_list(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        int32_t len = 0;
        int32_t iret = Serialization::get_int32(data, data_len, pos, &len);
        if (TFS_SUCCESS == iret)
        {
          for (int32_t i = 0; i < len; ++i)
          {
            typename T::value_type tmp;
            iret = tmp.deserialize(data, data_len, pos);
            if (TFS_SUCCESS != iret)
              break;
            else
              value.push_back(tmp);
          }
        }
        return iret;
      }

      template <typename T>
      static int64_t get_list_length(const T& value)
      {
        int64_t len = INT_SIZE;
        typename T::const_iterator iter = value.begin();
        for (; iter != value.end(); ++iter)
        {
          len += iter->length();
        }
        return len;
      }

      template <typename T>
      static int serialize_kv(char* data, const int64_t data_len, int64_t& pos, const T& value)
      {
        //TODO
        return TFS_SUCCESS;
      }

      template <typename T>
      static int deserialize_kv(const char* data, const int64_t data_len, int64_t& pos, T& value)
      {
        //TODO
        return TFS_SUCCESS;
      }

      template <typename T>
      static int64_t get_kv_length(const T& value)
      {
        //TODO
        return TFS_SUCCESS;
      }
    static int int64_to_char(char* buff, const int32_t buff_size, const int64_t v)
    {
      int ret = TFS_ERROR;
      if (NULL != buff && buff_size >= 8)
      {
        buff[7] = v & 0xFF;
        buff[6] = (v>>8) & 0xFF;
        buff[5] = (v>>16) & 0xFF;
        buff[4] = (v>>24) & 0xFF;
        buff[3] = (v>>32) & 0xFF;
        buff[2] = (v>>40) & 0xFF;
        buff[1] = (v>>48) & 0xFF;
        buff[0] = (v>>56) & 0xFF;
        ret = TFS_SUCCESS;
      }
      return ret;
    }

    static int char_to_int64(char* data, const int32_t data_size, int64_t& v)
    {
      int ret = TFS_ERROR;
      if (data_size >= 8)
      {
        v = static_cast<unsigned char>(data[0]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[1]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[2]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[3]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[4]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[5]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[6]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[7]);
        ret = TFS_SUCCESS;
      }
      return ret;
    }
   };
  }//end namespace comon
}//end namespace tfs

#endif /** TFS_COMMON_SERIALIZATION_H_ */

