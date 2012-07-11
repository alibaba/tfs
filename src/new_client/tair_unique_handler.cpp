/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#include "md5.h"

#include "Memory.hpp"

#include "common/serialization.h"
#include "common/error_msg.h"
#include "tair_unique_handler.h"

using namespace tair;
using namespace tfs::common;

namespace tfs
{
  namespace client
  {
    //////////////////////
    // UniqueKey
    //////////////////////
    UniqueKey::UniqueKey() : data_(NULL), data_len_(0), entry_(NULL)
    {
    }

    UniqueKey::~UniqueKey()
    {
      tbsys::gDelete(entry_);
    }

    int UniqueKey::serialize()
    {
      int ret = TFS_SUCCESS;
      if (NULL == data_ || data_len_ <= 0)
      {
        TBSYS_LOG(ERROR, "invalid data or data length. data: %p, length: %d", data_, data_len_);
        ret = TFS_ERROR;
      }
      else
      {
        // maybe reinit
        // fixed key length, maybe reuse
        tbsys::gDelete(entry_);

        entry_ = new data_entry();
        char* key_buf = new char[UNIQUE_KEY_LENGTH];
        int64_t pos = 0;

        // add tair tag
        add_tair_tag(key_buf, pos);
        // md5
        get_md5(data_, data_len_, key_buf, pos);
        // size
        Serialization::set_int32(key_buf, UNIQUE_KEY_LENGTH, pos, data_len_);

        entry_->set_alloced_data(key_buf, UNIQUE_KEY_LENGTH);
      }

      return ret;
    }

    int UniqueKey::deserialize()
    {
      // do nothing
      return TFS_SUCCESS;
    }

    void UniqueKey::set_data(const char* data, const int32_t data_len)
    {
      data_ = data;
      data_len_ = data_len;
    }

    void UniqueKey::get_md5(const char* data, const int32_t length, char* buf, int64_t& pos)
    {
      md5_context md5;
      md5_starts(&md5);
      md5_update(&md5, (unsigned char*)data, length);
      md5_finish(&md5, reinterpret_cast<unsigned char*>(buf) + pos);
      pos += UNIQUE_MD5_LENGTH;
    }

    //////////////////////
    // UniqueValue
    //////////////////////
    UniqueValue::UniqueValue() : version_(0), ref_count_(0), entry_(NULL)
    {
      file_name_[0] = '\0';
    }

    UniqueValue::~UniqueValue()
    {
      clear();
    }

    int UniqueValue::serialize()
    {
      int ret = TFS_SUCCESS;
      if ('\0' == file_name_[0] || ref_count_ <= 0 || version_ < 0)
      {
        TBSYS_LOG(ERROR, "invalid value. filename: %s, refcount: %d, version: %d",
                  file_name_, ref_count_, version_);
        ret = TFS_ERROR;
      }
      else
      {
        // maybe reinit
        tbsys::gDelete(entry_);

        entry_ = new data_entry();
        int32_t file_name_len = strlen(file_name_);
        int32_t value_length = UNIQUE_VALUE_BASE_LENGTH + file_name_len;
        char* value_buf = new char[value_length];
        int64_t pos = 0;

        // add tair tag
        add_tair_tag(value_buf, pos);
        // reference count
        Serialization::set_int32(value_buf, UNIQUE_VALUE_BASE_LENGTH, pos, ref_count_);
        // file name
        Serialization::set_bytes(value_buf, value_length, pos, file_name_, file_name_len);

        entry_->set_alloced_data(value_buf, value_length);
        TBSYS_LOG(DEBUG, "serialization value: %d %s", ref_count_, file_name_);
      }

      return ret;
    }

    int UniqueValue::deserialize()
    {
      int ret = TFS_SUCCESS;

      if (NULL == entry_ || entry_->get_size() < UNIQUE_VALUE_BASE_LENGTH + FILE_NAME_LEN)
      {
        TBSYS_LOG(ERROR, "invalid data entry to deserialize. entry: %p, length: %d",
                  entry_, NULL == entry_ ? -1 : entry_->get_size());
        ret = TFS_ERROR;
      }
      else
      {
        char* buf = entry_->get_data();
        int32_t buf_len = entry_->get_size();
        int64_t pos = UNIQUE_TAIR_TAG_LENGTH;
        int32_t file_name_len = buf_len - UNIQUE_VALUE_BASE_LENGTH;

        TBSYS_LOG(DEBUG, "deserialize value. value len: %d, filename len: %d", buf_len, file_name_len);
        if (file_name_len >= MAX_FILE_NAME_LEN)
        {
          TBSYS_LOG(ERROR, "file name stored length larger than max value: %d >= %d",
                    file_name_len, MAX_FILE_NAME_LEN);
          ret = TFS_ERROR;
        }
        else
        {
          // version
          version_ = entry_->get_version();
          // refcount
          Serialization::get_int32(buf, buf_len, pos, &ref_count_);
          // file name
          Serialization::get_bytes(buf, buf_len, pos, file_name_, file_name_len);
          file_name_[file_name_len] = '\0';
          TBSYS_LOG(DEBUG, "deserialize value: %d %d %s", version_, ref_count_, file_name_);
        }
      }
      return ret;
    }

    void UniqueValue::set_file_name(const char* tfs_name, const char* suffix)
    {
      if (tfs_name != NULL)
      {
        strncpy(file_name_, tfs_name, FILE_NAME_LEN);
        file_name_[FILE_NAME_LEN] = '\0';
      }

      if (suffix != NULL)
      {
        // just copy max suffix length
        strncpy(file_name_ + FILE_NAME_LEN, suffix, MAX_SUFFIX_LEN);
        file_name_[MAX_FILE_NAME_LEN-1] = '\0';
      }
      TBSYS_LOG(DEBUG, "set file name. filename: %s, suffix: %s, valuefilename: %s", tfs_name, suffix, file_name_);
    }

    void UniqueValue::reserialize_ref_count(int32_t ref_count)
    {
      ref_count_ = ref_count;
      int64_t pos = UNIQUE_TAIR_TAG_LENGTH;
      Serialization::set_int32(entry_->get_data(), UNIQUE_VALUE_BASE_LENGTH, pos, ref_count_);
    }

    void UniqueValue::clear()
    {
      tbsys::gDelete(entry_);
      file_name_[0] = '\0';
    }

    //////////////////////
    // TairUniqueHandler
    //////////////////////
    TairUniqueHandler::TairUniqueHandler() :
      tair_client_(NULL), area_(0)
    {
    }

    TairUniqueHandler::~TairUniqueHandler()
    {
      tbsys::gDelete(tair_client_);
    }

    int TairUniqueHandler::initialize(const char* master_addr, const char* slave_addr,
                                      const char* group_name, const int32_t area)
    {
      int ret = TFS_SUCCESS;

      if (area <= 0)
      {
        TBSYS_LOG(ERROR, "invalid area: %d", area);
        ret = TFS_ERROR;
      }
      else
      {
        tbsys::gDelete(tair_client_);
        tair_client_ = new tair_client_api();

        if (!tair_client_->startup(master_addr, slave_addr, group_name))
        {
          TBSYS_LOG(ERROR, "starup tair client fail. master addr: %s, slave addr: %s, group name: %s, area: %d",
                    master_addr, slave_addr, group_name, area);
          ret = TFS_ERROR;
        }
        else
        {
          area_ = area;
        }
      }

      return ret;
    }

    int TairUniqueHandler::query(UniqueKey& key, UniqueValue& value)
    {
      int ret = TFS_ERROR;

      if ((ret = key.serialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialiaze key fail. ret: %d", ret);
      }
      else if ((ret = tair_get(key, value)) != TAIR_RETURN_SUCCESS)
      {
        TBSYS_LOG(INFO, "get value from tair fail, ret: %d", ret);
        if (TAIR_RETURN_DATA_NOT_EXIST == ret)
        {
          ret = EXIT_UNIQUE_META_NOT_EXIST;
        }
        else
        {
          ret = TFS_ERROR;
        }
      }
      else
      {
        ret = TFS_SUCCESS;
      }

      return ret;
    }

    int TairUniqueHandler::insert(UniqueKey& key, UniqueValue& value)
    {
      int ret = TFS_ERROR;

      if ((ret = key.serialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialiaze key fail. ret: %d", ret);
      }
      else if ((ret = value.serialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "value serialize fail. ret: %d", ret);
      }
      else
      {
        int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
        while (1)
        {
          ret = tair_put(key, value);

          if (ret != TAIR_RETURN_VERSION_ERROR)
          {
            break;
          }

          TBSYS_LOG(WARN, "insert tair version error occur. ret: %d, retry: %d",
                    ret, TAIR_CLIENT_TRY_COUNT - retry_count + 1);

          if (retry_count-- <= 0)
          {
            break;
          }

          // reuse value.
          // get success, value is deserialized,
          // only change refcount, no need serialization, just update refcount
          if (TAIR_RETURN_SUCCESS == (ret = tair_get(key, value)))
          {
            value.reserialize_ref_count(value.ref_count_ + 1);
          }
          else
          {
            TBSYS_LOG(ERROR, "reget value from tair fail. ret: %d", ret);
            break;
          }
        }
        ret = TAIR_RETURN_SUCCESS == ret ? TFS_SUCCESS : TFS_ERROR;
      }

      return ret;
    }

    int32_t TairUniqueHandler::decrease(UniqueKey& key, UniqueValue& value, const int32_t count)
    {
      int32_t ret = TFS_SUCCESS;

      if (count <= 0)
      {
        TBSYS_LOG(ERROR, "invalid decrease count: %d", count);
        ret = TFS_ERROR;
      }
      else if (value.ref_count_ <= count)
      {
        // CAUTION: comment this line for histroy problem, avoid unlinking file on line,
        // uncomment this line for normal use
        //ret = erase(key);
        value.ref_count_ = 0;
      }
      else
      {
        value.ref_count_ -= count;
        ret = insert(key, value);
      }

      return TFS_SUCCESS == ret ? value.ref_count_ : INVALID_REFERENCE_COUNT;
    }

    int32_t TairUniqueHandler::increase(UniqueKey& key, UniqueValue& value, const int32_t count)
    {
      int ret = TFS_ERROR;

      if (count <= 0)
      {
        TBSYS_LOG(ERROR, "invalid increase count: %d", count);
      }
      else
      {
        value.ref_count_ += count;
        ret = insert(key, value);
      }

      return TFS_SUCCESS == ret ? value.ref_count_ : INVALID_REFERENCE_COUNT;
    }

    int TairUniqueHandler::erase(UniqueKey& key)
    {
      int ret = TFS_ERROR;

      if ((ret = key.serialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "key serialiaze fail. ret: %d", ret);
      }
      else
      {
        ret = tair_remove(key);

        if (ret != TAIR_RETURN_SUCCESS)
        {
          TBSYS_LOG(ERROR, "remove tair key fail. ret: %d", ret);
          ret = TFS_ERROR;
        }
        else
        {
          ret = TFS_SUCCESS;
        }
      }

      return ret;
    }

    int TairUniqueHandler::tair_get(UniqueKey& key, UniqueValue& value)
    {
      int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
      int ret = TAIR_RETURN_SUCCESS;

      value.clear();
      do
      {
        ret = tair_client_->get(area_, *(key.entry_), value.entry_);
      } while (TAIR_RETURN_TIMEOUT == ret && retry_count-- > 0);

      if (TAIR_RETURN_SUCCESS == ret)
      {
        // value deserialize here
        if ((ret = value.deserialize()) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "deserialize value fail. ret: %d", ret);
          ret = TAIR_RETURN_FAILED;
        }
      }

      return ret;
    }

    int TairUniqueHandler::tair_put(UniqueKey& key, UniqueValue& value)
    {
      int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
      int ret = TAIR_RETURN_SUCCESS;

      do
      {
        ret = tair_client_->put(area_, *key.entry_, *value.entry_, DEFAULT_EXPIRE_TIME, value.version_);
      } while (TAIR_RETURN_TIMEOUT == ret && retry_count-- > 0);

      return ret;
    }

    int TairUniqueHandler::tair_remove(UniqueKey& key)
    {
      int32_t retry_count = TAIR_CLIENT_TRY_COUNT;
      int ret = TAIR_RETURN_SUCCESS;

      do
      {
        ret = tair_client_->remove(area_, *(key.entry_));
      } while (TAIR_RETURN_TIMEOUT == ret && retry_count-- > 0);

      // ignore TAIR_RETURN_VERSION_ERROR
      return ret;
    }

  }
}


