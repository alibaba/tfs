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
#ifndef TFS_CLIENT_TAIRUNIQUEHANDLER_H_
#define TFS_CLIENT_TAIRUNIQUEHANDLER_H_

#include "tair_client_api.hpp"
#include "define.hpp"

#include "common/internal.h"
#include "unique_handler.h"

namespace tfs
{
  namespace client
  {
    static const int32_t INVALID_REFERENCE_COUNT = -1;
    static const int32_t UNIQUE_TAIR_TAG_LENGTH = 2; // tairtag length

    inline void add_tair_tag(char* buf, int64_t& pos)
    {
      if (buf != NULL)
      {
        buf[pos] = '\0';
        buf[pos+1] = '\02';
        pos += UNIQUE_TAIR_TAG_LENGTH;
      }
    }

    struct UniqueKey
    {
      UniqueKey();
      ~UniqueKey();

      int deserialize();
      int serialize();
      void set_data(const char* data, const int32_t data_len);
      void clear();

      void get_md5(const char* data, const int32_t length, char* buf, int64_t& pos);

      static const int32_t UNIQUE_MD5_LENGTH = 16; // md5 length
      static const int32_t UNIQUE_FILESIZE_LENGTH = 4; // int32_t length
      static const int32_t UNIQUE_KEY_LENGTH =
        UNIQUE_TAIR_TAG_LENGTH + UNIQUE_MD5_LENGTH + UNIQUE_FILESIZE_LENGTH; // tairtag + md5len + size

      const char* data_;
      int32_t data_len_;

      tair::data_entry* entry_;
    };

    struct UniqueValue
    {
      UniqueValue();
      ~UniqueValue();

      int deserialize();
      int serialize();
      void set_file_name(const char* tfs_name, const char* suffix);
      void reserialize_ref_count(int32_t ref_count);
      void clear();

      static const int32_t UNIQUE_REFCOUNT_LENGTH = 4;
      static const int32_t UNIQUE_VALUE_BASE_LENGTH =
        UNIQUE_TAIR_TAG_LENGTH + UNIQUE_REFCOUNT_LENGTH;

      int32_t version_;
      int32_t ref_count_;
      char file_name_[common::MAX_FILE_NAME_LEN];

      tair::data_entry* entry_;
    };

    class TairUniqueHandler : public UniqueHandler<UniqueKey, UniqueValue>
    {
    public:
      TairUniqueHandler();
      virtual ~TairUniqueHandler();

      int initialize(const char* master_addr, const char* slave_addr,
                     const char* group_name, const int32_t area);

      int query(UniqueKey& key, UniqueValue& value);
      int insert(UniqueKey& key, UniqueValue& value);
      int32_t decrease(UniqueKey& key, UniqueValue& value, const int32_t count = 1);
      int32_t increase(UniqueKey& key, UniqueValue& value, const int32_t count = 1);
      int erase(UniqueKey& key);

    private:
      int tair_get(UniqueKey& key, UniqueValue& value);
      int tair_put(UniqueKey& key, UniqueValue& value);
      int tair_remove(UniqueKey& key);

      const static int32_t TAIR_CLIENT_TRY_COUNT = 2;
      const static int32_t DEFAULT_EXPIRE_TIME = 0;

    private:
      tair::tair_client_api* tair_client_;
      int32_t area_;
    };
  }
}

#endif
