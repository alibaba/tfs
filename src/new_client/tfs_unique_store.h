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
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSUNIQUESTORE_H_
#define TFS_CLIENT_TFSUNIQUESTORE_H_

#include "common/internal.h"
#include "tair_unique_handler.h"

namespace tfs
{
  namespace client
  {
    enum UniqueAction
    {
      UNIQUE_ACTION_NONE = 0,
      UNIQUE_ACTION_SAVE_DATA,
      UNIQUE_ACTION_SAVE_DATA_SAVE_META,
      UNIQUE_ACTION_SAVE_DATA_UPDATE_META,
      UNIQUE_ACTION_UPDATE_META
    };

    const static int32_t UNIQUE_FIRST_INSERT_VERSION = 0x0FFFFFFF;

    class TfsUniqueStore
    {
    public:
      TfsUniqueStore();
      ~TfsUniqueStore();

      int initialize(const char* master_addr, const char* slave_addr, const char* group_name, const int32_t area,
                     const char* ns_addr);

      int64_t save(const char* buf, const int64_t count,
                   const char* tfs_name, const char* suffix,
                   char* ret_tfs_name, const int32_t ret_tfs_name_len);

      int64_t save(const char* local_file,
                   const char* tfs_name, const char* suffix,
                   char* ret_tfs_name, const int32_t ret_tfs_name_len);

      int32_t unlink(const char* tfs_name, const char* suffix, int64_t& file_size, const int32_t count);

    private:
      bool check_init();
      bool check_name_match(const char* orig_tfs_name, const char* tfs_name, const char* suffix);
      bool check_suffix_match(const char* orig_tfs_name, const char* suffix);
      bool check_tfsname_match(const char* orig_tfs_name, const char* tfs_name, const char* suffix);
      UniqueAction check_unique(UniqueKey& unique_key, UniqueValue& unique_value,
                           const char* tfs_name, const char* suffix);
      int process(UniqueAction action, UniqueKey& unique_key, UniqueValue& unique_value,
                  const char* tfs_name, const char* suffix,
                  char* ret_tfs_name, const int32_t ret_tfs_name_len);

      int save_data(UniqueKey& unique_key,
                    const char* tfs_name, const char* suffix,
                    char* ret_tfs_name, const int32_t ret_tfs_name_len);
      int save_data_save_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                              const char* tfs_name, const char* suffix,
                              char* ret_tfs_name, const int32_t ret_tfs_name_len);
      int save_data_update_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                                const char* tfs_name, const char* suffix,
                                char* ret_tfs_name, const int32_t ret_tfs_name_len);
      int update_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                      char* ret_tfs_name, const int32_t ret_tfs_name_len);

      int wrap_file_name(const char* tfs_name, char* ret_tfs_name, const int32_t ret_tfs_name_len);
      int64_t get_local_file_size(const char* local_file);
      int read_local_file(const char* local_file, char*& buf, int64_t& count);

    private:
      UniqueHandler<UniqueKey, UniqueValue>* unique_handler_;
      std::string ns_addr_;
    };
  }
}

#endif
