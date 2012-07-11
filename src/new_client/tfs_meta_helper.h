/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *      chuyu(chuyu@taobao.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_NAMEMETA_HELPER_H_
#define TFS_CLIENT_NAMEMETA_HELPER_H_

#include <string>
#include "common/meta_server_define.h"
#include "common/internal.h"

namespace tfs
{
  namespace client
  {
    class NameMetaHelper
    {
    public:
      static int do_file_action(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
          const common::MetaActionOp action, const char* path, const char* new_path, const int64_t version_id);

      static int do_write_file(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
          const char* path, const int64_t version_id, const common::FragInfo& frag_info);

      static int do_read_file(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
          const char* path, const int64_t offset, const int64_t size, const int64_t version_id,
          common::FragInfo& frag_info, bool& still_have);

      static int do_ls(const uint64_t server_id, const int64_t app_id, const int64_t user_id,
          const char* path, const common::FileType file_type, const int64_t pid, const int64_t version_id,
          std::vector<common::MetaInfo>& meta_info, bool& still_have);
      static int get_table(const uint64_t server_id,
          char* table_info, uint64_t& table_length, int64_t& version_id);
    };
  }
}

#endif  // TFS_CLIENT_RCHELPER_H_
