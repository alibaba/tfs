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
#ifndef TFS_CLIENT_NAMEMETA_TFS_FILE_MANAGER_H_
#define TFS_CLIENT_NAMEMETA_TFS_FILE_MANAGER_H_

#include <string>
#include "common/meta_server_define.h"
#include "tfs_client_impl.h"

namespace tfs
{
  namespace client
  {
    const int64_t MAX_WRITE_DATA_IO = 1 << 19;
    class TfsMetaManager
    {
    public:
      int initialize();
      int destroy();
      int32_t get_cluster_id(const char* ns_addr);
      int64_t read_data(const char* ns_addr, const uint32_t block_id, const uint64_t file_id,
          void* buffer, const int32_t pos, const int64_t length);

      int64_t write_data(const char* ns_addr, const void* buffer, const int64_t pos, const int64_t length,
          common::FragMeta& frag_meta);
    };
  }
}

#endif  // TFS_CLIENT_RCHELPER_H_
