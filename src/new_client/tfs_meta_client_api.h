/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *      chuyu(chuyu@taobao.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_META_CLIENT_API_H_
#define TFS_CLIENT_META_CLIENT_API_H_

#include <string>
#include "common/define.h"
#include "common/stream.h"
#include "common/meta_server_define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class NameMetaClientImpl;
    class NameMetaClient
    {
      public:
        NameMetaClient();
        ~NameMetaClient();

        int initialize(const char* rs_addr);
        int initialize(const int64_t rs_addr);

        TfsRetType create_dir(const int64_t app_id, const int64_t uid, const char* dir_path);
        TfsRetType create_dir_with_parents(const int64_t app_id, const int64_t uid, const char* dir_path);
        TfsRetType create_file(const int64_t app_id, const int64_t uid, const char* file_path);

        TfsRetType rm_dir(const int64_t app_id, const int64_t uid, const char* dir_path);
        TfsRetType rm_file(const char* ns_addr, const int64_t app_id, const int64_t uid, const char* file_path);

        TfsRetType mv_dir(const int64_t app_id, const int64_t uid,
            const char* src_dir_path, const char* dest_dir_path);
        TfsRetType mv_file(const int64_t app_id, const int64_t uid,
            const char* src_file_path, const char* dest_file_path);

        TfsRetType ls_dir(const int64_t app_id, const int64_t uid,
            const char* dir_path,
            std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive = false);
        TfsRetType ls_file(const int64_t app_id, const int64_t uid,
            const char* file_path,
            common::FileMetaInfo& file_meta_info);

        bool is_dir_exist(const int64_t app_id, const int64_t uid, const char* dir_path);
        bool is_file_exist(const int64_t app_id, const int64_t uid, const char* file_path);

        int32_t get_cluster_id(const int64_t app_id, const int64_t uid, const char* path);

        int64_t read(const char* ns_addr, const int64_t app_id, const int64_t uid,
            const char* file_path, void* buffer, const int64_t offset, const int64_t length);
        int64_t write(const char* ns_addr, const int64_t app_id, const int64_t uid,
            const char* file_path, const void* buffer, const int64_t length);
        int64_t write(const char* ns_addr, const int64_t app_id, const int64_t uid,
            const char* file_path, const void* buffer, const int64_t offset, const int64_t length);

        int64_t save_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
            const char* local_file, const char* tfs_name);
        int64_t fetch_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
            const char* local_file, const char* tfs_name);

      private:
        DISALLOW_COPY_AND_ASSIGN(NameMetaClient);
        NameMetaClientImpl* impl_;

    };
  }
}

#endif
