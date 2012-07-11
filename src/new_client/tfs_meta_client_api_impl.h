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
#ifndef TFS_CLIENT_META_CLIENT_API_IMPL_H_
#define TFS_CLIENT_META_CLIENT_API_IMPL_H_

#include <stdio.h>
#include <tbsys.h>
#include <Timer.h>
#include "common/define.h"
#include "common/meta_server_define.h"
#include "common/base_packet_streamer.h"
#include "common/base_packet_factory.h"
#include "tfs_meta_client_api.h"
#include "tfs_meta_manager.h"

namespace tfs
{
  namespace client
  {
    const int64_t MAX_BATCH_DATA_LENGTH = 1 << 23;
    const int64_t MAX_SEGMENT_LENGTH = 1 << 21;
    struct MetaTable
    {
      MetaTable(): version_id_(-1)
      {
      }
      ~MetaTable() {}

      std::vector<uint64_t> v_meta_table_;
      int64_t version_id_;

      void dump();

    };

    class NameMetaClientImpl
    {
      public:
        NameMetaClientImpl();
        ~NameMetaClientImpl();

        //TODO set in conf file, change to private
        int initialize(const char* rs_addr);
        int initialize(const int64_t rs_addr);

        int create_dir(const int64_t app_id, const int64_t uid, const char* dir_path);
        int create_dir_with_parents(const int64_t app_id, const int64_t uid, const char* dir_path);
        int create_file(const int64_t app_id, const int64_t uid, const char* file_path);

        int rm_dir(const int64_t app_id, const int64_t uid, const char* dir_path);
        int rm_file(const char* ns_addr, const int64_t app_id, const int64_t uid, const char* file_path);

        int mv_dir(const int64_t app_id, const int64_t uid,
            const char* src_dir_path, const char* dest_dir_path);
        int mv_file(const int64_t app_id, const int64_t uid,
            const char* src_file_path, const char* dest_file_path);

        int ls_dir(const int64_t app_id, const int64_t uid,
            const char* dir_path,
            std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive = false);
        int ls_file(const int64_t app_id, const int64_t uid,
            const char* file_path, common::FileMetaInfo& frag_info);

        bool is_dir_exist(const int64_t app_id, const int64_t uid, const char* dir_path);
        bool is_file_exist(const int64_t app_id, const int64_t uid, const char* file_path);

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
        int32_t get_cluster_id(const int64_t app_id, const int64_t uid, const char* path);
        //for tools
        int read_frag_info(const int64_t app_id, const int64_t uid,
            const char* file_path, common::FragInfo& frag_info);
        uint64_t get_meta_server_id(int64_t app_id, int64_t uid);

      private:
        //meta server related
        int do_file_action(const int64_t app_id, const int64_t uid,
            common::MetaActionOp action, const char* path, const char* new_path = NULL);
        int ls_dir(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* dir_path, const int64_t pid,
            std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive = false);
        int do_ls_ex(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* file_path, const common::FileType file_type, const int64_t pid,
            std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_chk_exist = false);
        int do_read(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* path, const int64_t offset, const int64_t size,
            common::FragInfo& frag_info, bool& still_have);
        int do_write(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* path, common::FragInfo& frag_info);

        int read_frag_info(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* file_path, common::FragInfo& frag_info);
        int32_t get_cluster_id(const uint64_t meta_server_id, const int64_t app_id, const int64_t uid,
            const char* path);

        // tfs cluster related
        int unlink_file(common::FragInfo& frag_info, const char* ns_addr);
        int64_t read_data(const char* ns_addr, const common::FragInfo& frag_info, void* buffer, int64_t pos, int64_t length);
        int64_t write_data(const char* ns_addr, int32_t cluster_id, const void* buffer, int64_t pos, int64_t length,
            common::FragInfo& frag_info);

        // root server related
        bool need_update_table(const int ret_status);
        int update_table_from_rootserver();

        static bool is_valid_file_path(const char* file_path);
#ifdef TFS_TEST
      public:
#endif
        static int get_parent_dir(char* dir_path, char*& parent_dir_path);

      private:
        DISALLOW_COPY_AND_ASSIGN(NameMetaClientImpl);
        tbsys::CRWLock meta_table_mutex_;
        MetaTable meta_table_;
        uint64_t rs_id_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
        TfsMetaManager tfs_meta_manager_;
    };
  }
}

#endif
