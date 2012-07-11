/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_client_api.cpp 943 2011-10-20 01:40:25Z chuyu $
 *
 * Authors:
 *    daoan<aoan@taobao.com>
 *      - initial release
 *
 */
#include "common/define.h"
#include "tfs_meta_client_api.h"
#include "tfs_meta_client_api_impl.h"

namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    NameMetaClient::NameMetaClient():impl_(NULL)
    {
      impl_ = new NameMetaClientImpl();
    }

    NameMetaClient::~NameMetaClient()
    {
      delete impl_;
      impl_ = NULL;
    }

    int NameMetaClient::initialize(const char* rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    int NameMetaClient::initialize(const int64_t rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    TfsRetType NameMetaClient::create_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      return impl_->create_dir(app_id, uid, dir_path);
    }

    TfsRetType NameMetaClient::create_dir_with_parents(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      return impl_->create_dir_with_parents(app_id, uid, dir_path);
    }

    TfsRetType NameMetaClient::create_file(const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      return impl_->create_file(app_id, uid, file_path);
    }

    TfsRetType NameMetaClient::rm_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      return impl_->rm_dir(app_id, uid, dir_path);
    }

    TfsRetType NameMetaClient::rm_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      return impl_->rm_file(ns_addr, app_id, uid, file_path);
    }

    TfsRetType NameMetaClient::mv_dir(const int64_t app_id, const int64_t uid,
        const char* src_dir_path, const char* dest_dir_path)
    {
      return impl_->mv_dir(app_id, uid, src_dir_path, dest_dir_path);
    }

    TfsRetType NameMetaClient::mv_file(const int64_t app_id, const int64_t uid,
        const char* src_file_path, const char* dest_file_path)
    {
      return impl_->mv_file(app_id, uid, src_file_path, dest_file_path);
    }

    TfsRetType NameMetaClient::ls_dir(const int64_t app_id, const int64_t uid,
        const char* dir_path, std::vector<FileMetaInfo>& v_file_meta_info,
        bool is_recursive)
    {
      return impl_->ls_dir(app_id, uid, dir_path, v_file_meta_info, is_recursive);
    }

    TfsRetType NameMetaClient::ls_file(const int64_t app_id, const int64_t uid,
        const char* file_path, FileMetaInfo& file_meta_info)
    {
      return impl_->ls_file(app_id, uid, file_path, file_meta_info);
    }

    bool NameMetaClient::is_dir_exist(const int64_t app_id, const int64_t uid,
        const char* dir_path)
    {
      return impl_->is_dir_exist(app_id, uid, dir_path);
    }

    bool NameMetaClient::is_file_exist(const int64_t app_id, const int64_t uid,
        const char* file_path)
    {
      return impl_->is_file_exist(app_id, uid, file_path);
    }

    int32_t NameMetaClient::get_cluster_id(const int64_t app_id, const int64_t uid, const char* path)
    {
      return impl_->get_cluster_id(app_id, uid, path);
    }

    int64_t NameMetaClient::read(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* file_path, void* buffer, int64_t offset, int64_t length)
    {
      return impl_->read(ns_addr, app_id, uid, file_path, buffer, offset, length);
    }

    int64_t NameMetaClient::write(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* file_path, const void* buffer, const int64_t length)
    {
      return impl_->write(ns_addr, app_id, uid, file_path, buffer, length);
    }

    int64_t NameMetaClient::write(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* file_path, const void* buffer, const int64_t offset, const int64_t length)
    {
      return impl_->write(ns_addr, app_id, uid, file_path, buffer, offset, length);
    }

    int64_t NameMetaClient::save_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* local_file, const char* tfs_name)
    {
      return impl_->save_file(ns_addr, app_id, uid, local_file, tfs_name);
    }

    int64_t NameMetaClient::fetch_file(const char* ns_addr, const int64_t app_id, const int64_t uid,
        const char* local_file, const char* tfs_name)
    {
      return impl_->fetch_file(ns_addr, app_id, uid, local_file, tfs_name);
    }
  }
}
