/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id: meta_store_manager.h 1003 2011-11-03 03:11:52Z daoan@taobao.com $
*
* Authors:
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#define TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#include "Memory.hpp"
#include "common/meta_server_define.h"

#include "mysql_database_helper.h"
#include "database_pool.h"
#include "meta_cache_info.h"
#include "lru.h"

namespace tfs
{
  namespace namemetaserver
  {
    class MetaStoreManager
    {
      public:
        MetaStoreManager();
        ~MetaStoreManager();
        int init(const int32_t pool_size, const int32_t cache_size,
            const double gc_ratio, const int32_t mutex_count);
        int destroy(void);
        tbsys::CThreadMutex* get_mutex(const int64_t app_id, const int64_t uid);

        void do_lru_gc(const double ratio);
        void do_lru_gc(const std::set<int64_t>& change);
        //this func will get root_node from lru cache
        //when we gen a new root node we will ls '/'
        //so dir_meta_ == NULL means no top dir;
        CacheRootNode* get_root_node(const int64_t app_id, const int64_t uid);
        void revert_root_node(CacheRootNode* root_node);

        int create_top_dir(const int64_t app_id, const int64_t uid, CacheRootNode* root_node);

        int select(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
            const char* name, const bool is_file, void*& ret_node);

        int insert(const int64_t app_id, const int64_t uid,
            const int64_t pp_id, CacheDirMetaNode* p_dir_node,
            const char* name, const int32_t name_len,
            const common::FileType type, common::MetaInfo* meta_info = NULL);

        int update(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, CacheDirMetaNode* s_p_dir_node,
            const int64_t d_ppid, CacheDirMetaNode* d_p_dir_node,
            const char* s_name, const char* d_name,
            const common::FileType type);

        int remove(const int64_t app_id, const int64_t uid, int64_t ppid,
            CacheDirMetaNode* p_dir_node, void* node_will_removed, const common::FileType type);

        int get_dir_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
            const char* name, const int32_t name_len, common::MetaInfo& out_meta_info);
        static void calculate_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
          const std::vector<common::MetaInfo>::iterator meta_info_end, const bool ls_file,
          std::vector<common::MetaInfo>& v_meta_info, common::MetaInfo& last_meta_info);
      public:
        int get_file_frag_info(const int64_t app_id, const int64_t uid,
            CacheDirMetaNode* p_dir_node, CacheFileMetaNode* file_node,
            const int64_t offset, std::vector<common::MetaInfo>& out_v_meta_info,
            int32_t& cluster_id, int64_t& last_offset);

        int get_meta_info_from_db(const int64_t app_id, const int64_t uid, const int64_t pid,
            const char* name, const int32_t name_len,
            const int64_t offset, std::vector<common::MetaInfo>& tmp_v_meta_info,
            int32_t& cluster_id, int64_t& last_offset);

        int select(const int64_t app_id, const int64_t uid,
            const int64_t pid, const char* name, const int32_t name_len, const bool is_file,
            std::vector<common::MetaInfo>& out_v_meta_info);

        int ls(const int64_t app_id, const int64_t uid, const int64_t pid,
            const char* name, const int32_t name_len,
            const char* name_end, const int32_t name_end_len,
            const bool is_file,
            std::vector<common::MetaInfo>& out_v_meta_info, bool& still_have);

        int insert(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const char* name, const int32_t name_len,
            const common::FileType type, common::MetaInfo* meta_info = NULL);

        int update(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len,
            const common::FileType type);

        int remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int64_t name_len,
            const common::FileType type);
        uint64_t get_cache_get_times() const {return cache_get_times_;}
        uint64_t get_cache_hit_times() const {return cache_hit_times_;}
        float get_cache_hit_ratio() { return cache_get_times_ == 0? cache_hit_times_ = 0: (float)cache_hit_times_/(float)cache_get_times_;}

      private:
        void* malloc(const int64_t size, const int32_t type = CACHE_NONE_NODE);
        int ls_top_dir(const int64_t app_id, const int64_t uid, CacheRootNode* root_node,
            const bool is_new_dir);
        int get_children_from_db(const int64_t app_id, const int64_t uid,
            CacheDirMetaNode* p_dir_node, const char* name, bool is_file);

        int fill_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
            const std::vector<common::MetaInfo>::iterator meta_info_end,
            CacheDirMetaNode* p_dir_node);

        int fill_dir_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
            const std::vector<common::MetaInfo>::iterator meta_info_end,
            CacheDirMetaNode* p_dir_node);

        int get_return_status(const int status, const int proc_ret);
        int force_rc(const int32_t need_size);

      private:
        char top_dir_name_[10];
        int32_t top_dir_size_;
        int32_t cache_size_;   //MB
        uint64_t cache_get_times_;
        uint64_t cache_hit_times_;
        double gc_ratio_;
        DISALLOW_COPY_AND_ASSIGN(MetaStoreManager);
        DataBasePool* database_pool_;
        tbsys::CThreadMutex lru_mutex_;
        Lru lru_;
        int mutex_count_;
        tbsys::CThreadMutex* app_id_uid_mutex_;
    };
  }
}

#endif
