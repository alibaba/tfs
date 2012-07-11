/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id: meta_store_manager.cpp 1007 2011-11-03 08:14:39Z daoan@taobao.com $
*
* Authors:
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#include "meta_store_manager.h"

#include "common/parameter.h"
#include "common/error_msg.h"
#include "database_helper.h"
#include "meta_cache_helper.h"
#include "meta_server_service.h"

using namespace tfs::common;
namespace tfs
{
  namespace namemetaserver
  {
    using namespace std;
    MetaStoreManager::MetaStoreManager():
      cache_size_(1024), cache_get_times_(0), cache_hit_times_(0),
      gc_ratio_(0.1), mutex_count_(16), app_id_uid_mutex_(NULL)
    {
       database_pool_ = new DataBasePool();
       top_dir_name_[0] = 1;
       top_dir_name_[1] = '/';
       top_dir_size_ = 2;
    }
    int MetaStoreManager::init(const int32_t pool_size, const int32_t cache_size,
        const double gc_ratio, const int32_t mutex_count)
    {
      char* conn_str[DataBasePool::MAX_POOL_SIZE];
      char* user_name[DataBasePool::MAX_POOL_SIZE];
      char* passwd[DataBasePool::MAX_POOL_SIZE];
      int32_t hash_flag[DataBasePool::MAX_POOL_SIZE];

      int32_t my_pool_size = pool_size;
      int ret = TFS_SUCCESS;
      cache_size_ = cache_size;
      /*if (cache_size_ < 200)//TODO
      {
        cache_size_ = 200;
        TBSYS_LOG(WARN, "change cache_size to %d MB", cache_size_);
      }*/
      TBSYS_LOG(INFO, "set cache size %d MB", cache_size_);
      gc_ratio_ = gc_ratio;
      if (gc_ratio_ >= 1.0 || gc_ratio_ < 0.0)
      {
        gc_ratio_ = 0.1;
        TBSYS_LOG(WARN, "change gc_ratio to 0.1");
      }
      TBSYS_LOG(INFO, "set gc_ratio to %g", gc_ratio_);

      mutex_count_ = mutex_count;

      if (mutex_count_ < 5)
      {
        mutex_count_ = 5;
        TBSYS_LOG(WARN, "change mutex_count to %d", mutex_count_);
      }
      TBSYS_LOG(INFO, "set mutex_count to  %d", mutex_count_);
      if (pool_size > DataBasePool::MAX_POOL_SIZE)
      {
        TBSYS_LOG(INFO, "pool size is too lage set it to %d",
            DataBasePool::MAX_POOL_SIZE);
        my_pool_size = DataBasePool::MAX_POOL_SIZE;
      }
      for (int i = 0; i < my_pool_size; i++)
      {
        int data_base_index = i % SYSPARAM_NAMEMETASERVER.db_infos_.size();
        const NameMetaServerParameter::DbInfo& dbinfo = SYSPARAM_NAMEMETASERVER.db_infos_[data_base_index];

        conn_str[i] = (char*)::malloc(100);
        snprintf(conn_str[i], 100, "%s", dbinfo.conn_str_.c_str());
        user_name[i] = (char*)::malloc(100);
        snprintf(user_name[i], 100, "%s", dbinfo.user_.c_str());
        passwd[i] = (char*)::malloc(100);
        snprintf(passwd[i], 100, "%s", dbinfo.passwd_.c_str());
        hash_flag[i] = dbinfo.hash_value_;
      }
      bool pool_ret = database_pool_->init_pool(my_pool_size,
          conn_str, user_name, passwd, hash_flag);

      if(!pool_ret)
      {
        TBSYS_LOG(ERROR, "database pool init error");
        ret = TFS_ERROR;
      }
      for (int i = 0; i < my_pool_size; i++)
      {
        ::free(conn_str[i]);
        conn_str[i] = NULL;
        ::free(user_name[i]);
        user_name[i] = NULL;
        ::free(passwd[i]);
        passwd[i] = NULL;
      }
      app_id_uid_mutex_ = new tbsys::CThreadMutex[mutex_count_];
      return ret;
    }

    int MetaStoreManager::destroy(void)
    {
      if (NULL != database_pool_)
      {
        database_pool_->destroy_pool();
      }
      return TFS_SUCCESS;
    }

    MetaStoreManager::~MetaStoreManager()
    {
      vector<CacheRootNode*> v_root_node;
      lru_.gc(v_root_node);
      vector<CacheRootNode*>::iterator it = v_root_node.begin();
      for (; it != v_root_node.end(); it++)
      {
        MetaCacheHelper::free(*it);
      }
      TBSYS_LOG(DEBUG, "used size is %"PRI64_PREFIX"d", MemHelper::get_used_size());
      delete database_pool_;
      delete [] app_id_uid_mutex_;
    }

    tbsys::CThreadMutex* MetaStoreManager::get_mutex(const int64_t app_id, const int64_t uid)
    {
      assert(NULL != app_id_uid_mutex_);
      assert(mutex_count_ > 0);
      HashHelper helper(app_id, uid);
      int32_t hash_value = tbsys::CStringUtil::murMurHash((const void*)&helper, sizeof(HashHelper))
        % mutex_count_;
      TBSYS_LOG(DEBUG, "app_id: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d get mutex: %d", app_id, uid, hash_value);
      return app_id_uid_mutex_ + hash_value;
    }

    void MetaStoreManager::do_lru_gc(const double ratio)
    {
      int64_t used_size = MemHelper::get_used_size();
      used_size = used_size >> 20;
      BaseStrategy strategy(lru_);
      vector<CacheRootNode*> v_root_node;
      if ((double)used_size/(double)cache_size_ > (1 - ratio/2))
      {
        tbsys::CThreadGuard mutex_guard(&lru_mutex_);
        if (TFS_SUCCESS != lru_.gc(ratio, &strategy, v_root_node))
        {
          TBSYS_LOG(ERROR, "lru gc error");
        }
      }
      TBSYS_LOG(INFO, "gc %zd root node", v_root_node.size());
      vector<CacheRootNode*>::iterator it = v_root_node.begin();
      for (; it != v_root_node.end(); it++)
      {
        TBSYS_LOG(INFO, "gc app_id: %"PRI64_PREFIX"d uid: %"PRI64_PREFIX"d bucket: %"PRI64_PREFIX"d root node",
          (*it)->key_.app_id_, (*it)->key_.uid_, (*it)->key_.get_hash());
        MetaCacheHelper::free(*it);
      }
      return ;
    }

    void MetaStoreManager::do_lru_gc(const std::set<int64_t>& change)
    {
      lru_.gc(change);
      std::set<int64_t>::const_iterator iter = change.begin();
      for (; iter != change.end(); ++iter)
      {
        TBSYS_LOG(INFO, "gc bucket: %"PRI64_PREFIX"d", (*iter));
      }
    }

    CacheRootNode* MetaStoreManager::get_root_node(const int64_t app_id, const int64_t uid)
    {
      AppIdUid lru_key(app_id, uid);
      CacheRootNode* root_node = NULL;
      {
        tbsys::CThreadGuard mutex_guard(&lru_mutex_);
        root_node = lru_.get(lru_key);
      }
      if (NULL == root_node)
      {
        CacheRootNode* tmp_root_node = (CacheRootNode*)malloc(sizeof(CacheRootNode), CACHE_ROOT_NODE);
        if(NULL != tmp_root_node)
        {
          tmp_root_node->key_.app_id_ = app_id;
          tmp_root_node->key_.uid_ = uid;
          tmp_root_node->dir_meta_ = NULL;

          //if root not exist in lru, we need ls '/' and put the result ro lru
          ls_top_dir(app_id, uid, tmp_root_node, false);
          {
            tbsys::CThreadGuard mutex_guard(&lru_mutex_);
            root_node = lru_.get(lru_key);
            if (NULL == root_node)
            {
              int tmp_ret = lru_.insert(lru_key, tmp_root_node);
              assert (TFS_SUCCESS == tmp_ret);
              tmp_root_node = NULL;
              root_node = lru_.get(lru_key);
            }
          }
          if (NULL != tmp_root_node)
          {
            MetaCacheHelper::free(tmp_root_node);
          }
        }
      }
      return root_node;
    }
    void MetaStoreManager::revert_root_node(CacheRootNode* root_node)
    {
      tbsys::CThreadGuard mutex_guard(&lru_mutex_);
      lru_.put(root_node);
      return;
    }
    int MetaStoreManager::create_top_dir(const int64_t app_id, const int64_t uid, CacheRootNode* root_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL == root_node->dir_meta_);
      ret = insert(app_id, uid, 0, NULL, 0, 0, top_dir_name_, top_dir_size_, DIRECTORY);
      if (TFS_SUCCESS == ret)
      {
        ret = ls_top_dir(app_id, uid, root_node, true);
      }
      return ret;
    }
    void* MetaStoreManager::malloc(const int64_t size, const int32_t type)
    {
      int64_t used_size = MemHelper::get_used_size();
      TBSYS_LOG(DEBUG, "malloc size %"PRI64_PREFIX"d type %d cache size %d MB used size %"PRI64_PREFIX"d",
          size, type, cache_size_, used_size);
      used_size = used_size >> 20;
      if (cache_size_ <= used_size)
      {
        do_lru_gc(gc_ratio_);
      }
      return MemHelper::malloc(size, type);
    }

    int MetaStoreManager::ls_top_dir(const int64_t app_id, const int64_t uid, CacheRootNode* root_node,
        const bool new_dir)
    {
      int ret = TFS_ERROR;
      vector<MetaInfo> out_v_meta_info;
      bool still_have;
      ret = ls(app_id, uid, 0, top_dir_name_, top_dir_size_,
          NULL, 0, false, out_v_meta_info, still_have);
      if (TFS_SUCCESS == ret && !out_v_meta_info.empty())
      {
        root_node->dir_meta_ = (CacheDirMetaNode*)malloc(sizeof(CacheDirMetaNode), CACHE_DIR_META_NODE);
        assert(NULL != root_node->dir_meta_);
        FileMetaInfo& file_info = out_v_meta_info[0].file_info_;
        root_node->dir_meta_->id_ = file_info.id_;
        root_node->dir_meta_->create_time_ = file_info.create_time_;
        root_node->dir_meta_->modify_time_ = file_info.modify_time_;
        root_node->dir_meta_->name_ = top_dir_name_; //do not free top_dir name
        root_node->dir_meta_->version_ = file_info.ver_no_;
        root_node->dir_meta_->child_dir_infos_ = NULL;
        root_node->dir_meta_->child_file_infos_ = NULL;
        root_node->dir_meta_->flag_ = 0;
        if (new_dir)
        {
          root_node->dir_meta_->set_got_all_children();
        }
      }
      return ret;
    }

    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
                                 const char* name, const bool is_file, void*& ret_node)
    {
      int ret = TFS_SUCCESS;
      ret_node = NULL;
      if (NULL == name || NULL == p_dir_node)
      {
        TBSYS_LOG(ERROR, "parameter error");
        ret = TFS_ERROR;
      }
      bool got_all = false;
      if (TFS_SUCCESS == ret)
      {
        if (!(app_id == -1 && uid == -1)) cache_get_times_++;
        if (is_file)
        {
          CacheFileMetaNode* ret_file_node = NULL;
          ret = MetaCacheHelper::find_file(p_dir_node, name, ret_file_node);
          ret_node = ret_file_node;
        }
        else
        {
          CacheDirMetaNode* ret_dir_node = NULL;
          ret = MetaCacheHelper::find_dir(p_dir_node, name, ret_dir_node);
          ret_node = ret_dir_node;
        }
        if (!(app_id == -1 && uid == -1) && NULL != ret_node) cache_hit_times_++;
        got_all = p_dir_node->is_got_all_children();
      }
      if (TFS_SUCCESS == ret)
      {
        if (NULL == ret_node && !got_all && !(app_id == -1 && uid == -1))
        {
          //assert (app_id != 0);
          //assert (uid != 0);
          ret = get_children_from_db(app_id, uid, p_dir_node, name, is_file);
          if (TFS_SUCCESS == ret)
          {
            ret = select(-1, -1, p_dir_node, name, is_file, ret_node);
          }
        }
      }
      return ret;
    }

    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
                                 const int64_t pp_id, CacheDirMetaNode* p_dir_node,
                                 const char* name, const int32_t name_len,
                                 const common::FileType type, common::MetaInfo* meta_info)
    {
      int ret = TFS_SUCCESS;
      int32_t p_name_len = 0;
      //int64_t dir_id = 0;
      //we only cache the first line for file
      if (NULL == p_dir_node)
      {
        TBSYS_LOG(ERROR, "prameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        if (NORMAL_FILE == type)
        {
          int32_t files_count = MetaCacheHelper::get_sub_files_count(p_dir_node);
          if (files_count > SYSPARAM_NAMEMETASERVER.max_sub_files_count_)
          {
            ret = EXIT_OVER_MAX_SUB_FILES_COUNT;
          }
        }
        else if(DIRECTORY == type)
        {
          int32_t dirs_count = MetaCacheHelper::get_sub_dirs_count(p_dir_node);
          if (dirs_count > SYSPARAM_NAMEMETASERVER.max_sub_dirs_count_)
          {
            ret = EXIT_OVER_MAX_SUB_DIRS_COUNT;
          }
        }

      }
      if (TFS_SUCCESS == ret)
      {
        p_name_len = FileName::length(p_dir_node->name_);
        ret = insert(app_id, uid, pp_id, p_dir_node->name_,
                     p_name_len, p_dir_node->id_,
                     name, name_len, type, meta_info);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "insert into db error, ret : %d, app_id: %"PRI64_PREFIX"d,uid: %"PRI64_PREFIX"d, name: %s", ret,
            app_id, uid, name);
        }
        else
        {
          vector<MetaInfo> out_v_meta_info;
          bool still_have;
          char name_end[MAX_FILE_PATH_LEN];
          int32_t name_end_len = 0;
          MetaServerService::next_file_name_base_on(name_end, name_end_len, name, name_len);
          TBSYS_LOG(DEBUG, "ls %ld %ld %ld %.*s", app_id, uid, p_dir_node->id_, name_len-1, name+1);
          //use ls so we can get create time, modify time from db;
          ret = ls(app_id, uid, p_dir_node->id_, name, name_len,
              name_end, name_end_len,
              type != DIRECTORY, out_v_meta_info, still_have);
          if (TFS_SUCCESS == ret )
          {
            assert(!out_v_meta_info.empty());
            FileMetaInfo& file_info = out_v_meta_info[0].file_info_;
            out_v_meta_info[0].frag_info_.dump();
            if (DIRECTORY == type)
            {
              CacheDirMetaNode* dir_node =
                static_cast<CacheDirMetaNode*>(malloc(sizeof(CacheDirMetaNode), CACHE_DIR_META_NODE));
              dir_node->reset();
              dir_node->id_ = file_info.id_;
              dir_node->create_time_ = file_info.create_time_;
              dir_node->modify_time_ = file_info.modify_time_;
              dir_node->version_ = file_info.ver_no_;
              dir_node->flag_ = 0;
              dir_node->name_ = static_cast<char*>(malloc(name_len));
              memcpy(dir_node->name_, name, name_len);
              if ((ret = MetaCacheHelper::insert_dir(p_dir_node, dir_node)) != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "insert dir error");
                MetaCacheHelper::free(dir_node);
              }
            }
            else
            {
              int64_t file_size = file_info.size_;
              int32_t file_create_time = file_info.create_time_;
              int32_t file_modify_time = file_info.modify_time_;
              int64_t buff_len = 0;
              //calculate file info;
              MetaInfo last_meta_info;
              vector<MetaInfo> v_meta_info;
              vector<MetaInfo>::iterator tmp_v_meta_info_it = out_v_meta_info.begin();
              assert(!still_have);
              calculate_file_meta_info(tmp_v_meta_info_it, out_v_meta_info.end(),
                  true, v_meta_info, last_meta_info);
              if (!v_meta_info.empty())
              {
                file_size = v_meta_info[0].file_info_.size_;
                file_create_time = v_meta_info[0].file_info_.create_time_;
                file_modify_time = v_meta_info[0].file_info_.modify_time_;
              }
              //find file info
              CacheFileMetaNode* file_node;
              ret = MetaCacheHelper::find_file(p_dir_node, name, file_node);
              if (TFS_SUCCESS == ret && NULL != file_node)
              {
                file_node->size_ = file_size;
                file_node->create_time_ = file_create_time;
                file_node->modify_time_ = file_modify_time;
                //replace info if cur is the first line
                if (FileName::length(name) == name_len)
                {
                  TBSYS_LOG(DEBUG, "replace current info");
                  out_v_meta_info[0].frag_info_.dump();
                  file_node->version_ = file_info.ver_no_;
                  buff_len = out_v_meta_info[0].frag_info_.get_length();
                  MemHelper::free(file_node->meta_info_);
                  file_node->meta_info_ = (char*)malloc(buff_len);
                  assert(NULL != file_node->meta_info_);
                  int64_t pos = 0;
                  int tmp_ret = out_v_meta_info[0].frag_info_.serialize(file_node->meta_info_,
                      buff_len, pos);
                  assert(TFS_SUCCESS == tmp_ret);
                }
              }
              else
              {
                file_node =
                  static_cast<CacheFileMetaNode*>(malloc(sizeof(CacheFileMetaNode), CACHE_FILE_META_NODE));
                file_node->size_ = file_size;
                file_node->create_time_ = file_create_time;
                file_node->modify_time_ = file_modify_time;
                TBSYS_LOG(DEBUG, "insert new file node");
                out_v_meta_info[0].frag_info_.dump();
                buff_len = out_v_meta_info[0].frag_info_.get_length();
                file_node->meta_info_ = (char*)malloc(buff_len);
                assert(NULL != file_node->meta_info_);
                int64_t pos = 0;
                int tmp_ret = out_v_meta_info[0].frag_info_.serialize(
                    file_node->meta_info_, buff_len, pos);

                assert(TFS_SUCCESS == tmp_ret);
                file_node->version_ = file_info.ver_no_;
                file_node->name_ = static_cast<char*>(malloc(name_len));
                memcpy(file_node->name_, name, name_len);
                if ((ret = MetaCacheHelper::insert_file(p_dir_node, file_node)) != TFS_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "insert file error");
                  MetaCacheHelper::free(file_node);
                }
              }
            }
          }
        }
      }
      return ret;
    }

    int MetaStoreManager::update(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, CacheDirMetaNode* s_p_dir_node,
        const int64_t d_ppid, CacheDirMetaNode* d_p_dir_node,
        const char* s_name, const char* d_name,
        const common::FileType type)
    {
      int ret = TFS_ERROR;
      if (NULL != s_p_dir_node && NULL != d_p_dir_node && NULL != s_name && NULL != d_name)
      {
        //check if dest have exist (dir and file)
        void* ret_node = NULL;
        ret = TFS_SUCCESS;
        select(app_id, uid, d_p_dir_node, d_name, true, ret_node);
        if (NULL != ret_node)
        {
          TBSYS_LOG(INFO, "same name file exist");
          ret = EXIT_TARGET_EXIST_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          select(app_id, uid, d_p_dir_node, d_name, false, ret_node);
          if (NULL != ret_node)
          {
            TBSYS_LOG(INFO, "same name dir exist");
            ret = EXIT_TARGET_EXIST_ERROR;
          }
        }
        if (TFS_SUCCESS == ret)
        {
          //db_update
          ret = update(app_id, uid, s_ppid,
              s_p_dir_node->id_, s_p_dir_node->name_, FileName::length(s_p_dir_node->name_),
              d_ppid, d_p_dir_node->id_, d_p_dir_node->name_, FileName::length(d_p_dir_node->name_),
              s_name, FileName::length(s_name),
              d_name, FileName::length(d_name), type);
        }
        if (TFS_SUCCESS == ret)
        {
          //update mem
          if (DIRECTORY != type)
          {
            CacheFileMetaNode* ret_node = NULL;
            MetaCacheHelper::find_file(s_p_dir_node, s_name, ret_node);
            if (NULL != ret_node)
            {
              int tmp_ret = MetaCacheHelper::rm_file(s_p_dir_node, ret_node);
              assert(TFS_SUCCESS == tmp_ret);
              MemHelper::free(ret_node->name_);
              ret_node->name_ = (char*)malloc(FileName::length(d_name));
              assert(NULL != ret_node->name_);
              memcpy(ret_node->name_, d_name, FileName::length(d_name));
              ret = MetaCacheHelper::insert_file(d_p_dir_node, ret_node);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "can not insert new node");
                MetaCacheHelper::free(ret_node);
              }
            }
          }
          else
          {
            CacheDirMetaNode* ret_node = NULL;
            MetaCacheHelper::find_dir(s_p_dir_node, s_name, ret_node);
            if (NULL != ret_node)
            {
              int tmp_ret = MetaCacheHelper::rm_dir(s_p_dir_node, ret_node);
              assert(TFS_SUCCESS == tmp_ret);
              MemHelper::free(ret_node->name_);
              ret_node->name_ = (char*)malloc(FileName::length(d_name));
              assert(NULL != ret_node->name_);
              memcpy(ret_node->name_, d_name, FileName::length(d_name));
              ret = MetaCacheHelper::insert_dir(d_p_dir_node, ret_node);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "can not insert new node");
                MetaCacheHelper::free(ret_node);
              }
            }
          }
        }
      }

      return ret;
    }

    int MetaStoreManager::remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
        CacheDirMetaNode* p_dir_node, void* node_will_removed, const FileType type)
    {
      int ret = TFS_ERROR;
      if (NULL != p_dir_node && NULL != node_will_removed)
      {
        char* name = NULL;
        int64_t p_name_len = FileName::length(p_dir_node->name_);
        int64_t id = 0;
        CacheDirMetaNode* dir_node = NULL;
        CacheFileMetaNode* file_node = NULL;
        if (DIRECTORY != type)
        {
          file_node = (CacheFileMetaNode*)node_will_removed;
          name = file_node->name_;
        }
        else
        {
          dir_node = (CacheDirMetaNode*)node_will_removed;
          name = dir_node->name_;
          id = dir_node->id_;
        }

        ret = remove(app_id, uid, ppid, p_dir_node->name_, p_name_len,
            p_dir_node->id_, id, name, FileName::length(name), type);
        if (TFS_SUCCESS == ret)
        {
          int tmp_ret = 0;
          //remove from mem
          if (DIRECTORY != type)
          {
            tmp_ret = MetaCacheHelper::rm_file(p_dir_node, file_node);
            assert(TFS_SUCCESS == tmp_ret);
            MetaCacheHelper::free(file_node);
          }
          else
          {
            tmp_ret = MetaCacheHelper::rm_dir(p_dir_node, dir_node);
            assert(TFS_SUCCESS == tmp_ret);
            MetaCacheHelper::free(dir_node);
          }
        }
      }
      return ret;
    }

    int MetaStoreManager::get_file_frag_info(const int64_t app_id, const int64_t uid,
        CacheDirMetaNode* p_dir_node, CacheFileMetaNode* file_node,
        const int64_t offset, std::vector<common::MetaInfo>& out_v_meta_info,
        int32_t& cluster_id, int64_t& last_offset)
    {
      int ret = TFS_SUCCESS;
      assert (NULL != p_dir_node);
      assert (NULL != file_node);
      cluster_id = -1;
      last_offset = 0;
      assert(NULL != file_node->meta_info_);
      //assert(-1 != file_node->size_);
      MetaInfo meta_info;
      int64_t pos = 0;
      meta_info.frag_info_.deserialize(file_node->meta_info_, MAX_FRAG_INFO_SIZE, pos);
      TBSYS_LOG(DEBUG, "will dump meta_info.frag_info_");
      meta_info.frag_info_.dump();
      cluster_id = meta_info.frag_info_.cluster_id_;
      cache_get_times_++;
      if ((offset >= meta_info.frag_info_.get_last_offset() || -1 == offset)
          && meta_info.frag_info_.had_been_split_)
      {
        int32_t name_len = FileName::length(file_node->name_);
        ret = get_meta_info_from_db(app_id, uid, p_dir_node->id_,
            file_node->name_, name_len, offset, out_v_meta_info, cluster_id, last_offset);
      }
      else
      {
        meta_info.file_info_.name_.assign(file_node->name_, FileName::length(file_node->name_));
        meta_info.file_info_.ver_no_ = file_node->version_;
        meta_info.file_info_.create_time_ = file_node->create_time_;
        meta_info.file_info_.modify_time_ = file_node->modify_time_;
        meta_info.file_info_.size_ = file_node->size_;
        last_offset = meta_info.frag_info_.get_last_offset();
        out_v_meta_info.push_back(meta_info);
        cache_hit_times_++;
      }
      return ret;
    }

    int MetaStoreManager::get_meta_info_from_db(const int64_t app_id, const int64_t uid, const int64_t pid,
        const char* name, const int32_t name_len,
        const int64_t offset, std::vector<MetaInfo>& tmp_v_meta_info,
        int32_t& cluster_id, int64_t& last_offset)
    {
      int ret = TFS_ERROR;
      char search_name[MAX_FILE_PATH_LEN];
      int32_t search_name_len = name_len;
      assert(name_len <= MAX_FILE_PATH_LEN);
      memcpy(search_name, name, search_name_len);
      bool still_have = false;
      last_offset = 0;
      cluster_id = -1;
      do
      {
        tmp_v_meta_info.clear();
        still_have = false;
        ret = select(app_id, uid, pid,
            search_name, search_name_len, true, tmp_v_meta_info);
        TBSYS_LOG(DEBUG, "select size: %zd", tmp_v_meta_info.size());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get read meta info fail, name: %s, ret: %d", search_name, ret);
          break;
        }
        if (!tmp_v_meta_info.empty())
        {
          const MetaInfo& last_metaInfo = tmp_v_meta_info[tmp_v_meta_info.size() - 1];
          cluster_id = last_metaInfo.frag_info_.cluster_id_;
          last_offset = last_metaInfo.get_last_offset();

          // offset is -1 means file's max offset
          if ((-1 == offset || last_offset <= offset) &&
              last_metaInfo.frag_info_.had_been_split_)
          {
            still_have = true;
            MetaServerService::next_file_name_base_on(search_name, search_name_len,
                last_metaInfo.get_name(), last_metaInfo.get_name_len());
          }
          TBSYS_LOG(DEBUG, "still have %d", still_have);
          last_metaInfo.frag_info_.dump();
        }
      } while(TFS_SUCCESS == ret && still_have);

      return ret;
    }


    ////////////////////////////////////////
    //database about
    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name, const int32_t name_len, const bool is_file, std::vector<MetaInfo>& out_v_meta_info)
    {
      int ret = TFS_ERROR;
      out_v_meta_info.clear();
      bool still_have = false;
      char name_end[MAX_FILE_PATH_LEN];
      if (NULL == name || FileName::length(name) > MAX_FILE_PATH_LEN)
      {
        TBSYS_LOG(ERROR, "parameters error");
      }
      else
      {
        //make name_end [len]xxxxxxxx[max_offset]
        memcpy(name_end, name, name_len);
        char* p = name_end + FileName::length(name);
        int32_t name_end_len = name_len;
        name_end_len = FileName::length(name) + 8;
        Serialization::int64_to_char(p, 8, -1L);
        ret = ls(app_id, uid, pid, name, name_len, name_end, name_end_len,
            is_file, out_v_meta_info, still_have);
      }
      return ret;
    }

    int MetaStoreManager::ls(const int64_t app_id, const int64_t uid, const int64_t pid,
        const char* name, const int32_t name_len,
        const char* name_end, const int32_t name_end_len,
        const bool is_file, std::vector<MetaInfo>& out_v_meta_info, bool& still_have)
    {
      int ret = TFS_ERROR;
      still_have = false;
      out_v_meta_info.clear();
      int64_t real_pid = 0;
      if (is_file)
      {
        real_pid = pid | (1L<<63);
      }
      else
      {
        real_pid = pid & ~(1L<<63);
      }
      TBSYS_LOG(DEBUG, "ls real_pid %"PRI64_PREFIX"d is_file %d", real_pid, is_file);

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        TBSYS_LOG(DEBUG, "ls_meta_info appid %"PRI64_PREFIX"d uid %"PRI64_PREFIX"d",
            app_id, uid);
        PROFILER_BEGIN("ls_meta_info");
        ret = database_helper->ls_meta_info(out_v_meta_info, app_id, uid, real_pid,
            name, name_len, name_end, name_end_len);
        PROFILER_END();
        database_pool_->release(database_helper);
      }
      if (out_v_meta_info.size() > 0)
      {
        out_v_meta_info[0].frag_info_.dump();
      }

      if (static_cast<int32_t>(out_v_meta_info.size()) >= ROW_LIMIT)
      {
        still_have = true;
      }
      TBSYS_LOG(DEBUG, "out_v_meta_info.size() %zd", out_v_meta_info.size());
      return ret;
    }

    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const char* name, const int32_t name_len, const FileType type, MetaInfo* meta_info)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;
      int64_t id = 0;
      DatabaseHelper* database_helper = NULL;
      if (type == DIRECTORY)
      {
        database_helper = database_pool_->get(0);
        if (NULL != database_helper)
        {
          database_helper->get_nex_val(id);
          database_pool_->release(database_helper);
          database_helper = NULL;
        }
      }
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type == NORMAL_FILE)
        {
          PROFILER_BEGIN("create_file");
          status = database_helper->create_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
          PROFILER_END();
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper create file, status: %d", status);
          }
        }
        else if (type == DIRECTORY)
        {
          if (id != 0)
          {
            PROFILER_BEGIN("create_dir");
            status = database_helper->create_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
            PROFILER_END();
            if (TFS_SUCCESS != status)
            {
              TBSYS_LOG(DEBUG, "database helper create dir, status: %d", status);
            }
          }
        }
        else if (type == PWRITE_FILE)
        {
          TBSYS_LOG(DEBUG, "PWRITE_FILE");
          if (NULL == meta_info)
          {
            TBSYS_LOG(ERROR, "meta_info should not be NULL");
          }
          else
          {
            int64_t frag_len = meta_info->frag_info_.get_length();
            if (frag_len > MAX_FRAG_INFO_SIZE)
            {
              TBSYS_LOG(ERROR, "meta info is too long(%"PRI64_PREFIX"d > %d)", frag_len, MAX_FRAG_INFO_SIZE);
              ret = TFS_ERROR;
            }
            else
            {
              int64_t pos = 0;
              char* frag_info = (char*)malloc(frag_len);
              if (NULL == frag_info)
              {
                TBSYS_LOG(ERROR, "mem not enough");
                ret = TFS_ERROR;
              }
              else
              {
                status = meta_info->frag_info_.serialize(frag_info, frag_len, pos);
                TBSYS_LOG(DEBUG, "will dump meta_info->frag_info_");
                meta_info->frag_info_.dump();
                if (TFS_SUCCESS != status)
                {
                  TBSYS_LOG(ERROR, "get meta info failed, status: %d ", status);
                }
                else
                {
                  PROFILER_BEGIN("pwrite_file");
                  status = database_helper->pwrite_file(app_id, uid, pid, name, name_len,
                      meta_info->file_info_.size_, meta_info->file_info_.ver_no_, frag_info, frag_len, proc_ret);
                  PROFILER_END();
                  if (TFS_SUCCESS != status)
                  {
                    TBSYS_LOG(DEBUG, "database helper pwrite file, status: %d", status);
                  }
                }
                MemHelper::free(frag_info);
              }
            }
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    int MetaStoreManager::update(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          PROFILER_BEGIN("mv_file");
          status = database_helper->mv_file(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          PROFILER_END();
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv file, status: %d", status);
          }
        }
        else if (type & DIRECTORY)
        {
          PROFILER_BEGIN("mv_dir");
          status = database_helper->mv_dir(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          PROFILER_END();
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv dir, status: %d", status);
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    int MetaStoreManager::remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
        const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
        const char* name, const int64_t name_len,
        const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          PROFILER_BEGIN("rm_file");
          status = database_helper->rm_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
          PROFILER_END();
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper rm file, status: %d", status);
          }
        }
        else if (type & DIRECTORY)
        {
          if (id == 0)
          {
            TBSYS_LOG(DEBUG, "wrong type, target is file.");
          }
          else
          {
            PROFILER_BEGIN("rm_dir");
            status = database_helper->rm_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
            PROFILER_END();
            if (TFS_SUCCESS != status)
            {
              TBSYS_LOG(DEBUG, "database helper rm dir, status: %d", status);
            }
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    // proc_ret depend database's logic
    // database level and manager level should be be consensus about error code definition,
    // otherwise, manager level SHOULD merge difference
    int MetaStoreManager::get_return_status(const int status, const int proc_ret)
    {
      int ret = TFS_SUCCESS;
      // maybe network fail
      if (status != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "ret is %d when we ask data from database", status);
        // // TODO. maybe do sth.
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret && proc_ret < 0)
      {
        ret = proc_ret;
      }

      return ret;
    }

    int MetaStoreManager::get_dir_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
        const char* name, const int32_t name_len, MetaInfo& out_meta_info)
    {
      std::vector<MetaInfo> tmp_v_meta_info;
      int ret = select(app_id, uid, pid, name, name_len, false, tmp_v_meta_info);
      if (TFS_SUCCESS != ret || tmp_v_meta_info.empty())
      {
        TBSYS_LOG(INFO, "get dir meta info fail. %s, ret: %d", name, ret);
        if (TFS_SUCCESS == ret && tmp_v_meta_info.empty())
        {
          ret = EXIT_TARGET_EXIST_ERROR;
        }
      }
      else
      {
        // copy
        out_meta_info.copy_no_frag(*tmp_v_meta_info.begin());
      }
      return ret;
    }

    int MetaStoreManager::get_children_from_db(const int64_t app_id, const int64_t uid,
        CacheDirMetaNode* p_dir_node, const char* name, bool is_file)
    {
      TBSYS_LOG(DEBUG, "get_children_from_db app_id %"PRI64_PREFIX"d uid %"PRI64_PREFIX"d",
          app_id, uid);
      int ret = TFS_ERROR;
      assert (NULL != p_dir_node);
      std::vector<MetaInfo> tmp_v_meta_info;
      vector<MetaInfo>::iterator tmp_v_meta_info_it;
      int32_t name_len = FileName::length(name);
      bool still_have = false;
      do  //we use do while so we can break, no loop here
      {
        tmp_v_meta_info.clear();
        still_have = false;
        ret = ls(app_id, uid, p_dir_node->id_,
            name, name_len, NULL, 0,
            is_file, tmp_v_meta_info, still_have);

        if (!tmp_v_meta_info.empty())
        {
          tmp_v_meta_info_it = tmp_v_meta_info.begin();

          if (is_file)
          {
            // fill file meta info
            ret = fill_file_meta_info(tmp_v_meta_info_it, tmp_v_meta_info.end(), p_dir_node);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
          else
          {
            ret = fill_dir_info(tmp_v_meta_info_it, tmp_v_meta_info.end(), p_dir_node);
          }

          //// still have and need continue
          //if (still_have)
          //{
          //  tmp_v_meta_info_it--;
          //  MetaServerService::next_file_name_base_on(name, name_len,
          //      tmp_v_meta_info_it->get_name(), tmp_v_meta_info_it->get_name_len());
          //}
        }

        // directory over, continue list file
        //if (my_file_type == DIRECTORY && !still_have)
        //{
        //  my_file_type = NORMAL_FILE;
        //  still_have = true;
        //  name[0] = '\0';
        //  name_len = 1;
        //}
      } while (false);
      //if (TFS_SUCCESS == ret && !still_have)
      //{
      //  p_dir_node->set_got_all_children();
      //}

      return ret;
    }

    int MetaStoreManager::fill_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
        const std::vector<common::MetaInfo>::iterator meta_info_end,
        CacheDirMetaNode* p_dir_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != p_dir_node);
      for (; meta_info_begin != meta_info_end; meta_info_begin++)
      {
        CacheFileMetaNode* file_meta;
        ret = MetaCacheHelper::find_file(p_dir_node,
            meta_info_begin->file_info_.name_.c_str(), file_meta);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
        // is the first line of file?
        if (meta_info_begin->file_info_.name_.length() > 0
            && *((unsigned char*)(meta_info_begin->file_info_.name_.c_str())) == meta_info_begin->file_info_.name_.length() -1)
        {
          //first line of file, insert it;
          if (NULL == file_meta)
          {
            file_meta = (CacheFileMetaNode*)malloc(sizeof(CacheFileMetaNode), CACHE_FILE_META_NODE);
            assert(NULL != file_meta);
            file_meta->name_ = (char*)malloc(meta_info_begin->file_info_.name_.length());
            memcpy(file_meta->name_, meta_info_begin->file_info_.name_.c_str(),
                meta_info_begin->file_info_.name_.length());
            file_meta->version_ = meta_info_begin->file_info_.ver_no_;

            int64_t buff_len = 0;
            int64_t pos = 0;
            file_meta->meta_info_ = NULL;
            buff_len = meta_info_begin->frag_info_.get_length();
            file_meta->meta_info_ = (char*)malloc(buff_len);
            assert(NULL != file_meta->meta_info_);
            int tmp_ret =  meta_info_begin->frag_info_.serialize(file_meta->meta_info_, buff_len, pos);
            assert(TFS_SUCCESS == tmp_ret);

            ret = MetaCacheHelper::insert_file(p_dir_node, file_meta);
            if (TFS_SUCCESS != ret)
            {
              MetaCacheHelper::free(file_meta);
              break;
            }
          }
        }
        assert (NULL != file_meta);
        file_meta->size_ = meta_info_begin->frag_info_.get_last_offset();
        file_meta->create_time_ = meta_info_begin->file_info_.create_time_;
        file_meta->modify_time_ = meta_info_begin->file_info_.modify_time_;
      }
      return ret;
    }

    int MetaStoreManager::fill_dir_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
        const std::vector<common::MetaInfo>::iterator meta_info_end,
        CacheDirMetaNode* p_dir_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != p_dir_node);
      for (; meta_info_begin != meta_info_end; meta_info_begin++)
      {
        CacheDirMetaNode* dir_meta;
        ret = MetaCacheHelper::find_dir(p_dir_node,
            meta_info_begin->file_info_.name_.c_str(), dir_meta);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
        if (NULL == dir_meta)
        {
          //insert a dir meta
          dir_meta = (CacheDirMetaNode*)malloc(sizeof(CacheDirMetaNode), CACHE_DIR_META_NODE);
          assert(NULL != dir_meta);
          dir_meta->id_= meta_info_begin->file_info_.id_;
          dir_meta->name_ = (char*)malloc(meta_info_begin->file_info_.name_.length());
          memcpy(dir_meta->name_, meta_info_begin->file_info_.name_.c_str(),
              meta_info_begin->file_info_.name_.length());

          dir_meta->flag_ = 0;
          dir_meta->child_dir_infos_ = dir_meta->child_file_infos_ = NULL;
          ret = MetaCacheHelper::insert_dir(p_dir_node, dir_meta);
          if (TFS_SUCCESS != ret)
          {
            MetaCacheHelper::free(dir_meta);
            break;
          }
        }
        assert (NULL != dir_meta);
        dir_meta->create_time_ = meta_info_begin->file_info_.create_time_;
        dir_meta->modify_time_ = meta_info_begin->file_info_.modify_time_;
        dir_meta->version_ = meta_info_begin->file_info_.ver_no_;
      }
      return ret;
    }

    void MetaStoreManager::calculate_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
        const std::vector<common::MetaInfo>::iterator meta_info_end,
        const bool ls_file,
        std::vector<common::MetaInfo>& v_meta_info,
        common::MetaInfo& last_meta_info)
    {
      for (; meta_info_begin != meta_info_end; meta_info_begin++)
      {
        if (last_meta_info.empty()) // no last file
        {
          last_meta_info.copy_no_frag(*meta_info_begin);
          TBSYS_LOG(DEBUG, "copy meta_info to last_meta_info");
          if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, this is a completed file recored
          {
            v_meta_info.push_back(last_meta_info);
            last_meta_info.file_info_.name_.clear(); // empty last file
          }
        }
        else                    // have last file, need check whether this metainfo is of last file or not.
        {
          // this metaInfo is also of last file.
          if (0 == memcmp(last_meta_info.get_name(), meta_info_begin->get_name(),
                last_meta_info.get_name_len()))
          {
            // get_size() is the max file size that current recored hold
            last_meta_info.file_info_.size_ = meta_info_begin->get_size();
            if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, last file is completed
            {
              v_meta_info.push_back(last_meta_info);
              last_meta_info.file_info_.name_.clear();
            }
          }
          else                  // this metainfo is not of last file,
            {
              v_meta_info.push_back(last_meta_info); // then last file is completed
              last_meta_info.copy_no_frag(*meta_info_begin);
              if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, thie metainfo is completed
              {
                v_meta_info.push_back(last_meta_info);
                last_meta_info.file_info_.name_.clear();
              }
            }
        }

        if (ls_file && !v_meta_info.empty()) // if list file, only need one metainfo.
        {
          break;
        }
      }
      if (!last_meta_info.file_info_.name_.empty())
      {
        v_meta_info.push_back(last_meta_info);
        last_meta_info.file_info_.name_.clear();
      }
      return;
    }
  }
}
