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
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "meta_cache_info.h"

#include "common/meta_server_define.h"
#include "common/meta_hash_helper.h"
#include "meta_cache_helper.h"

namespace tfs
{
  namespace namemetaserver
  {
    using namespace common;
    int64_t CacheRootNode::get_hash() const
    {
      HashHelper helper(key_.app_id_, key_.uid_);
      return tbsys::CStringUtil::murMurHash((const void*)&helper, sizeof(helper)) % common::MAX_BUCKET_ITEM_DEFAULT;
    }
    void CacheRootNode::dump() const
    {
      TBSYS_LOG(DEBUG, "CacheRootNode: app_id = %"PRI64_PREFIX"d "
          "user_id_ = %"PRI64_PREFIX"d ",
          key_.app_id_, key_.uid_);
      if (NULL != dir_meta_)
        dir_meta_->dump();
      TBSYS_LOG(DEBUG, "dump CacheRootNode over");
    }
    void CacheDirMetaNode::dump() const
    {
      TBSYS_LOG(DEBUG, "CacheDirMetaNode: id_ = %"PRI64_PREFIX"d "
          "create_time_ = %d modify_time_ = %d "
          "version_ = %d flag_ = %d",
          id_, create_time_, modify_time_, version_, flag_);
      if (NULL != name_)
      {
        TBSYS_LOG(DEBUG, "name_ = %.*s", FileName::length(name_) - 1, name_ + 1);
      }
      if (NULL != child_dir_infos_)
      {
        InfoArray<CacheDirMetaNode>* p = (InfoArray<CacheDirMetaNode>*)child_dir_infos_;
        CacheDirMetaNode** begin = p->get_begin();
        TBSYS_LOG(DEBUG, "child dir size = %d", p->get_size());
        for (int i = 0; i < p->get_size(); i++)
        {
          TBSYS_LOG(DEBUG, "child %d dir", i);
          (*(begin + i))->dump();
        }
      }
      if (NULL != child_file_infos_)
      {
        InfoArray<CacheFileMetaNode>* p = (InfoArray<CacheFileMetaNode>*)child_file_infos_;
        CacheFileMetaNode** begin = p->get_begin();
        TBSYS_LOG(DEBUG, "child file size = %d", p->get_size());
        for (int i = 0; i < p->get_size(); i++)
        {
          TBSYS_LOG(DEBUG, "child %d file", i);
          (*(begin + i))->dump();
        }
      }
      TBSYS_LOG(DEBUG, "dump CacheDirMetaNode over");
    }
    void CacheFileMetaNode::dump() const
    {
      TBSYS_LOG(DEBUG, "CacheFileMetaNode: size_ = %"PRI64_PREFIX"d "
          "create_time_ = %d modify_time_ = %d "
          "version_ = %d",
          size_, create_time_, modify_time_, version_);
      if (NULL != name_)
      {
        TBSYS_LOG(DEBUG, "name_ = %.*s", FileName::length(name_) - 1, name_ + 1);
      }
      TBSYS_LOG(DEBUG, "dump CacheFileMetaNode over");
    }
    bool CacheDirMetaNode::operator < (const CacheDirMetaNode& right) const
    {
      return MetaCacheHelper::less_compare(*this, right);
    }
    bool CacheDirMetaNode::operator == (const CacheDirMetaNode& right) const
    {
      return MetaCacheHelper::equal_compare(*this, right);
    }
    bool CacheDirMetaNode::operator != (const CacheDirMetaNode& right) const
    {
      return !(*this == right);
    }
    bool CacheFileMetaNode::operator < (const CacheFileMetaNode& right) const
    {
      return MetaCacheHelper::less_compare(*this, right);
    }
    bool CacheFileMetaNode::operator == (const CacheFileMetaNode& right) const
    {
      return MetaCacheHelper::equal_compare(*this, right);
    }
    bool CacheFileMetaNode::operator != (const CacheFileMetaNode& right) const
    {
      return !(*this == right);
    }
  }
}
