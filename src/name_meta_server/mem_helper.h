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
#ifndef TFS_NAMEMETASERVER_MEM_HELPER_H_
#define TFS_NAMEMETASERVER_MEM_HELPER_H_
#include <stdint.h>
#include <tbsys.h>
#include "common/define.h"

namespace tfs
{
  namespace namemetaserver
  {
    //class CacheRootNode;
    //class CacheDirMetaNode;
    //class CacheFileMetaNode;

    enum
    {
      CACHE_NONE_NODE = 0,
      CACHE_ROOT_NODE = 1,
      CACHE_DIR_META_NODE = 2,
      CACHE_FILE_META_NODE = 3,
    };

    class MemNodeList
    {
    public:
      explicit MemNodeList(const int32_t capacity);
      ~MemNodeList();

      inline int32_t get_capacity() const
      {
        return capacity_;
      }

      void* get();
      bool put(void* p);
    private:
      int32_t size_;
      int32_t capacity_;
      void** p_root_;
    };

    class MemHelper
    {
    public:
      ~MemHelper();
      static bool init(const int32_t r_free_list_count, const int32_t d_free_list_count,
          const int32_t f_free_list_count);
      static void* malloc(const int64_t size, const int32_t type = CACHE_NONE_NODE);
      static void free(void* p, const int32_t type = CACHE_NONE_NODE);
      static int64_t get_used_size();
    private:
      MemHelper();
      void destroy();
      void clear();
    private:
      static tbsys::CThreadMutex mutex_;
      static MemHelper instance_;
      static int64_t used_size_;
    private:
      MemNodeList* root_node_free_list_;
      MemNodeList* dir_node_free_list_;
      MemNodeList* file_node_free_list_;
    };
  }
}
#endif
