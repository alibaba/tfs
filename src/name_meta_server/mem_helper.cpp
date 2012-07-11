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
#include "mem_helper.h"

#include <stdlib.h>
#include <tbsys.h>
#include "meta_cache_helper.h"
#include "common/meta_server_define.h"

namespace tfs
{
  namespace namemetaserver
  {
    MemNodeList::MemNodeList(const int32_t capacity)
      :size_(0), capacity_(capacity), p_root_(NULL)
    {
    }

    MemNodeList::~MemNodeList()
    {
    }

    void* MemNodeList::get()
    {
      void* p_ret = NULL;
      if (size_ > 0)
      {
        assert (NULL != p_root_);
        p_ret = (void*)p_root_;
        p_root_ = (void**)(*p_root_);
        --size_;
      }
      return p_ret;
    }

    bool MemNodeList::put(void* p)
    {
      bool ret = false;
      if (size_ < capacity_)
      {
        (*((void**)p)) = p_root_;
        ++size_;
        p_root_ = (void**)p;
        ret = true;
      }
      TBSYS_LOG(DEBUG, "put MemNodeList %s, size: %d, capacity: %d", ret ? "success" : "fail", size_, capacity_);
      return ret;
    }


    tbsys::CThreadMutex MemHelper::mutex_;
    MemHelper MemHelper::instance_;
    int64_t MemHelper::used_size_ = 0;

    MemHelper::MemHelper(): root_node_free_list_(NULL),
        dir_node_free_list_(NULL), file_node_free_list_(NULL)
    {
    }

    bool MemHelper::init(const int32_t r_free_list_count, const int32_t d_free_list_count,
          const int32_t f_free_list_count)
    {
      bool ret = false;
      tbsys::CThreadGuard mutex_guard(&mutex_);
      instance_.destroy();
      instance_.clear();
      instance_.root_node_free_list_ = new MemNodeList(r_free_list_count);
      instance_.dir_node_free_list_ = new MemNodeList(d_free_list_count);
      instance_.file_node_free_list_ = new MemNodeList(f_free_list_count);
      ret = (NULL != instance_.root_node_free_list_ && NULL != instance_.dir_node_free_list_ &&
             NULL != instance_.file_node_free_list_) ? true : false;
      return ret;
    }
    void MemHelper::clear()
    {
      if (NULL != root_node_free_list_)
      {
        delete root_node_free_list_;
        root_node_free_list_ = NULL;
      }
      if (NULL != dir_node_free_list_)
      {
        delete dir_node_free_list_;
        dir_node_free_list_ = NULL;
      }
      if (NULL != file_node_free_list_)
      {
        delete file_node_free_list_;
        file_node_free_list_ = NULL;
      }
    }
    MemHelper::~MemHelper()
    {
      destroy();
      clear();
    }

    void MemHelper::destroy()
    {
      void* p = NULL;
      while(NULL != root_node_free_list_ && NULL != (p = root_node_free_list_->get()))
      {
        free(p);
      }
      while(NULL != dir_node_free_list_ && NULL != (p = dir_node_free_list_->get()))
      {
        free(p);
      }
      while(NULL != file_node_free_list_ && NULL != (p = file_node_free_list_->get()))
      {
        free(p);
      }
    }

    int64_t MemHelper::get_used_size()
    {
      return used_size_;
    }
    void* MemHelper::malloc(const int64_t size, const int32_t type)
    {
      void* ret_p = NULL;
      if (size > common::MAX_FRAG_INFO_SIZE)
      {
        TBSYS_LOG(WARN, "size larger then MAX_FRAG_INFO_SIZE");
      }
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        used_size_ += size + sizeof(int32_t);
        if (type != CACHE_NONE_NODE)
        {
          switch (type)
          {
            case CACHE_ROOT_NODE:
              ret_p = instance_.root_node_free_list_->get();
              break;
            case CACHE_DIR_META_NODE:
              ret_p = instance_.dir_node_free_list_->get();
              break;
            case CACHE_FILE_META_NODE:
              ret_p = instance_.file_node_free_list_->get();
              break;
            default:
              break;
          }
        }
      }
      if (NULL == ret_p)
      {
        ret_p = ::malloc(size + sizeof(int32_t));
        *((int32_t*)ret_p) = size;
        ret_p = (char*)ret_p + sizeof(int32_t);
      }
      return ret_p;
    }
    void MemHelper::free(void* p, const int32_t type)
    {
      if (p != NULL)
      {
        char* real_p = (char*)p;
        real_p -= sizeof(int32_t);
        int64_t size = *((int32_t*)real_p);
        bool ret = false;
        {
          tbsys::CThreadGuard mutex_guard(&mutex_);
          used_size_ -= size + sizeof(int32_t);
          if (type != CACHE_NONE_NODE)
          {
            switch (type)
            {
              case CACHE_ROOT_NODE:
                ret = instance_.root_node_free_list_->put(p);
                break;
              case CACHE_DIR_META_NODE:
                ret = instance_.dir_node_free_list_->put(p);
                break;
              case CACHE_FILE_META_NODE:
                ret = instance_.file_node_free_list_->put(p);
                break;
              default:
                break;
            }
          }
        }
        if (!ret)
        {
          ::free(real_p);
        }
      }
    }
  }
}

