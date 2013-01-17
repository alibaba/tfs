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
#ifndef TFS_NAMEMETASERVER_CACHE_INFO_H_
#define TFS_NAMEMETASERVER_CACHE_INFO_H_
#include <string.h>
#include <algorithm>
#include <tbsys.h>
#include "common/define.h"
#include "common/meta_server_define.h"
#include "mem_helper.h"
namespace tfs
{
  namespace namemetaserver
  {
    class CacheDirMetaNode;
    struct CacheRootNode
    {
      CacheRootNode(){ reset(); }
      void reset()   { memset(this, 0, sizeof(CacheRootNode)); }
      void inc_ref() { ++ref_count_; }
      void dec_ref() { --ref_count_; }
      void inc_visit_count() { ++visit_count_; }

      int64_t get_hash() const;
      void dump() const;
      common::AppIdUid key_;
      int64_t ref_count_;
      int64_t visit_count_;
      //int64_t size_;    //the mem size occupied by this user

      CacheDirMetaNode* dir_meta_; // top dir
    };
    struct CacheDirMetaNode
    {
      CacheDirMetaNode()
      {
        reset();
      }
      void reset()
      {
        memset(this, 0, sizeof(CacheDirMetaNode));
      }
      void dump() const;
      bool operator < (const CacheDirMetaNode& right) const;
      bool operator == (const CacheDirMetaNode& right) const;
      bool operator != (const CacheDirMetaNode& right) const;
      bool is_got_all_children() const
      {
        return 1 == (flag_ & 0x01);
      }
      void set_got_all_children()
      {
        flag_ |= 0x01;
      }

      int64_t id_;
      char* name_;
      void* child_dir_infos_;
      void* child_file_infos_;
      int32_t create_time_;
      int32_t modify_time_;
      int16_t version_;
      int16_t flag_;    //we will set lowest bit if we got all  children in cache
    };
    struct CacheFileMetaNode
    {
      CacheFileMetaNode()
      {
        reset();
      }
      void reset()
      {
        memset(this, 0, sizeof(CacheFileMetaNode));
        size_ = -1;
      }
      void dump() const;
      bool operator < (const CacheFileMetaNode& right) const;
      bool operator == (const CacheFileMetaNode& right) const;
      bool operator != (const CacheFileMetaNode& right) const;
      int64_t size_;    //init as -1, if we can not get the real size we use -1 for this;
      char* name_;
      char* meta_info_;
      int32_t create_time_;
      int32_t modify_time_;
      int16_t version_;
    };
    template<class T>
      class InfoArray
      {
        public:
          InfoArray();
          T** find(const char* name);
          bool insert(T* node);
          bool remove(const char* name);
          ~InfoArray();
          int32_t get_size() const
          {
            return size_;
          }
          int32_t get_capacity() const
          {
            return capacity_;
          }
          T** get_begin() const
          {
            return begin_;
          }
        private:
          class PointCompareHelper
          {
            public:
              bool operator ()(const T* l_node, const T* r_node) const
              {
                assert(NULL != l_node);
                assert(NULL != r_node);
                return *l_node < *r_node;
              }
          };
        private:
          int32_t size_;
          int32_t capacity_;
          T** begin_;
      };
    template<class T>
      InfoArray<T>::InfoArray():
        size_(0), capacity_(4), begin_(NULL)
    {
      begin_ = (T**)MemHelper::malloc(sizeof(void*) * capacity_);
      assert(NULL != begin_);
    }
    template<class T>
      InfoArray<T>::~InfoArray()
      {
        if (NULL != begin_)
        {
          MemHelper::free(begin_);
        }
      }

    template<class T>
      T** InfoArray<T>::find(const char* name)
      {
        T** ret = NULL;
        T tmp;
        tmp.name_ = (char*)name;
        ret = std::lower_bound(begin_, begin_ + size_, &tmp, PointCompareHelper());
        if (ret == begin_ + size_ || **ret != tmp)
        {
          ret = NULL;
        }
        return ret;
      }
    template<class T>
      bool InfoArray<T>::insert(T* node)
      {
        bool ret = false;
        if (NULL != node)
        {
          T** insert_pos = NULL;
          insert_pos = std::lower_bound(begin_, begin_ + size_, node, PointCompareHelper());
          int index = insert_pos - begin_;
          if (size_ == 0 || index == size_ ||**insert_pos != *node)
          {
            //insert node to pos = index;
            if (capacity_ == size_)
            {
              //expand
              capacity_ += capacity_;
              T** new_begin = (T**)MemHelper::malloc(sizeof(void*) * capacity_);
              assert(NULL != new_begin);
              memcpy(new_begin, begin_, sizeof(void*) * index);
              new_begin[index] = node;
              memcpy(new_begin + index + 1, begin_ + index, (size_ - index) * sizeof(void*));
              MemHelper::free(begin_);
              begin_ = new_begin;
            }
            else
            {
              memmove(begin_ + index + 1, begin_ + index, (size_ - index) * sizeof(void*));
              begin_[index] = node;
            }
            size_++;
            ret = true;
          }
          else
          {
            TBSYS_LOG(ERROR, "should not insert the same value");
          }
        }
        return ret;
      }
    template<class T>
      bool InfoArray<T>::remove(const char* name)
      {
        bool ret = false;
        T** pos = find(name);
        if (NULL != pos)
        {
          ret = true;
          int index = pos - begin_;
          memmove(begin_ + index, begin_ + index + 1, (size_ - index - 1) * sizeof(void*));
          size_ --;
        }
        return ret;
      }
  }
}
#endif
