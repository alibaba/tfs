/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lru.h 5 2011-09-06 16:00:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *   duanfei<duanfei@taobao.com>
 *      -reconstruction 2012-01-15
 */
#ifndef TFS_NAMEMETASERVER_LRU_H_
#define TFS_NAMEMETASERVER_LRU_H_

#ifdef TFS_GTEST
  #include <gtest/gtest.h>
#endif

#include <list>
#include <ext/hash_map>
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/meta_hash_helper.h"
#include "common/rts_define.h"

#include "meta_cache_info.h"
namespace tfs
{
  namespace namemetaserver
  {
    class BaseStrategy;
    class Lru
    {
      #ifdef TFS_GTEST
      friend class LruTest;
      FRIEND_TEST(LruTest, insert);
      FRIEND_TEST(LruTest, get);
      FRIEND_TEST(LruTest, put);
      FRIEND_TEST(LruTest, gc);
      #endif

      typedef std::list<CacheRootNode*> LRU_LIST;
      typedef LRU_LIST::iterator LRU_LIST_ITERATOR;

      typedef std::map<common::AppIdUid, LRU_LIST_ITERATOR> LRU_SUB_MAP;
      typedef LRU_SUB_MAP::iterator LRU_SUB_MAP_ITERATOR;

      typedef std::map<int64_t, LRU_SUB_MAP>  LRU_MAP;
      typedef LRU_MAP::iterator LRU_MAP_ITERATOR;

      friend class BaseStrategy;
    public:
      Lru(){}
      virtual ~Lru() {}
      inline CacheRootNode* get(const common::AppIdUid& key);
      inline int put(CacheRootNode* value);
      inline int insert(const common::AppIdUid& key, CacheRootNode* value);
      inline int gc(const double ratio, BaseStrategy* st, std::vector<CacheRootNode*>& values);
      inline int gc(const std::set<int64_t>& buckets);
      inline int gc(std::vector<CacheRootNode*>& values);
      inline int64_t size();
    private:
      inline CacheRootNode* get_node(const common::AppIdUid& key);
      LRU_LIST list_;
      LRU_MAP  index_;
      DISALLOW_COPY_AND_ASSIGN(Lru);
    };

    class BaseStrategy
    {
    public:
      explicit BaseStrategy(Lru& lru): lru_(lru){}
      virtual ~BaseStrategy(){}
      inline virtual int gc(const double ratio, std::vector<CacheRootNode*>& values);
    private:
      inline int64_t calc_gc_count(const double ratio);
      Lru& lru_;
    };

    CacheRootNode* Lru::get(const common::AppIdUid& key)
    {
      CacheRootNode* value = NULL;
      int64_t hash_value = key.get_hash();
      LRU_MAP_ITERATOR iter = index_.find(hash_value);
      if (index_.end() != iter)
      {
        LRU_SUB_MAP_ITERATOR sub_iter = iter->second.find(key);
        if (iter->second.end() != sub_iter)
        {
          list_.splice(list_.end(), list_, sub_iter->second);
          (*sub_iter->second)->inc_visit_count();
          (*sub_iter->second)->inc_ref();
          value = (*sub_iter->second);
          TBSYS_LOG(DEBUG, "ref_count_ = %"PRI64_PREFIX"d", (*sub_iter->second)->ref_count_);
        }
      }
      return value;
    }

    int Lru::put(CacheRootNode* value)
    {
      int32_t iret = NULL == value ? common::EXIT_PARAMETER_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        value->dec_ref();
      }
      return iret;
    }

    int Lru::insert(const common::AppIdUid& key, CacheRootNode* value)
    {
      int32_t iret = NULL == value ? common::EXIT_PARAMETER_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        int64_t hash_value = key.get_hash();
        LRU_MAP_ITERATOR iter = index_.find(hash_value);
        if (index_.end() == iter)
        {
          std::pair<LRU_MAP_ITERATOR, bool> res1 = index_.insert(std::pair<int64_t, LRU_SUB_MAP>(
            hash_value, std::map<common::AppIdUid, LRU_LIST_ITERATOR>()));
          iter = res1.first;
        }

        LRU_SUB_MAP_ITERATOR sub_iter = iter->second.find(key);
        iret = iter->second.end() != sub_iter ? common::EXIT_LRU_VALUE_EXIST : common::TFS_SUCCESS;
        if (common::TFS_SUCCESS == iret)
        {
          value->ref_count_   = 0;
          value->visit_count_ = 0;
          LRU_LIST_ITERATOR it = list_.insert(list_.end(), value);
          std::pair<LRU_SUB_MAP_ITERATOR, bool> res = iter->second.insert(std::pair<common::AppIdUid, LRU_LIST_ITERATOR>(key, it));
          iret = res.second ? common::TFS_SUCCESS : common::EXIT_LRU_VALUE_EXIST;
          if (common::TFS_SUCCESS != iret)
          {
            list_.pop_back();
          }
        }
      }
      return iret;
    }

    int Lru::gc(const double ratio, BaseStrategy* st, std::vector<CacheRootNode*>& values)
    {
      int32_t iret = NULL == st ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iret = st->gc(ratio, values);
      }
      return iret;
    }

    int Lru::gc(std::vector<CacheRootNode*>& values)
    {
      LRU_LIST_ITERATOR iter = list_.begin();
      for (; iter != list_.end(); ++iter)
      {
        values.push_back((*iter));
      }
      list_.clear();
      index_.clear();
      return common::TFS_SUCCESS;
    }

    int Lru::gc(const std::set<int64_t>& buckets)
    {
      std::set<int64_t>::const_iterator iter = buckets.begin();
      for (; iter != buckets.end(); ++iter)
      {
        LRU_MAP_ITERATOR it = index_.find((*iter));
        if (index_.end() != it)
        {
          index_.erase(it);
        }
      }
      return common::TFS_SUCCESS;
    }

    int64_t Lru::size()
    {
      int64_t count = 0;
      LRU_MAP_ITERATOR iter = index_.begin();
      for (; iter != index_.end(); ++iter)
      {
        count += iter->second.size();
      }
      return count;
    }

    CacheRootNode* Lru::get_node(const common::AppIdUid& key)
    {
      CacheRootNode* value = NULL;
      int64_t hash_value = key.get_hash();
      LRU_MAP_ITERATOR iter = index_.find(hash_value);
      if (index_.end() != iter)
      {
        LRU_SUB_MAP_ITERATOR sub_iter = iter->second.find(key);
        value = iter->second.end() != sub_iter ? (*sub_iter->second) : NULL;
      }
      return value;
    }

    int BaseStrategy::gc(const double ratio, std::vector<CacheRootNode*>& values)
    {
      int64_t begin = lru_.size() - calc_gc_count(ratio);
      if (begin < 0)
          begin = 0;
      int32_t iret = begin >= 0 ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int64_t gc_count = calc_gc_count(ratio);
        int64_t count = 0;
        Lru::LRU_LIST_ITERATOR iter = lru_.list_.begin();
        for(; iter != lru_.list_.end() && count < gc_count;)
        {
          if ((*iter)->ref_count_ <= 0)
          {
            ++count;
            values.push_back((*iter));
            int64_t hash_value = (*iter)->get_hash();
            Lru::LRU_MAP_ITERATOR it = lru_.index_.find(hash_value);
            if (lru_.index_.end() != it)
            {
              Lru::LRU_SUB_MAP_ITERATOR sub_iter = it->second.find((*iter)->key_);
              if (it->second.end() != sub_iter)
              {
                if ((*iter) == (*sub_iter->second))
                {
                  it->second.erase(sub_iter);
                }
              }
            }
            lru_.list_.erase(iter++);
          }
          else
          {
            ++iter;
          }
        }
      }
      return iret;
    }

    int64_t BaseStrategy::calc_gc_count(const double ratio)
    {
      return static_cast<int64_t>(lru_.size() * ratio) + 1;
    }
  }/** namemetaserver **/
}/** tfs **/
#endif

