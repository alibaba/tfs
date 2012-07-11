/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session_pool.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "tfs_session_pool.h"

using namespace tfs::client;
using namespace tfs::common;

TfsSessionPool TfsSessionPool::g_session_pool_;

TfsSessionPool::TfsSessionPool()
{
}

TfsSessionPool::~TfsSessionPool()
{
  tbutil::Mutex::Lock lock(mutex_);
  SESSION_MAP::iterator it = pool_.begin();
  for (; it != pool_.end(); it++)
  {
    tbsys::gDelete(it->second);
  }
  pool_.clear();
}

TfsSession* TfsSessionPool::get(const char* ns_addr, const int64_t cache_time, const int64_t cache_items)
{
  tbutil::Mutex::Lock lock(mutex_);
  SESSION_MAP::iterator it = pool_.find(ns_addr);
  if (it != pool_.end())
  {
    return it->second;
  }
  TfsSession* session = new TfsSession(ns_addr, cache_time, cache_items);
  int32_t ret = session->initialize();
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize tfs session failed, ret: %d", ret);
    tbsys::gDelete(session);
    return NULL;
  }
  pool_[ns_addr] = session;
  return session;
}

void TfsSessionPool::release(TfsSession* session)
{
  tbutil::Mutex::Lock lock(mutex_);
  if (session)
  {
    pool_.erase(session->get_ns_addr_str());
    tbsys::gDelete(session);
  }
}
