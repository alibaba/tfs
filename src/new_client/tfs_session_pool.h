/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session_pool.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSSESSION_POOL_H_
#define TFS_CLIENT_TFSSESSION_POOL_H_

#include <map>
#include <tbsys.h>
#include <Mutex.h>
#include "tfs_session.h"

namespace tfs
{
  namespace client
  {
#define SESSION_POOL TfsSessionPool::get_instance()
    class TfsSessionPool
    {
      typedef std::map<std::string, TfsSession*> SESSION_MAP;
    public:
      TfsSessionPool();
      virtual ~TfsSessionPool();

      inline static TfsSessionPool& get_instance()
      {
        return g_session_pool_;
      }

      TfsSession* get(const char* ns_addr, const int64_t cache_time = ClientConfig::cache_time_,
                      const int64_t cache_items = ClientConfig::cache_items_);
      void release(TfsSession* session);

    private:
      DISALLOW_COPY_AND_ASSIGN( TfsSessionPool);
      tbutil::Mutex mutex_;
      SESSION_MAP pool_;
      static TfsSessionPool g_session_pool_;
    };
  }
}

#endif /* TFSSESSION_POOL_H_ */
