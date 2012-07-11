/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lock.h 185 2011-04-07 06:31:43Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_LOCK_H_
#define TFS_COMMON_LOCK_H_

#include <pthread.h>
#include <tbsys.h>
#include <tblog.h>


namespace tfs
{
  namespace common
  {

    enum ELockMode
    {
      NO_PRIORITY,
      WRITE_PRIORITY,
      READ_PRIORITY
    };

    class ScopedRWLock;
    class RWLock
    {
      public:
        typedef ScopedRWLock Lock;
        RWLock(ELockMode lockMode = NO_PRIORITY);
        virtual ~RWLock();

        int rdlock() ;
        int wrlock() ;
        int tryrdlock() ;
        int trywrlock() ;
        int unlock() ;

      private:
        pthread_rwlock_t rwlock_;
    };

    enum ELockType
    {
      READ_LOCKER,
      WRITE_LOCKER
    };

    class ScopedRWLock
    {
      public:
        ScopedRWLock(RWLock& locker, const ELockType lock_type)
          : locker_(locker)
        {
          if (lock_type == READ_LOCKER)
          {
            int ret = locker_.rdlock();
            if (0 !=ret)
            {
              TBSYS_LOG(WARN , "lock failed, ret: %d", ret);
            }
          }
          else
          {
            int ret = locker_.wrlock();
            if (0 !=ret)
            {
              TBSYS_LOG(WARN , "lock failed, ret: %d", ret);
            }
          }
        }

        ~ScopedRWLock()
        {
          int ret = locker_.unlock();
          if (0 !=ret)
          {
            TBSYS_LOG(WARN , "unlock failed, ret: %d", ret);
          }
        }

      private:
        RWLock& locker_;
    };

  }
}

#endif //TFS_COMMON_LOCK_H_
