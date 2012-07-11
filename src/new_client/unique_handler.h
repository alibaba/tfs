/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_UNIQUEHANDLER_H_
#define TFS_CLIENT_UNIQUEHANDLER_H_

namespace tfs
{
  namespace client
  {
    template<typename K, typename V>
    class UniqueHandler
    {
    public:
      UniqueHandler(){}
      virtual ~UniqueHandler(){}

      virtual int query(K& key, V& value) = 0;
      virtual int insert(K& key, V& value) = 0;
      virtual int32_t decrease(K& key, V& value, const int32_t count = 1) = 0;
      virtual int32_t increase(K& key, V& value, const int32_t count = 1) = 0;
      virtual int erase(K& key) = 0;
    };
  }
}

#endif
