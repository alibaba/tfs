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
#ifndef TFS_COMMON_META_HASH_HELPER_H_
#define TFS_COMMON_META_HASH_HELPER_H_
#include <stdint.h>
namespace tfs
{
  namespace common
  {
    struct HashHelper
    {
      HashHelper(const int64_t app_id, const int64_t uid);
      int64_t app_id_;
      int64_t uid_;
    };
  }
}
#endif
