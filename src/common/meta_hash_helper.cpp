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
#include "meta_hash_helper.h"
#include "serialization.h"

namespace tfs
{
  namespace common
  {
    HashHelper::HashHelper(const int64_t app_id, const int64_t uid)
    {
     assert(TFS_SUCCESS == Serialization::int64_to_char((char*)(&app_id_), sizeof(int64_t), app_id));
     assert(TFS_SUCCESS == Serialization::int64_to_char((char*)(&uid_), sizeof(int64_t), uid));
    }
  }
}
