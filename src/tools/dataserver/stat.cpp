/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: stat.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "stat.h"

namespace tfs
{
  namespace client
  {
    int32_t Stat::summary()
    {
      int32_t ret = 0;
      ret = refresh();
      if (ret)
      {
        return ret;
      }
      ret = calc();
      if (ret)
      {
        return ret;
      }
      ret = output();
      if (ret)
      {
        return ret;
      }
      ret = save();
      if (ret)
      {
        return ret;
      }
      return 0;
    }
  }
}

