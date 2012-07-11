/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: version.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com> 
 *      - initial release
 *
 */
#include "version.h"

namespace tfs
{
  namespace dataserver
  {
    const char* Version::get_build_description()
    {
      return _build_description;
    }
  }
}
