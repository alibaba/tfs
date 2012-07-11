/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: define.h 440 2011-06-08 08:41:54Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_DEFINE_H_
#define TFS_COMMON_DEFINE_H_

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#include <stdint.h>
#include <stdlib.h>

namespace tfs
{
  namespace common
  {
    // wrap cdefin.h into namespace.
    // undef maroc to reserve global cdefine's namespace
#undef TFS_COMMON_CDEFINE_H_
#include "cdefine.h"
#undef TFS_COMMON_CDEFINE_H_

#define DISALLOW_COPY_AND_ASSIGN(TypeName)      \
     TypeName(const TypeName&);                  \
     void operator=(const TypeName&)
  }
}
#endif //TFS_COMMON_DEFINE_H_
