/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: stat.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DSTOOL_TASK_H_
#define TFS_DSTOOL_TASK_H_

#include <stdint.h>

namespace tfs
{
  namespace client
  {
    class Stat
    {
    public:
      virtual ~Stat()
      {
      }
      virtual int32_t summary();
      virtual int32_t init() = 0;
      virtual int32_t output() = 0;

    protected:
      virtual int32_t calc() = 0;
      virtual int32_t save() = 0;
      virtual int32_t refresh() = 0;
    };
  }
}
#endif //TFS_DSTOOL_TASK_H_
