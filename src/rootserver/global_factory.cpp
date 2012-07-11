/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: global_factory.cpp 2139 2011-09-05 09:15:26Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "global_factory.h"
#include "common/internal.h"
#include "common/error_msg.h"

using namespace tfs::common;

namespace tfs
{
  namespace rootserver
  {
    int GFactory::initialize()
    {
      return common::TFS_SUCCESS;
    }

    int GFactory::wait_for_shut_down()
    {
      return common::TFS_SUCCESS;
    }

    int GFactory::destroy()
    {
      return common::TFS_SUCCESS;
    }
  }/** rootserver **/
}/** tfs **/
