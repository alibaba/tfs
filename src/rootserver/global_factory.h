/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: global_factory.h 2139 2011-09-05 09:15:26Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_ROOTSERVER_GLOBAL_FACTORY_H_ 
#define TFS_ROOTSERVER_GLOBAL_FACTORY_H_ 

#include <string>
#include "common/rts_define.h"
#include "common/statistics.h"

namespace tfs
{
  namespace rootserver
  {
    struct GFactory
    {
      static int initialize();
      static int wait_for_shut_down();
      static int destroy();
      static common::RsRuntimeGlobalInformation& get_runtime_info()
      {
        return common::RsRuntimeGlobalInformation::instance(); 
      }     
    };
  }
}

#endif


