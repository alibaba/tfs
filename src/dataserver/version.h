/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: version.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com> 
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_VERSION_H_
#define TFS_DATASERVER_VERSION_H_

namespace tfs
{
  namespace dataserver
  {
#if defined(__DATE__) && defined(__TIME__) && defined(PACKAGE) && defined(VERSION)
    static const char _build_description[] = "TaoBao File System(TFS), Version: " VERSION ", Build Time: " __DATE__ " " __TIME__;
#else
    static const char _build_description[] = "unknown";
#endif

    class Version
    {
      public:
        Version();
        static const char* get_build_description();
    };

  }
}
#endif //TFS_DATASERVER_VERSION_H_
