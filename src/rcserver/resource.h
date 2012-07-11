/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: resource.h 264 2011-05-06 08:38:41Z daoan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_RESOURCE_H_
#define TFS_RCSERVER_RESOURCE_H_
#include <tbsys.h>
#include <Shared.h>
namespace tfs
{
  namespace rcserver
  {
    class DatabaseHelper;
    class IResource 
    {
      public:
        explicit IResource(DatabaseHelper& database_helper) : database_helper_(database_helper)
        {
        }
        virtual ~IResource()
        {
        }

      public:
        virtual int load() = 0;

      protected:
        DatabaseHelper& database_helper_;

      private:
        DISALLOW_COPY_AND_ASSIGN(IResource);
    };
  }
}
#endif //TFS_RCSERVER_RESOURCE_H_
