/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_id_factory.h 2140 2011-03-29 01:42:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMESERVER_FSIMAGE_H
#define TFS_NAMESERVER_FSIMAGE_H

#include <Mutex.h>
#include "ns_define.h"

namespace tfs
{
  namespace nameserver
  {
    class BlockIdFactory
    {
      public:
        BlockIdFactory();
        virtual ~BlockIdFactory();
        int initialize(const std::string& path);
        int destroy();
        uint32_t generation(const uint32_t id = 0);
        uint32_t skip(const int32_t num = SKIP_BLOCK_NUMBER);
      private:
        int update(const uint32_t id) const;
        DISALLOW_COPY_AND_ASSIGN(BlockIdFactory);
        static BlockIdFactory instance_;
        tbutil::Mutex mutex_;
        static const uint16_t BLOCK_START_NUMBER;
        static const uint16_t SKIP_BLOCK_NUMBER;
        uint32_t global_id_;
        int32_t  count_;
        int32_t  fd_;
    };
  }/** nameserver **/
}/** tfs **/

#endif
