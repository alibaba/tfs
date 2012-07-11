/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_status.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com> 
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_BLOCKSTATUS_H_
#define TFS_DATASERVER_BLOCKSTATUS_H_

#include <tbsys.h>

namespace tfs
{
  namespace dataserver
  {

    class BlockStatus
    {
      public:
        BlockStatus()
        {
          reset();
        }

        ~BlockStatus()
        {
        }

        inline void reset()
        {
          atomic_set(&crc_error_, 0);
          atomic_set(&eio_error_, 0);
          atomic_set(&oper_warn_, 0);
        }

        inline int add_crc_error()
        {
          return atomic_add_return(1, &crc_error_);
        }

        inline int get_crc_error() const
        {
          return atomic_read(&crc_error_);
        }

        inline int add_eio_error()
        {
          return atomic_add_return(1, &eio_error_);
        }

        inline int get_eio_error() const
        {
          return atomic_read(&eio_error_);
        }

        inline int add_oper_warn()
        {
          return atomic_add_return(1, &oper_warn_);
        }

        inline int get_oper_warn() const
        {
          return atomic_read(&oper_warn_);
        }

      private:
        atomic_t crc_error_;
        atomic_t eio_error_;
        atomic_t oper_warn_;
    };

  }
}

#endif //TFS_DATASERVER_BLOCKSTATUS_H_
