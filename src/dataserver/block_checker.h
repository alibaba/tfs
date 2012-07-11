/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_checker.h 356 2011-05-26 07:51:09Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_BLOCKCHECKER_H_
#define TFS_DATASERVER_BLOCKCHECKER_H_

#include <Mutex.h>
#include "logic_block.h"
#include "dataserver_define.h"
#include "file_repair.h"
#include "requester.h"
//#include "common/config.h"

namespace tfs
{
  namespace dataserver
  {
    class BlockChecker
    {
      public:
        BlockChecker() :
          stop_(0), dataserver_id_(0), ds_requester_(NULL)
        {
        }
        ~BlockChecker();

        int init(const uint64_t dataserver_id, Requester* ds_requester);
        void stop();
        int add_repair_task(common::CrcCheckFile* repair_task);
        int consume_repair_task();
        int do_repair_crc(const common::CrcCheckFile& check_file);
        int do_repair_eio(const uint32_t blockid);
        int expire_error_block();

      private:
        int check_block(const LogicBlock* logic_block);
        int set_error_bitmap(LogicBlock* logic_block);

      private:
        DISALLOW_COPY_AND_ASSIGN(BlockChecker);
        static const int32_t MAX_CHECK_BLOCK_SIZE = 50;

        int stop_;
        uint64_t dataserver_id_;
        FileRepair file_repair_;
        std::deque<common::CrcCheckFile*> check_file_queue_;
        tbutil::Mutex check_mutex_;
        Requester* ds_requester_;
    };
  }
}
#endif //TFS_DATASERVER_BLOCKCHECKER_H_
