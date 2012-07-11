/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: server_helper.h 432 2012-04-13 22:06:11Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CHECKSERVER_SERVERHELPER_H
#define TFS_CHECKSERVER_SERVERHELPER_H

#include "tbnet.h"
#include <Handle.h>
#include "common/internal.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "common/config_item.h"


namespace tfs
{
  namespace checkserver
  {
    class ServerStat
    {
      public:
        ServerStat();
        virtual ~ServerStat();

        int deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset);

#ifdef TFS_NS_DEBUG
        int64_t total_elect_num_;
#endif
        uint64_t id_;
        int64_t use_capacity_;
        int64_t total_capacity_;
        common::Throughput total_tp_;
        common::Throughput last_tp_;
        int32_t current_load_;
        int32_t block_count_;
        time_t last_update_time_;
        time_t startup_time_;
        time_t current_time_;
        common::DataServerLiveStatus status_;
   };

   class ServerHelper
   {
     public:

       /**
       * @brief get data server list
       *
       * @param ns_ip: nameserver ip
       * @param ds_list: reslut
       *
       * @return 0 on success
       */
       static int get_ds_list(const uint64_t ns_ip, common::VUINT64& ds_list);

       /**
       * @brief check dataserver
       *
       * @param ds_id: dataserver id
       * @param check_result: check result vector
       *
       * @return
       */
       static int check_ds(const uint64_t ds_id, const uint32_t check_time,
           const uint32_t last_check_time, common::CheckBlockInfoVec& check_result);


       /**
       * @brief get logic block location info
       *
       * @param server_id: nameserver address
       * @param block_id: logic block id
       * @param ds_list: result
       *
       * @return
       */
      static int get_block_ds_list(const uint64_t server_id, const uint32_t block_id, common::VUINT64& ds_list);

      /**
      * @brief check logic block
      *
      * @param ns_id: nameserver id
      * @param ds_id:
      * @param check_result
      *
      * @return
      */
      static int check_block(const uint64_t ns_id, const uint32_t block_id,
            common::CheckBlockInfoVec& check_result);
   };

  }
}

#endif

