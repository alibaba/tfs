/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog_sync_manager.h 596 2011-07-21 10:03:24Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_OPERATION_LOG_SYNC_MANAGER_H_
#define TFS_NAMESERVER_OPERATION_LOG_SYNC_MANAGER_H_

#include <deque>
#include <map>
#include <Mutex.h>
#include <Monitor.h>
#include <Timer.h>
#include "common/file_queue.h"
#include "common/file_queue_thread.h"
#include "message/message_factory.h"

#include "oplog.h"
#include "block_id_factory.h"

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    class OpLogSyncManager: public tbnet::IPacketQueueHandler
    {
      friend class FlushOpLogTimerTask;
    public:
      explicit OpLogSyncManager(LayoutManager& mm);
      virtual ~OpLogSyncManager();
      int initialize();
      int wait_for_shut_down();
      int destroy();
      int register_slots(const char* const data, const int64_t length) const;
      void switch_role();
      int log(const uint8_t type, const char* const data, const int64_t length, const time_t now);
      int push(common::BasePacket* msg, int32_t max_queue_size = 0, bool block = false);
      inline common::FileQueueThread* get_file_queue_thread() const { return file_queue_thread_;}
      int replay_helper(const char* const data, const int64_t data_len, int64_t& pos, const time_t now = common::Func::get_monotonic_time());
      int replay_helper_do_msg(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos);
      int replay_helper_do_oplog(const time_t now, const int32_t type, const char* const data, const int64_t data_len, int64_t& pos);

      inline uint32_t generation(const uint32_t id = 0) { return id_factory_.generation(id);}
    private:
      DISALLOW_COPY_AND_ASSIGN( OpLogSyncManager);
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);
      static int sync_log_func(const void* const data, const int64_t len, const int32_t threadIndex, void *arg);
      int send_log_(const char* const data, const int64_t length);
      int transfer_log_msg_(common::BasePacket* msg);
      int recv_log_(common::BasePacket* msg);
      int replay_all_();
    private:
      LayoutManager& manager_;
      OpLog* oplog_;
      common::FileQueue* file_queue_;
      common::FileQueueThread* file_queue_thread_;
      BlockIdFactory id_factory_;
      tbutil::Mutex mutex_;
      tbnet::PacketQueueThread work_thread_;
    };
  }//end namespace nameserver
}//end namespace tfs
#endif
