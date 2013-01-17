/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_manager.h 983 2011-10-31 09:59:33Z duanfei $
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
#ifndef TFS_NAMESERVER_HEART_MANAGEMENT_
#define TFS_NAMESERVER_HEART_MANAGEMENT_

#include <Timer.h>
#include "message/message_factory.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    class HeartManagement
    {
    public:
      explicit HeartManagement(NameServer& manager);
      virtual ~HeartManagement();
      int initialize(const int32_t keepalive_thread_count, const int32_t report_block_thread_count);
      void wait_for_shut_down();
      void destroy();
      int push(common::BasePacket* msg);
    private:
      class KeepAliveIPacketQueueHeaderHelper : public tbnet::IPacketQueueHandler
      {
      public:
        explicit KeepAliveIPacketQueueHeaderHelper(HeartManagement& manager): manager_(manager){};
        virtual ~KeepAliveIPacketQueueHeaderHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHeaderHelper);
        HeartManagement& manager_;
      };
      class ReportBlockIPacketQueueHeaderHelper: public tbnet::IPacketQueueHandler
      {
      public:
        explicit ReportBlockIPacketQueueHeaderHelper(HeartManagement& manager): manager_(manager){};
        virtual ~ReportBlockIPacketQueueHeaderHelper(){}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(ReportBlockIPacketQueueHeaderHelper);
        HeartManagement& manager_;
      };
    private:
      DISALLOW_COPY_AND_ASSIGN(HeartManagement);
      int keepalive(tbnet::Packet* packet);
      int report_block(tbnet::Packet* packet);
      NameServer& manager_;
      tbnet::PacketQueueThread keepalive_threads_;
      tbnet::PacketQueueThread report_block_threads_;
      KeepAliveIPacketQueueHeaderHelper keepalive_queue_header_;
      ReportBlockIPacketQueueHeaderHelper report_block_queue_header_;
    };

    class NameServerHeartManager: public tbnet::IPacketQueueHandler
    {
        friend class NameServer;
     public:
        explicit NameServerHeartManager(LayoutManager& manager);
        virtual ~NameServerHeartManager();
        int initialize();
        int wait_for_shut_down();
        int destroy();
        int push(common::BasePacket* message, const int32_t max_queue_size = 0, const bool block = false);
        virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);
      private:
        class CheckThreadHelper : public tbutil::Thread
        {
          public:
            explicit CheckThreadHelper(NameServerHeartManager& manager): manager_(manager) { start();}
            virtual ~CheckThreadHelper() {}
            void run();
          private:
            NameServerHeartManager& manager_;
            DISALLOW_COPY_AND_ASSIGN(CheckThreadHelper);
        };
        typedef tbutil::Handle<CheckThreadHelper> CheckThreadHelperPtr;
      private:
        int keepalive_(common::BasePacket* message);
        int keepalive_(int32_t& sleep_time, NsKeepAliveType& type, NsRuntimeGlobalInformation& ngi);
        void check_();
        bool check_vip_(const NsRuntimeGlobalInformation& ngi) const;
        int ns_role_establish_(NsRuntimeGlobalInformation& ngi, const time_t now);
        int establish_peer_role_(NsRuntimeGlobalInformation& ngi);
        int ns_check_lease_expired_(NsRuntimeGlobalInformation& ngi, const time_t now);

        void switch_role_master_to_slave_(NsRuntimeGlobalInformation& ngi, const time_t now);
        void switch_role_salve_to_master_(NsRuntimeGlobalInformation& ngi, const time_t now);

        int keepalive_in_heartbeat_(common::BasePacket* message);
      private:
        LayoutManager& manager_;
        CheckThreadHelperPtr check_thread_;
        tbnet::PacketQueueThread work_thread_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
#endif

