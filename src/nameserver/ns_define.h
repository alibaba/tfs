/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ns_define.h 983 2011-10-31 09:59:33Z duanfei $
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
#ifndef TFS_NAMESERVER_DEFINE_H_
#define TFS_NAMESERVER_DEFINE_H_

#include <Mutex.h>
#include <tbsys.h>
#include "common/internal.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/parameter.h"
#include "common/new_client.h"
#include "common/array_helper.h"

namespace tfs
{
  namespace nameserver
  {
    enum NsRole
    {
      NS_ROLE_NONE = 0x00,
      NS_ROLE_MASTER,
      NS_ROLE_SLAVE
    };

    enum NsStatus
    {
      NS_STATUS_NONE = -1,
      NS_STATUS_UNINITIALIZE = 0x00,
      NS_STATUS_INITIALIZED
    };

    enum NsSwitchFlag
    {
      NS_SWITCH_FLAG_NO = 0x00,
      NS_SWITCH_FLAG_YES
    };

    enum ReportBlockStatus
    {
      REPORT_BLOCK_STATUS_NONE = 0x0,
      REPORT_BLOCK_STATUS_IN_REPORT_QUEUE,
      REPORT_BLOCK_STATUS_REPORTING,
      REPORT_BLOCK_STATUS_COMPLETE
    };

    enum HandleDeleteBlockFlag
    {
      HANDLE_DELETE_BLOCK_FLAG_BOTH = 1,
      HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION = 2
    };

    enum NsKeepAliveType
    {
      NS_KEEPALIVE_TYPE_LOGIN = 0,
      NS_KEEPALIVE_TYPE_RENEW = 1,
      NS_KEEPALIVE_TYPE_LOGOUT = 2
    };

    enum BlockInReplicateQueueFlag
    {
      BLOCK_IN_REPLICATE_QUEUE_NO  = 0,
      BLOCK_IN_REPLICATE_QUEUE_YES = 1
    };

    enum BlockCompareServerFlag
    {
      BLOCK_COMPARE_SERVER_BY_ID = 0,
      BLOCK_COMPARE_SERVER_BY_POINTER = 1,
      BLOCK_COMPARE_SERVER_BY_ID_POINTER = 2
    };

    class LayoutManager;
    class GCObject
    {
    public:
      explicit GCObject(const time_t now):
        last_update_time_(now) {}
      virtual ~GCObject() {}
      virtual void callback(LayoutManager& ) {}
      inline void free(){ delete this;}
      inline time_t get_last_update_time() const { return last_update_time_;}
      inline void update_last_time(const time_t now = common::Func::get_monotonic_time()) { last_update_time_ = now;}
      inline bool can_be_clear(const time_t now) const
      {
        return now >= (last_update_time_ + common::SYSPARAM_NAMESERVER.object_clear_max_time_);
      }
      inline bool can_be_free(const time_t now) const
      {
        return now >= (last_update_time_ + common::SYSPARAM_NAMESERVER.object_dead_max_time_);
      }
    protected:
      time_t last_update_time_;
    };

    struct NsGlobalStatisticsInfo : public common::RWLock
    {
      NsGlobalStatisticsInfo();
      NsGlobalStatisticsInfo(uint64_t use_capacity, uint64_t totoal_capacity, uint64_t total_block_count, int32_t total_load,
          int32_t max_load, int32_t max_block_count, int32_t alive_server_count);
			void update(const common::DataServerStatInfo& info, const bool is_new = true);
      void update(const NsGlobalStatisticsInfo& info);
      static NsGlobalStatisticsInfo& instance();
      void dump(int32_t level, const char* file = __FILE__, const int32_t line = __LINE__, const char* function =
          __FUNCTION__) const;
      volatile int64_t use_capacity_;
      volatile int64_t total_capacity_;
      volatile int64_t total_block_count_;
      int32_t total_load_;
      int32_t max_load_;
      int32_t max_block_count_;
      volatile int32_t alive_server_count_;
      static NsGlobalStatisticsInfo instance_;
    };

    struct NsRuntimeGlobalInformation
    {
      uint64_t owner_ip_port_;
      uint64_t peer_ip_port_;
      int64_t switch_time_;
      int64_t discard_newblk_safe_mode_time_;
      int64_t last_owner_check_time_;
      int64_t last_push_owner_check_packet_time_;
      int64_t lease_id_;
      int64_t lease_expired_time_;
      int64_t startup_time_;
      uint32_t vip_;
      bool destroy_flag_;
      int8_t owner_role_;
      int8_t peer_role_;
      int8_t owner_status_;
      int8_t peer_status_;

      bool is_destroyed() const;
      bool in_safe_mode_time(const int64_t now) const;
      bool in_discard_newblk_safe_mode_time(const int64_t now) const;
      bool is_master() const;
      bool peer_is_master() const;
      int keepalive(int64_t& lease_id, const uint64_t server,
         const int8_t role, const int8_t status, const int8_t type, const time_t now);
      bool logout();
      bool has_valid_lease(const time_t now) const;
      bool renew(const int32_t step, const time_t now);
      bool renew(const int64_t lease_id, const int32_t step, const time_t now);
      void switch_role(const bool startup = false, const int64_t now = common::Func::get_monotonic_time());
      void update_peer_info(const uint64_t server, const int8_t role, const int8_t status);
      bool own_is_initialize_complete() const;
      void initialize();
      void destroy();
      void dump(int32_t level, const char* format = NULL);
      NsRuntimeGlobalInformation();
      static NsRuntimeGlobalInformation& instance();
      static NsRuntimeGlobalInformation instance_;
    };

    static const int32_t THREAD_STATCK_SIZE = 16 * 1024 * 1024;
    static const int32_t MAX_SERVER_NUMS = 3000;
    static const int32_t MAX_PROCESS_NUMS = MAX_SERVER_NUMS * 12;
    static const int32_t MAX_BLOCK_CHUNK_NUMS = 10240 * 4;
    static const int32_t MAX_REPLICATION = 64;
    static const int32_t MAX_WRITE_FILE_COUNT = 256;

    static const uint64_t GB = 1 * 1024 * 1024 * 1024;
    static const uint64_t MB = 1 * 1024 * 1024;
    static const double PERCENTAGE_MIN = 0.000001;
    static const double PERCENTAGE_MAX = 1.000000;
    static const double PERCENTAGE_MAGIC = 1000000.0;
    double calc_capacity_percentage(const uint64_t capacity, const uint64_t total_capacity);

    static const int32_t MAX_POP_SERVER_FROM_DEAD_QUEUE_LIMIT = 5;

    class BlockCollect;
    class ServerCollect;

    extern int ns_async_callback(common::NewClient* client);
    extern std::string& print_servers(const common::ArrayHelper<ServerCollect*>&servers, std::string& result);
    extern void print_servers(const common::ArrayHelper<uint64_t>&servers, std::string& result);
    extern void print_servers(const std::vector<uint64_t>& servers, std::string& result);
    extern void print_blocks(const std::vector<uint32_t>& blocks, std::string& result);
 }/** nameserver **/
}/** tfs **/

#endif
