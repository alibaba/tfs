/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duanfei <duanfei @taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_COMMON_ROOT_SERVER_DEFINE_H_
#define TFS_COMMON_ROOT_SERVER_DEFINE_H_

#include <map>
#include <ext/hash_map>
#include <Time.h>

#include "internal.h"
#include "lock.h"

#ifndef INT64_MAX
#define INT64_MAX 0x7fffffffffffffffLL
#endif

namespace tfs
{
  namespace common
  {
    typedef enum _BucketStatus
    {
      BUCKET_STATUS_NONE = 0, /** uninitialize **/
      BUCKET_STATUS_VERSION_INVALID = 1, /** bucket version invalid **/
      BUCKET_STATUS_READ_ONLY = 2, /** read only **/
      BUCKET_STATUS_RW = 3,   /** read & write **/
    }BucketStatus;

    typedef enum _BuildTableInterruptFlag
    {
      BUILD_TABLE_INTERRUPT_INIT = 1, /** initialie value**/
      BUILD_TABLE_INTERRUPT_ALL  = 2 /** interrupt all(build table, update table) **/
    }BuildTableInterruptFlag;

    struct RSLease
    {
      bool has_valid_lease(const int64_t now) ;
      bool renew(const int32_t step, const int64_t now);
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t lease_id_; /** lease id **/
      int64_t  lease_expired_time_; /** lease expire time (ms) */
    };

    struct MetaServerTables
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t version_; /** table version **/
    };

    struct MetaServerThroughput
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t read_count_;
      int64_t write_count_;
      int64_t cache_dir_count_;
      int64_t cache_dir_hit_count_;
      int64_t cache_file_count_;
      int64_t cache_file_hit_count_;
    };

    struct NetWorkStatInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t recv_packet_count_;
      int64_t recv_bytes_count_;
      int64_t send_packet_count_;
      int64_t send_bytes_count_;
    };

    struct MetaServerCapacity
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t  mem_capacity_;    /** memory total capacity **/
      int64_t  use_mem_capacity_;/** memory use capacity **/
    };

    struct MetaServerBaseInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t id_; /** MetaServer id(IP + PORT) **/
      int64_t  start_time_; /** MetaServer start time (ms)**/
      int64_t  last_update_time_;/** MetaServer last update time (ms)**/
      int32_t  current_load_; /** MetaServer current load **/
      int8_t   status_; /** MetaServer status : META_SERVER_STATUS_NONE, META_SERVER_STATUS_RW, META_SERVER_STATUS_READ_ONLY**/
      int8_t   reserve_[3];
      void update(const MetaServerBaseInformation& info, const int64_t now = time(NULL));
    };

    struct MetaServer
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      RSLease lease_;
      MetaServerTables tables_;
      MetaServerThroughput throughput_;
      NetWorkStatInformation net_work_stat_;
      MetaServerCapacity capacity_;
      MetaServerBaseInformation base_info_;
    };

    struct RootServerBaseInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t id_; /** RootServer id(IP + PORT) **/
      int64_t  start_time_; /** RootServer start time (ms)**/
      int64_t  last_update_time_;/** RootServer last update time (ms)**/
      int8_t   status_;
      int8_t   role_;
    };

    struct RootServerTableVersion
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t active_table_version_;
    };

    struct RootServerInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      RSLease lease_;
      RootServerTableVersion version_;
      RootServerBaseInformation base_info_;
    };

    typedef enum _NewTableItemUpdateStatus
    {
      NEW_TABLE_ITEM_UPDATE_STATUS_BEGIN = 0,
      NEW_TABLE_ITEM_UPDATE_STATUS_RUNNING = 1,
      NEW_TABLE_ITEM_UPDATE_STATUS_END = 2,
      NEW_TABLE_ITEM_UPDATE_STATUS_FAILED = 3
    }NewTableItemUpdateStatus;

    typedef enum _UpdateTablePhase
    {
      UPDATE_TABLE_PHASE_1 = 0,
      UPDATE_TABLE_PHASE_2 = 1
    };

    struct NewTableItem
    {
      tbutil::Time begin_time_;
      tbutil::Time send_msg_time_;
      int8_t status_;
      int8_t phase_;
    };

    typedef enum _DumpTableType
    {
      DUMP_TABLE_TYPE_TABLE = 0,
      DUMP_TABLE_TYPE_ACTIVE_TABLE = 1
    }DumpTableType;

    typedef enum _RsRole
    {
      RS_ROLE_MASTER = 0,
      RS_ROLE_SLAVE  = 1
    }RsRole;

    typedef enum _RsStatus
    {
      RS_STATUS_NONE = -1,
      RS_STATUS_UNINITIALIZE = 0,
      RS_STATUS_OTHERSIDEDEAD = 1,
      RS_STATUS_NO_USE = 2,/* no use*/
      RS_STATUS_INITIALIZED = 3
    }RsStatus;

    typedef enum _RsDestroyFlag
    {
      NS_DESTROY_FLAGS_NO = 0,
      NS_DESTROY_FLAGS_YES = 1
    }RsDestroyFlag;

    struct RsRuntimeGlobalInformation : public RWLock
    {
      uint64_t other_id_;
      int64_t  switch_time_;
      uint32_t vip_;
      RsDestroyFlag destroy_flag_;
      RootServerInformation info_;
      RsRuntimeGlobalInformation();
      bool in_safe_mode_time(const int64_t now);
      bool is_master(void);
      static RsRuntimeGlobalInformation& instance();
      static RsRuntimeGlobalInformation instance_;
    };

    typedef enum _RtsMsKeepAliveType
    {
      RTS_MS_KEEPALIVE_TYPE_LOGIN = 0,
      RTS_MS_KEEPALIVE_TYPE_RENEW = 1,
      RTS_MS_KEEPALIVE_TYPE_LOGOUT = 2
    }RtsMsKeepAliveType;

    typedef enum _RtsRsKeepAliveType
    {
      RTS_RS_KEEPALIVE_TYPE_LOGIN = 0,
      RTS_RS_KEEPALIVE_TYPE_RENEW = 1,
      RTS_RS_KEEPALIVE_TYPE_LOGOUT = 2
    }RtsRsKeepAliveType;

    static const int64_t MAX_BUCKET_ITEM_DEFAULT = 10240;
    static const int64_t MAX_SERVER_ITEM_DEFAULT = 1024;
    static const int64_t MAX_BUCKET_DATA_LENGTH = MAX_BUCKET_ITEM_DEFAULT * INT64_SIZE;

    static const int64_t INVALID_TABLE_VERSION = -1;
    static const int64_t TABLE_VERSION_MAGIC = INT64_MAX - 1;
    static const int16_t RTS_MS_RENEW_LEASE_INTERVAL_TIME_DEFAULT = 1;
    static const int16_t RTS_MS_LEASE_EXPIRED_TIME_DEFAULT = 6;
    static const int16_t RTS_RS_RENEW_LEASE_INTERVAL_TIME_DEFAULT = 1;
    static const int16_t RTS_RS_LEASE_EXPIRED_TIME_DEFAULT = 2;

    typedef __gnu_cxx::hash_map<uint64_t, MetaServer> META_SERVER_MAPS;
    typedef META_SERVER_MAPS::iterator META_SERVER_MAPS_ITER;
    typedef META_SERVER_MAPS::const_iterator META_SERVER_MAPS_CONST_ITER;
    typedef std::map<uint64_t, NewTableItem> NEW_TABLE;
    typedef NEW_TABLE::iterator NEW_TABLE_ITER;
    typedef NEW_TABLE::const_iterator NEW_TABLE_CONST_ITER;
    typedef __gnu_cxx::hash_map<uint64_t, RootServerInformation> ROOT_SERVER_MAPS;
    typedef ROOT_SERVER_MAPS::iterator ROOT_SERVER_MAPS_ITER;
    typedef ROOT_SERVER_MAPS::const_iterator ROOT_SERVER_MAPS_CONST_ITER;
  } /** common **/
} /** tfs **/

#endif
