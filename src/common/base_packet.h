/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_PACKET_H_
#define TFS_COMMON_BASE_PACKET_H_

#include <tbsys.h>
#include <tbnet.h>
#include <Memory.hpp>
#include "internal.h"
#include "func.h"
#include "stream.h"
#include "serialization.h"

namespace tfs
{
  namespace common
  {
    //old structure
    #pragma pack(4)
    struct TfsPacketNewHeaderV0
    {
      uint32_t flag_;
      int32_t length_;
      int16_t type_;
      int16_t check_;
      int serialize(char*data, const int64_t data_len, int64_t& pos) const
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, flag_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, check_);
        }
        return iret;
      }

      int deserialize(char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&flag_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, &length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &check_);
        }
        return iret;
      }

      int64_t length() const
      {
        return INT_SIZE * 3;
      }
    };

    //new structure
    struct TfsPacketNewHeaderV1
    {
      uint64_t id_;
      uint32_t flag_;
      uint32_t crc_;
      int32_t  length_;
      int16_t  type_;
      int16_t  version_;
      int serialize(char*data, const int64_t data_len, int64_t& pos) const
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, flag_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int16(data, data_len, pos, version_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int64(data, data_len, pos, id_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::set_int32(data, data_len, pos, crc_);
        }
        return iret;
      }

      int deserialize(const char*data, const int64_t data_len, int64_t& pos)
      {
        int32_t iret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&flag_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, &length_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &type_);
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int16(data, data_len, pos, &version_);
        }
        /*if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&id_));
        }
        if (TFS_SUCCESS == iret)
        {
          iret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&crc_));
        }*/
        return iret;
      }

      int64_t length() const
      {
        return INT_SIZE * 4 + INT64_SIZE;
      }
    };
  #pragma pack()

    enum DirectionStatus
    {
      DIRECTION_RECEIVE = 1,
      DIRECTION_SEND = 2,
      DIRECTION_MASTER_SLAVE_NS = 4
    };

    enum HeartMessageStatus
    {
      HEART_MESSAGE_OK = 0,
      HEART_NEED_SEND_BLOCK_INFO = 1,
      HEART_EXP_BLOCK_ID = 2,
      HEART_MESSAGE_FAILED = 3,
      HEART_REPORT_BLOCK_SERVER_OBJECT_NOT_FOUND = 4,
      HEART_REPORT_UPDATE_RELATION_ERROR = 5
    };

    enum ReportBlockMessageStatus
    {
      REPORT_BLOCK_OK = 0,
      REPORT_BLOCK_SERVER_OBJECT_NOT_FOUND = 1,
      REPORT_BLOCK_UPDATE_RELATION_ERROR   = 2,
      REPORT_BLOCK_OTHER_ERROR = 3
    };

    enum TfsPacketVersion
    {
      TFS_PACKET_VERSION_V0 = 0,
      TFS_PACKET_VERSION_V1 = 1,
      TFS_PACKET_VERSION_V2 = 2
    };

    enum MessageType
    {
      STATUS_MESSAGE = 1,
      GET_BLOCK_INFO_MESSAGE = 2,
      SET_BLOCK_INFO_MESSAGE = 3,
      CARRY_BLOCK_MESSAGE = 4,
      SET_DATASERVER_MESSAGE = 5,
      UPDATE_BLOCK_INFO_MESSAGE = 6,
      READ_DATA_MESSAGE = 7,
      RESP_READ_DATA_MESSAGE = 8,
      WRITE_DATA_MESSAGE = 9,
      CLOSE_FILE_MESSAGE = 10,
      UNLINK_FILE_MESSAGE = 11,
      REPLICATE_BLOCK_MESSAGE = 12,
      COMPACT_BLOCK_MESSAGE = 13,
      GET_SERVER_STATUS_MESSAGE = 14,
      SHOW_SERVER_INFORMATION_MESSAGE = 15,
      SUSPECT_DATASERVER_MESSAGE = 16,
      FILE_INFO_MESSAGE = 17,
      RESP_FILE_INFO_MESSAGE = 18,
      RENAME_FILE_MESSAGE = 19,
      CLIENT_CMD_MESSAGE = 20,
      CREATE_FILENAME_MESSAGE = 21,
      RESP_CREATE_FILENAME_MESSAGE = 22,
      ROLLBACK_MESSAGE = 23,
      RESP_HEART_MESSAGE = 24,
      RESET_BLOCK_VERSION_MESSAGE = 25,
      BLOCK_FILE_INFO_MESSAGE = 26,
      LEGACY_UNIQUE_FILE_MESSAGE = 27,
      LEGACY_RETRY_COMMAND_MESSAGE = 28,
      NEW_BLOCK_MESSAGE = 29,
      REMOVE_BLOCK_MESSAGE = 30,
      LIST_BLOCK_MESSAGE = 31,
      RESP_LIST_BLOCK_MESSAGE = 32,
      BLOCK_WRITE_COMPLETE_MESSAGE = 33,
      BLOCK_RAW_META_MESSAGE = 34,
      WRITE_RAW_DATA_MESSAGE = 35,
      WRITE_INFO_BATCH_MESSAGE = 36,
      BLOCK_COMPACT_COMPLETE_MESSAGE = 37,
      READ_DATA_MESSAGE_V2 = 38,
      RESP_READ_DATA_MESSAGE_V2 = 39,
      LIST_BITMAP_MESSAGE =40,
      RESP_LIST_BITMAP_MESSAGE = 41,
      RELOAD_CONFIG_MESSAGE = 42,
      SERVER_META_INFO_MESSAGE = 43,
      RESP_SERVER_META_INFO_MESSAGE = 44,
      READ_RAW_DATA_MESSAGE = 45,
      RESP_READ_RAW_DATA_MESSAGE = 46,
      REPLICATE_INFO_MESSAGE = 47,
      ACCESS_STAT_INFO_MESSAGE = 48,
      READ_SCALE_IMAGE_MESSAGE = 49,
      OPLOG_SYNC_MESSAGE = 50,
      OPLOG_SYNC_RESPONSE_MESSAGE = 51,
      MASTER_AND_SLAVE_HEART_MESSAGE = 52,
      MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE = 53,
      HEARTBEAT_AND_NS_HEART_MESSAGE = 54,
      //OWNER_CHECK_MESSAGE = 55,
      GET_BLOCK_LIST_MESSAGE = 56,
      CRC_ERROR_MESSAGE = 57,
      ADMIN_CMD_MESSAGE = 58,
      BATCH_GET_BLOCK_INFO_MESSAGE = 59,
      BATCH_SET_BLOCK_INFO_MESSAGE = 60,
      REMOVE_BLOCK_RESPONSE_MESSAGE = 61,
      READ_DATA_MESSAGE_V3 = 62,
      RESP_READ_DATA_MESSAGE_V3 = 63,
      DUMP_PLAN_MESSAGE = 64,
      DUMP_PLAN_RESPONSE_MESSAGE = 65,
      REQ_RC_LOGIN_MESSAGE = 66,
      RSP_RC_LOGIN_MESSAGE = 67,
      REQ_RC_KEEPALIVE_MESSAGE = 68,
      RSP_RC_KEEPALIVE_MESSAGE = 69,
      REQ_RC_LOGOUT_MESSAGE = 70,
      REQ_RC_RELOAD_MESSAGE = 71,
      GET_DATASERVER_INFORMATION_MESSAGE = 72,
      GET_DATASERVER_INFORMATION_RESPONSE_MESSAGE = 73,
      FILEPATH_ACTION_MESSAGE = 74,
      WRITE_FILEPATH_MESSAGE = 75,
      READ_FILEPATH_MESSAGE = 76,
      RESP_READ_FILEPATH_MESSAGE = 77,
      LS_FILEPATH_MESSAGE = 78,
      RESP_LS_FILEPATH_MESSAGE = 79,
      REQ_RT_UPDATE_TABLE_MESSAGE = 80,
      RSP_RT_UPDATE_TABLE_MESSAGE = 81,
      REQ_RT_MS_KEEPALIVE_MESSAGE = 82,
      RSP_RT_MS_KEEPALIVE_MESSAGE = 83,
      REQ_RT_GET_TABLE_MESSAGE = 84,
      RSP_RT_GET_TABLE_MESSAGE = 85,
      REQ_RT_RS_KEEPALIVE_MESSAGE = 86,
      RSP_RT_RS_KEEPALIVE_MESSAGE = 87,
      REQ_CALL_DS_REPORT_BLOCK_MESSAGE = 88,
      REQ_REPORT_BLOCKS_TO_NS_MESSAGE  = 89,
      RSP_REPORT_BLOCKS_TO_NS_MESSAGE  = 90,
      REQ_CHECK_BLOCK_MESSAGE = 200,
      RSP_CHECK_BLOCK_MESSAGE = 201,
      LOCAL_PACKET = 500
    };

    // StatusMessage status value
    enum StatusMessageStatus
    {
      STATUS_MESSAGE_OK = 0,
      STATUS_MESSAGE_ERROR,
      STATUS_NEED_SEND_BLOCK_INFO,
      STATUS_MESSAGE_PING,
      STATUS_MESSAGE_REMOVE,
      STATUS_MESSAGE_BLOCK_FULL,
      STATUS_MESSAGE_ACCESS_DENIED
    };

		static const uint32_t TFS_PACKET_FLAG_V0 = 0x4d534654;//TFSM
		static const uint32_t TFS_PACKET_FLAG_V1 = 0x4e534654;//TFSN(V1)
		static const int32_t TFS_PACKET_HEADER_V0_SIZE = sizeof(TfsPacketNewHeaderV0);
		static const int32_t TFS_PACKET_HEADER_V1_SIZE = sizeof(TfsPacketNewHeaderV1);
		static const int32_t TFS_PACKET_HEADER_DIFF_SIZE = TFS_PACKET_HEADER_V1_SIZE - TFS_PACKET_HEADER_V0_SIZE;

    class BasePacket: public tbnet::Packet
    {
    public:
      typedef BasePacket* (*create_packet_handler)(int32_t);
      typedef std::map<int32_t, create_packet_handler> CREATE_PACKET_MAP;
      typedef CREATE_PACKET_MAP::iterator CREATE_PACKET_MAP_ITER;
    public:
      BasePacket();
      virtual ~BasePacket();
      virtual bool copy(BasePacket* src, const int32_t version, const bool deserialize);
      bool encode(tbnet::DataBuffer* output);
      bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);

      virtual int serialize(Stream& output) const = 0;
      virtual int deserialize(Stream& input) = 0;
      virtual int64_t length() const = 0;
      int64_t get_data_length() const { return stream_.get_data_length();}
      virtual int reply(BasePacket* packet);
      int reply_error_packet(const int32_t level, const char* file, const int32_t line,
               const char* function, const int32_t error_code, const char* fmt, ...);
      virtual void dump() const {}

      inline bool is_enable_dump() const { return dump_flag_;}
      inline void enable_dump() { dump_flag_ = true;}
      inline void disable_dump() { dump_flag_ = false;}
      //inline void set_auto_free(const bool auto_free = true) { auto_free_ = auto_free;}
      //inline bool get_auto_free() const { return auto_free_;}
      //virtual void free() { if (auto_free_) { delete this;} }
      virtual void free() { delete this;}
      inline void set_connection(tbnet::Connection* connection) { connection_ = connection;}
      inline tbnet::Connection* get_connection() const { return connection_;}
      inline void set_direction(const DirectionStatus direction) { direction_ = direction; }
      inline DirectionStatus get_direction() const { return direction_;}
      inline void set_version(const int32_t version) { version_ = version;}
      inline int32_t get_version() const { return version_;}
      inline void set_crc(const uint32_t crc) { crc_ = crc;}
      inline uint32_t get_crc() const { return crc_;}
      inline void set_id(const uint64_t id) { id_ = id;}
      inline uint64_t get_id() const { return id_;}

      static bool parse_special_ds(std::vector<uint64_t>& value, int32_t& version, uint32_t& lease);

  #ifdef TFS_GTEST
    public:
  #else
    protected:
  #endif
      Stream stream_;
      tbnet::Connection* connection_;
      uint64_t id_;
      uint32_t crc_;
      DirectionStatus direction_;
      int32_t version_;
      static const int16_t MAX_ERROR_MSG_LENGTH = 511; /** not include '\0'*/
      //bool auto_free_;
      bool dump_flag_;
    };
  } /** common **/
}/** tfs **/
#endif //TFS_COMMON_BASE_PACKET_H_
