/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *   duanfei <duanfei@taobao.com>
 *      -modify interface-2012.03.15
 *
 */
#ifndef TFS_NAMESERVER_CLIENT_REQUEST_SERVER_H_
#define TFS_NAMESERVER_CLIENT_REQUEST_SERVER_H_
#include <tbnet.h>
#include "common/lock.h"
#include "block_collect.h"
#include "server_collect.h"
#include "common/base_packet.h"
#include "oplog_sync_manager.h"
namespace tfs
{
  namespace nameserver
  {
    struct CloseParameter
    {
      common::BlockInfo block_info_;
      uint64_t id_;
      uint32_t lease_id_;
      common::UnlinkFlag unlink_flag_;
      common::WriteCompleteStatus status_;
      bool need_new_;
      char error_msg_[256];
    };
    class LayoutManager;
    class NameServer;
    class ClientRequestServer
    {
      public:
        explicit ClientRequestServer(LayoutManager& manager);
        virtual ~ClientRequestServer(){}

        int keepalive(const common::DataServerStatInfo& info, const time_t now);
        int report_block(const uint64_t server, const time_t now, std::set<common::BlockInfo>& blocks);
        int open(uint32_t& block_id, uint32_t& lease_id, int32_t& version,
            common::VUINT64& servers, const int32_t mode, const time_t now);
        int batch_open(const common::VUINT32& blocks, const int32_t mode, const int32_t block_count, std::map<uint32_t, common::BlockInfoSeg>& out);

        int close(CloseParameter& param);

        void dump_plan(tbnet::DataBuffer& output);

        int handle_control_cmd(const common::ClientCmdInformation& info, common::BasePacket* msg, const int64_t buf_length, char* buf);

        int handle(common::BasePacket* msg);

      private:
        int open_read_mode_(common::VUINT64& servers, const uint32_t block) const;
        int open_write_mode_(uint32_t& block_id, uint32_t& lease_id,
            int32_t& version, common::VUINT64& servers, const int32_t mode, const time_t now);
        int batch_open_read_mode_(std::map<uint32_t, common::BlockInfoSeg>& out, const common::VUINT32& blocks) const;
        int batch_open_write_mode_(std::map<uint32_t, common::BlockInfoSeg>& out,const int32_t mode, const int32_t block_count);

        int  handle_control_load_block(const time_t now, const common::ClientCmdInformation& info, common::BasePacket* message, const int64_t buf_length, char* error_buf);
        int  handle_control_delete_block(const time_t now, const common::ClientCmdInformation& info,const int64_t buf_length, char* error_buf);
        int  handle_control_compact_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int  handle_control_immediately_replicate_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int  handle_control_rotate_log(void);
        int  handle_control_set_runtime_param(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int  handle_control_get_balance_percent(const int64_t buf_length, char* error_buf);
        int  handle_control_set_balance_percent(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int  handle_control_clear_system_table(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);

        bool is_discard(void);

      private:
        volatile uint64_t ref_count_;
        LayoutManager& manager_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
#endif

