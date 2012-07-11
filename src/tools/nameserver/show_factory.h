/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SHOWFACTORY_H_
#define TFS_TOOLS_SHOWFACTORY_H_

#include <stdio.h>
#include <vector>
#include "common.h"

namespace tfs
{
  namespace tools
  {
    void compute_tp(common::Throughput* tp, const int32_t time);
    void add_tp(const common::Throughput* atp, const common::Throughput* btp, common::Throughput*, const int32_t sign);
    void print_header(const int8_t print_type, const int8_t type, FILE* fp);

    class ServerShow : public ServerBase
    {
      public:
        ServerShow(){}
        virtual ~ServerShow(){}
        int serialize(tbnet::DataBuffer& output, int32_t& length);
        int deserialize(tbnet::DataBuffer& input, int32_t& length);
        int calculate(ServerShow& old_server);
        void dump(const int8_t flag, FILE* fp) const;
      private:
        void dump(const uint64_t server_id, const std::set<uint32_t>& blocks, FILE* fp) const;
    };

    class BlockShow : public BlockBase
    {
      public:
        BlockShow(){}
        virtual ~BlockShow(){}
        void dump(const int8_t flag, FILE* fp) const;
    };

    class MachineShow
    {
      public:
        MachineShow();
        ~MachineShow(){}
        void dump(const int8_t flag, FILE* fp) const;
        int init(ServerShow& server, ServerShow& old_server);
        int add(ServerShow& server, ServerShow& old_server);
        int calculate();

      public:
        uint64_t machine_id_;
        int64_t use_capacity_;
        int64_t total_capacity_;
        common::Throughput total_tp_;
        common::Throughput last_tp_;
        common::Throughput max_tp_;
        int32_t current_load_;
        int32_t block_count_;
        time_t last_startup_time_;
        time_t consume_time_;
        int32_t index_;

    };

    struct StatStruct
    {
      public:
        int32_t server_count_;
        int32_t machine_count_;
        int64_t use_capacity_;
        int64_t total_capacity_;
        common::Throughput total_tp_;
        common::Throughput last_tp_;
        int32_t current_load_;
        int32_t block_count_;
        time_t last_update_time_;
        int64_t file_count_;
        int32_t block_size_;
        int64_t delfile_count_;
        int32_t block_del_size_;
        StatStruct();
        ~StatStruct();

        int add(ServerShow& server);
        int add(MachineShow& machine);
        int add(BlockShow& block);
        void dump(const int8_t flag, const int8_t sub_flag, FILE* fp) const;
    };
  }
}
#endif
