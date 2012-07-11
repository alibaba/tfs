/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: show.h 432 2011-06-08 07:06:11Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SHOW_H_
#define TFS_TOOLS_SHOW_H_

#include <stdio.h>
#include <vector>
#include "show_factory.h"
#include "common/directory_op.h"

namespace tfs
{
  namespace tools
  {
    class ShowInfo
    {
      public:
        ShowInfo(){}
        ~ShowInfo(){}

        int set_ns_ip(const std::string& ns_ip_port);
        void clean_last_file();
        int show_server(const int8_t flag, const int32_t num, const std::string& server_ip_port,
            int32_t count, const int32_t interval, const std::string& filename);
        int show_machine(const int8_t flag, const int32_t num,
            int32_t count, const int32_t interval, const std::string& filename);
        int show_block(const int8_t flag, const int32_t num, const uint32_t block_id, const int32_t block_chunk_num,
            int32_t count, const int32_t interval, const std::string& filename);
        bool is_loop_;
        bool interrupt_;

      private:
        void load_last_ds();
        void save_last_ds();
        int get_file_handle(const std::string& filename, FILE** fp);
        uint64_t get_machine_id(const uint64_t server_id);
        std::map<uint64_t, ServerShow> last_server_map_;
        std::map<uint64_t, ServerShow> server_map_;
        std::map<uint64_t, MachineShow> machine_map_;
        std::set<BlockShow> block_set_;
        uint64_t ns_ip_;
    };
  }
}

#endif

