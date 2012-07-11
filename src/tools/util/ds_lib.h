/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ds_lib.h 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   jihe
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_TOOLS_UTIL_DS_LIB_H_
#define TFS_TOOLS_UTIL_DS_LIB_H_

#include "common/internal.h"

namespace tfs
{
  namespace tools
  {
    class DsLib
    {
    public:
      DsLib()
      {
      }

      ~DsLib()
      {
      }

      static int get_server_status(common::DsTask& ds_task);
      static int get_ping_status(common::DsTask& ds_task);
      static int new_block(common::DsTask& ds_task);
      static int remove_block(common::DsTask& ds_task);
      static int list_block(common::DsTask& list_block_task);
      static int get_block_info(common::DsTask& list_block_task);
      static int reset_block_version(common::DsTask& list_block_task);
      static int create_file_id(common::DsTask& ds_task);
      static int list_file(common::DsTask& ds_task);
      static int check_file_info(common::DsTask& ds_task);
      static int read_file_data(common::DsTask& ds_task);
      static int write_file_data(common::DsTask& ds_task);
      static int unlink_file(common::DsTask& ds_task);
      static int rename_file(common::DsTask& ds_task);
      static int read_file_info(common::DsTask& ds_task);
      static int list_bitmap(common::DsTask& ds_task);
      static int send_crc_error(common::DsTask& ds_task);

    private:
      static void print_file_info(const char* name, common::FileInfo& file_info);
      static int write_data(const uint64_t server_id, const uint32_t block_id, const char* data, const int32_t length,
                            const int32_t offset, const uint64_t file_id, const uint64_t file_num);
      static int create_file_num(const uint64_t server_id, const uint32_t block_id, const uint64_t file_id, uint64_t& new_file_id,
                                 int64_t& file_num);
      static int close_data(const uint64_t server_id, const uint32_t block_id, const uint32_t crc, const uint64_t file_id,
                            const uint64_t file_num);
    };
  }
}
#endif
